# Loads SSRS RDL metadata into repository tables and logs errors into dbo.RPT_Error_Log
# Error log column names aligned to actual table structure.
# Updated to better parse CTE-based SQL, bracketed identifiers, and 3-part object names.
# Updated to resolve shared data sources from dbo.DataSource.Link -> dbo.Catalog.ItemID where Catalog.Type = 5.
# Updated to populate DataSource_Type from either <Extension> or <DataProvider>.
# Updated to load only Catalog.Type = 5 rows into RPT_DataSources while still loading Type = 2 reports/queries/objects.

import pyodbc
import xml.etree.ElementTree as ET
import re
import traceback
from datetime import datetime

SOURCE_SERVER = "Server1"
SOURCE_DATABASE = "AdventureWorks2019"
SOURCE_TABLE = "dbo.Catalog"
SOURCE_DATASOURCE_TABLE = "dbo.DataSource"

TARGET_SERVER = "Server2"
TARGET_DATABASE = "ReportMetaDataRepository"

TARGET_RPT_CATALOG = "dbo.RPT_Catalog"
TARGET_RPT_DATASOURCES = "dbo.RPT_DataSources"
TARGET_RPT_DATASOURCE_MAP = "dbo.RPT_DataSource_Map"
TARGET_RPT_QUERIES = "dbo.RPT_Queries"
TARGET_RPT_OBJECTS = "dbo.RPT_Objects"
TARGET_RPT_SERVERS = "dbo.RPT_Servers"
TARGET_ERROR_LOG = "dbo.RPT_Error_Log"

ODBC_DRIVER = "ODBC Driver 17 for SQL Server"
LOAD_REPORTS_ONLY = True
CLEAR_TARGET_TABLES_BEFORE_LOAD = False
BUSINESS_SUITE_VALUE = "SSRS"
PROCESS_NAME = "SSRS_RDL_ETL_Load"


def get_connection(server_name: str, database_name: str) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def print_error(ex: Exception) -> None:
    print("\nERROR DETAILS")
    print("-" * 80)
    print(f"Type : {type(ex).__name__}")
    print(f"Text : {str(ex)}")
    if hasattr(ex, "args"):
        print("Args :")
        for i, arg in enumerate(ex.args, start=1):
            print(f"  [{i}] {arg}")
    print("-" * 80)


def log_error_to_table(
    target_conn,
    process_name,
    source_table,
    target_table,
    report_path,
    report_name,
    error_type,
    error_message,
    error_details
):
    try:
        cursor = target_conn.cursor()
        sql = f"""
            INSERT INTO {TARGET_ERROR_LOG}
            (
                Process_Name,
                Source_Table,
                Target_Table,
                Report_Path,
                Report_Name,
                Error_Type,
                Error_Message,
                Error_Details,
                Error_Date,
                Created_By,
                Created_Date,
                Updated_By,
                Updated_Date
            )
            VALUES
            (
                ?, ?, ?, ?, ?, ?, ?, ?,
                GETDATE(),
                USER_NAME(), GETDATE(), USER_NAME(), GETDATE()
            )
        """
        cursor.execute(
            sql,
            process_name,
            source_table,
            target_table,
            report_path,
            report_name,
            error_type,
            error_message,
            error_details
        )
        target_conn.commit()
        print("  Error logged to dbo.RPT_Error_Log.")
    except Exception as log_ex:
        print("  Failed to write to error log table.")
        print(type(log_ex).__name__, str(log_ex))


def fetch_catalog_rows(source_conn):
    where_clause = "WHERE Content IS NOT NULL"
    if LOAD_REPORTS_ONLY:
        where_clause += " AND Type = 2"

    sql = f"""
        SELECT
            ItemID,
            Name,
            Type,
            Path,
            Content
        FROM {SOURCE_TABLE}
        {where_clause}
        ORDER BY Path
    """
    cursor = source_conn.cursor()
    print("Reading source Catalog rows...")
    cursor.execute(sql)
    rows = cursor.fetchall()
    print(f"Source rows fetched: {len(rows)}")
    return rows


def decode_rdl_content(content_bytes):
    if content_bytes is None:
        return None

    if isinstance(content_bytes, memoryview):
        content_bytes = content_bytes.tobytes()

    for enc in ["utf-8", "utf-16", "utf-16-le", "utf-16-be"]:
        try:
            text = content_bytes.decode(enc)
            if "<Report" in text or "<?xml" in text:
                return text
        except Exception:
            pass

    try:
        return content_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return None


def decode_xml_content(content_bytes):
    """Decode SSRS Catalog.Content for reports and shared datasource XML.

    Some ReportServer databases store Catalog.Content as UTF-16 bytes.
    Decoding it as UTF-8 can make the values look like unreadable/Chinese characters,
    so this helper tries the common encodings and only returns text that looks like XML.
    """
    if content_bytes is None:
        return None

    if isinstance(content_bytes, memoryview):
        content_bytes = content_bytes.tobytes()

    for enc in ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "utf-8"]:
        try:
            text = content_bytes.decode(enc)
            text = text.strip("\ufeff").strip()
            if text.startswith("<") or "<?xml" in text:
                return text
        except Exception:
            pass

    try:
        return content_bytes.decode("utf-8", errors="ignore").strip("\ufeff").strip()
    except Exception:
        return None


def get_namespace(root):
    match = re.match(r"\{(.*)\}", root.tag)
    return match.group(1) if match else ""


def ns_tag(namespace, tag_name):
    return f"{{{namespace}}}{tag_name}" if namespace else tag_name


def extract_server_instance(connect_string):
    if not connect_string:
        return None
    match = re.search(
        r"(?:Data Source|Server|Address|Addr|Network Address)\s*=\s*([^;]+)",
        connect_string,
        flags=re.IGNORECASE
    )
    return match.group(1).strip() if match else None


def extract_database_name(connect_string):
    if not connect_string:
        return None
    match = re.search(
        r"(?:Initial Catalog|Database)\s*=\s*([^;]+)",
        connect_string,
        flags=re.IGNORECASE
    )
    return match.group(1).strip() if match else None


def parse_shared_datasource_content(content_bytes):
    """Parse dbo.Catalog.Content for Catalog.Type = 5 shared datasource rows."""
    result = {
        "connection_string": None,
        "connection_username": None,
        "server_instance": None,
        "database_name": None,
        "datasource_type": None
    }

    xml_text = decode_xml_content(content_bytes)
    if not xml_text:
        return result

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return result

    for elem in root.iter():
        tag = elem.tag.split("}")[-1].lower()
        value = elem.text.strip() if elem.text else None

        if tag in ("connectstring", "connectionstring") and value:
            result["connection_string"] = value
        elif tag in ("username", "user_name") and value:
            result["connection_username"] = value
        elif tag in ("extension", "dataprovider") and value:
            # SSRS XML may store provider as either <Extension> or <DataProvider>.
            # Use the first available value to populate DataSource_Type.
            if not result["datasource_type"]:
                result["datasource_type"] = value

    result["server_instance"] = extract_server_instance(result["connection_string"])
    result["database_name"] = extract_database_name(result["connection_string"])
    return result


def fetch_shared_datasource_lookup(source_conn):
    """Build lookup by report ItemID + report datasource name.

    For normal report rows, dbo.Catalog.Type = 2 and Catalog.ItemID = DataSource.ItemID.
    For shared datasource rows, DataSource.Link points to dbo.Catalog.ItemID where Catalog.Type = 5.
    """
    sql = f"""
        SELECT
            ds.ItemID AS ReportItemID,
            ds.Name AS ReportDataSourceName,
            ds.Link AS SharedDataSourceItemID,
            shared_cat.Name AS SharedDataSourceName,
            shared_cat.Path AS SharedDataSourcePath,
            shared_cat.Content AS SharedDataSourceContent
        FROM {SOURCE_DATASOURCE_TABLE} ds
        INNER JOIN {SOURCE_TABLE} shared_cat
            ON ds.Link = shared_cat.ItemID
           AND shared_cat.Type = 5
        WHERE ds.Link IS NOT NULL
    """

    cursor = source_conn.cursor()
    print("Reading shared datasource definitions from dbo.DataSource.Link -> dbo.Catalog.ItemID...")
    cursor.execute(sql)
    rows = cursor.fetchall()

    lookup = {}
    for row in rows:
        parsed = parse_shared_datasource_content(row.SharedDataSourceContent)
        report_item_id = str(row.ReportItemID).upper()
        report_ds_name = row.ReportDataSourceName

        lookup[(report_item_id, report_ds_name)] = {
            "datasource_name": row.SharedDataSourceName,
            "report_datasource_name": report_ds_name,
            "shared_datasource_path": row.SharedDataSourcePath,
            "shared_datasource_item_id": str(row.SharedDataSourceItemID).upper() if row.SharedDataSourceItemID else None,
            "connection_string": parsed.get("connection_string"),
            "connection_username": parsed.get("connection_username"),
            "server_instance": parsed.get("server_instance"),
            "database_name": parsed.get("database_name"),
            "datasource_type": parsed.get("datasource_type"),
            "datasource_reference": row.SharedDataSourcePath,
            "is_shared_datasource": 1
        }

    print(f"Shared datasource links fetched: {len(lookup)}")
    return lookup


def fetch_catalog_type5_datasources(source_conn):
    """Fetch all shared datasource definitions stored as dbo.Catalog.Type = 5.

    Some ReportServer databases have datasource rows where dbo.DataSource.ItemID is the
    same ItemID as the shared datasource row in dbo.Catalog. These rows may not be
    returned by the report-level Link join, so this function loads them directly into
    dbo.RPT_DataSources.

    Join requested:
        dbo.DataSource ds
        INNER JOIN dbo.Catalog shared_cat
            ON ds.ItemID = shared_cat.ItemID
           AND shared_cat.Type = 5
    """
    sql = f"""
        SELECT DISTINCT
            ds.ItemID AS SharedDataSourceItemID,
            ds.Name AS DataSourceTableName,
            shared_cat.Name AS SharedDataSourceName,
            shared_cat.Path AS SharedDataSourcePath,
            shared_cat.Content AS SharedDataSourceContent
        FROM {SOURCE_DATASOURCE_TABLE} ds
        INNER JOIN {SOURCE_TABLE} shared_cat
            ON ds.ItemID = shared_cat.ItemID
           AND shared_cat.Type = 5
    """

    cursor = source_conn.cursor()
    print("Reading standalone shared datasource definitions from dbo.DataSource.ItemID -> dbo.Catalog.ItemID...")
    cursor.execute(sql)
    rows = cursor.fetchall()

    datasources = []
    for row in rows:
        parsed = parse_shared_datasource_content(row.SharedDataSourceContent)

        # Prefer dbo.Catalog.Name because this is the actual shared datasource object name.
        datasource_name = row.SharedDataSourceName or row.DataSourceTableName

        datasources.append({
            "datasource_name": datasource_name,
            "report_datasource_name": row.DataSourceTableName,
            "shared_datasource_path": row.SharedDataSourcePath,
            "shared_datasource_item_id": str(row.SharedDataSourceItemID).upper() if row.SharedDataSourceItemID else None,
            "connection_string": parsed.get("connection_string"),
            "connection_username": parsed.get("connection_username"),
            "server_instance": parsed.get("server_instance"),
            "database_name": parsed.get("database_name"),
            "datasource_type": parsed.get("datasource_type"),
            "datasource_reference": row.SharedDataSourcePath,
            "is_shared_datasource": 1
        })

    print(f"Standalone Catalog.Type = 5 shared datasources fetched: {len(datasources)}")
    return datasources


def load_catalog_type5_datasources(cursor, type5_datasources, rpt_datasource_columns):
    """Insert/get all Catalog.Type = 5 shared datasource rows into dbo.RPT_DataSources."""
    processed_count = 0

    for ds in type5_datasources:
        datasource_id = get_or_create_datasource(cursor, ds, rpt_datasource_columns)
        processed_count += 1
        print(
            f"  Loaded Catalog.Type=5 datasource: {ds.get('datasource_name')} "
            f"-> DataSource_ID = {datasource_id}"
        )

    return processed_count


def apply_shared_datasource_values(report_item_id, datasources, shared_datasource_lookup):
    """Replace report-level shared datasource placeholders with actual Catalog.Type=5 details."""
    report_item_id = str(report_item_id).upper()
    updated = []

    for ds in datasources:
        report_ds_name = ds.get("datasource_name")
        shared = shared_datasource_lookup.get((report_item_id, report_ds_name))

        if shared:
            merged = dict(ds)
            merged.update(shared)
            updated.append(merged)
            print(
                f"  Resolved shared datasource: report DS '{report_ds_name}' "
                f"-> shared DS '{shared.get('datasource_name')}'"
            )
        else:
            ds["datasource_type"] = ds.get("datasource_type")
            ds["is_shared_datasource"] = 0
            updated.append(ds)

    return updated


def get_table_columns(cursor, table_name):
    """Return target table column names so optional columns can be loaded only when they exist."""
    schema_name, object_name = table_name.split(".", 1) if "." in table_name else ("dbo", table_name)
    sql = """
        SELECT c.name
        FROM sys.columns c
        JOIN sys.objects o ON c.object_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = ?
          AND o.name = ?
    """
    cursor.execute(sql, schema_name.replace("[", "").replace("]", ""), object_name.replace("[", "").replace("]", ""))
    return {row[0].lower() for row in cursor.fetchall()}


def _normalize_sql_for_parsing(command_text):
    if not command_text:
        return ""

    # Remove single-line comments
    sql_text = re.sub(r"--.*?$", " ", command_text, flags=re.MULTILINE)

    # Remove block comments
    sql_text = re.sub(r"/\*.*?\*/", " ", sql_text, flags=re.DOTALL)

    # Normalize whitespace
    sql_text = re.sub(r"\s+", " ", sql_text).strip()
    return sql_text


def _extract_cte_names(sql_text):
    cte_names = set()
    if not sql_text:
        return cte_names

    # Capture CTE names in patterns like:
    # WITH cte1 AS (...), cte2 AS (...)
    matches = re.findall(r"\b(?:WITH|,)\s*\[?([A-Za-z_][\w]*)\]?\s+AS\s*\(", sql_text, flags=re.IGNORECASE)
    for name in matches:
        cte_names.add(name.lower())
    return cte_names


def extract_schema_objects(command_text):
    if not command_text:
        return []

    sql_text = _normalize_sql_for_parsing(command_text)
    cte_names = _extract_cte_names(sql_text)
    found = set()

    # Supports:
    #   schema.object
    #   [schema].[object]
    #   database.schema.object
    #   [database].[schema].[object]
    # Captures schema + object only.
    patterns = [
        r"\b(?:from|join|apply|cross\s+apply|outer\s+apply)\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?\.\[?([\w]+)\]?",
        r"\bupdate\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?\.\[?([\w]+)\]?",
        r"\binsert\s+into\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?\.\[?([\w]+)\]?",
        r"\bmerge(?:\s+into)?\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?\.\[?([\w]+)\]?",
        r"\bdelete\s+from\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?\.\[?([\w]+)\]?",
        r"\bexec(?:ute)?\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?\.\[?([\w]+)\]?"
    ]

    for pattern in patterns:
        matches = re.findall(pattern, sql_text, flags=re.IGNORECASE)
        for schema_name, object_name in matches:
            schema_name = schema_name.strip()
            object_name = object_name.strip()

            if schema_name.startswith("#") or object_name.startswith("#"):
                continue

            # Avoid inserting references to CTE aliases as real objects.
            if object_name.lower() in cte_names:
                continue

            found.add((schema_name, object_name))

    return sorted(found)


def parse_rdl(xml_text):
    root = ET.fromstring(xml_text)
    namespace = get_namespace(root)

    datasource_map = {}
    datasources_parent = root.find(ns_tag(namespace, "DataSources"))
    if datasources_parent is not None:
        for ds in datasources_parent.findall(ns_tag(namespace, "DataSource")):
            ds_name = ds.attrib.get("Name")
            connect_string = None
            user_name = None
            server_instance = None
            database_name = None
            datasource_type = None

            conn_props = ds.find(ns_tag(namespace, "ConnectionProperties"))
            if conn_props is not None:
                cs_node = conn_props.find(ns_tag(namespace, "ConnectString"))
                un_node = conn_props.find(ns_tag(namespace, "UserName"))
                dp_node = conn_props.find(ns_tag(namespace, "DataProvider"))
                ext_node = conn_props.find(ns_tag(namespace, "Extension"))

                if cs_node is not None and cs_node.text:
                    connect_string = cs_node.text.strip()

                if un_node is not None and un_node.text:
                    user_name = un_node.text.strip()

                # Some RDL/shared datasource XML versions use <DataProvider>,
                # while others use <Extension>. Load either value into DataSource_Type.
                if dp_node is not None and dp_node.text:
                    datasource_type = dp_node.text.strip()
                elif ext_node is not None and ext_node.text:
                    datasource_type = ext_node.text.strip()

                server_instance = extract_server_instance(connect_string)
                database_name = extract_database_name(connect_string)

            ds_ref_node = ds.find(ns_tag(namespace, "DataSourceReference"))
            datasource_reference = ds_ref_node.text.strip() if ds_ref_node is not None and ds_ref_node.text else None

            datasource_map[ds_name] = {
                "datasource_name": ds_name,
                "connection_string": connect_string,
                "connection_username": user_name,
                "server_instance": server_instance,
                "database_name": database_name,
                "datasource_type": datasource_type,
                "datasource_reference": datasource_reference,
                "is_shared_datasource": 0
            }

    queries = []
    datasets_parent = root.find(ns_tag(namespace, "DataSets"))
    if datasets_parent is not None:
        for dataset in datasets_parent.findall(ns_tag(namespace, "DataSet")):
            dataset_name = dataset.attrib.get("Name")
            datasource_name = None
            command_text = None

            query_node = dataset.find(ns_tag(namespace, "Query"))
            if query_node is not None:
                ds_node = query_node.find(ns_tag(namespace, "DataSourceName"))
                ct_node = query_node.find(ns_tag(namespace, "CommandText"))

                if ds_node is not None and ds_node.text:
                    datasource_name = ds_node.text.strip()

                if ct_node is not None and ct_node.text:
                    command_text = ct_node.text.strip()

            queries.append({
                "dataset_name": dataset_name,
                "datasource_name": datasource_name,
                "command_text": command_text,
                "objects": extract_schema_objects(command_text)
            })

    datasources = list(datasource_map.values())

    for q in queries:
        dsn = q.get("datasource_name")
        if dsn and dsn not in datasource_map:
            datasources.append({
                "datasource_name": dsn,
                "connection_string": None,
                "connection_username": None,
                "server_instance": None,
                "database_name": None,
                "datasource_type": None,
                "datasource_reference": None,
                "is_shared_datasource": 0
            })

    seen = set()
    deduped = []
    for ds in datasources:
        key = (
            ds.get("datasource_name"),
            ds.get("connection_string"),
            ds.get("connection_username"),
            ds.get("server_instance"),
            ds.get("database_name"),
            ds.get("datasource_type"),
            ds.get("datasource_reference")
        )
        if key not in seen:
            seen.add(key)
            deduped.append(ds)

    return deduped, queries


def clear_target_tables(target_conn):
    cursor = target_conn.cursor()
    print("Clearing target tables in dependency order...")
    cursor.execute(f"DELETE FROM {TARGET_RPT_OBJECTS}")
    cursor.execute(f"DELETE FROM {TARGET_RPT_QUERIES}")
    cursor.execute(f"DELETE FROM {TARGET_RPT_DATASOURCE_MAP}")
    cursor.execute(f"DELETE FROM {TARGET_RPT_DATASOURCES}")
    cursor.execute(f"DELETE FROM {TARGET_RPT_CATALOG}")
    target_conn.commit()
    print("Target tables cleared.")


def insert_rpt_catalog(cursor, report_row):
    sql = f"""
        INSERT INTO {TARGET_RPT_CATALOG}
        (
            RPT_Name,
            RPT_Type,
            RPT_Business_Suite,
            RPT_Path,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        OUTPUT INSERTED.RPT_ID
        VALUES (?, ?, ?, ?, USER_NAME(), GETDATE(), USER_NAME(), GETDATE())
    """
    cursor.execute(
        sql,
        report_row["report_name"],
        report_row["report_type"],
        report_row["business_suite"],
        report_row["report_path"]
    )
    return cursor.fetchone()[0]


def _first_existing_column(table_columns, *candidate_names):
    for name in candidate_names:
        if name.lower() in table_columns:
            return name
    return None


def get_or_create_datasource(cursor, ds_row, rpt_datasource_columns):
    find_sql = f"""
        SELECT TOP 1 DataSource_ID
        FROM {TARGET_RPT_DATASOURCES}
        WHERE ISNULL(DataSource_Name, '') = ISNULL(?, '')
          AND ISNULL(Connection_String_Text, '') = ISNULL(?, '')
          AND ISNULL(Connection_UserName, '') = ISNULL(?, '')
          AND ISNULL(Server_Instance, '') = ISNULL(?, '')
        ORDER BY DataSource_ID
    """
    cursor.execute(
        find_sql,
        ds_row.get("datasource_name"),
        ds_row.get("connection_string"),
        ds_row.get("connection_username"),
        ds_row.get("server_instance")
    )
    existing = cursor.fetchone()
    if existing:
        return existing[0]

    columns = [
        "DataSource_Name",
        "Connection_String_Text",
        "Connection_UserName",
        "Server_Instance"
    ]
    values = [
        ds_row.get("datasource_name"),
        ds_row.get("connection_string"),
        ds_row.get("connection_username"),
        ds_row.get("server_instance")
    ]

    # Load optional columns only if they exist in your target table.
    database_col = _first_existing_column(rpt_datasource_columns, "Database_Name")
    datasource_type_col = _first_existing_column(rpt_datasource_columns, "DataSource_Type", "Datasource_Type", "Extension")
    shared_path_col = _first_existing_column(rpt_datasource_columns, "DataSource_Path", "Shared_DataSource_Path")

    if database_col:
        columns.append(database_col)
        values.append(ds_row.get("database_name"))

    if datasource_type_col:
        columns.append(datasource_type_col)
        values.append(ds_row.get("datasource_type"))

    if shared_path_col:
        columns.append(shared_path_col)
        values.append(ds_row.get("shared_datasource_path"))

    columns.extend(["Created_By", "Created_Date", "Updated_By", "Updated_Date"])
    column_text = ",\n            ".join(columns)
    placeholders = ", ".join(["?"] * len(values))

    insert_sql = f"""
        INSERT INTO {TARGET_RPT_DATASOURCES}
        (
            {column_text}
        )
        OUTPUT INSERTED.DataSource_ID
        VALUES ({placeholders}, USER_NAME(), GETDATE(), USER_NAME(), GETDATE())
    """
    cursor.execute(insert_sql, *values)
    return cursor.fetchone()[0]


def get_existing_datasource_id(cursor, ds_row):
    """Return an existing DataSource_ID from dbo.RPT_DataSources without inserting.

    This is used while processing Catalog.Type = 2 reports so report parsing can
    continue, but dbo.RPT_DataSources stays limited to Catalog.Type = 5 shared
    datasource rows only.
    """
    find_sql = f"""
        SELECT TOP 1 DataSource_ID
        FROM {TARGET_RPT_DATASOURCES}
        WHERE ISNULL(DataSource_Name, '') = ISNULL(?, '')
          AND ISNULL(Connection_String_Text, '') = ISNULL(?, '')
          AND ISNULL(Connection_UserName, '') = ISNULL(?, '')
          AND ISNULL(Server_Instance, '') = ISNULL(?, '')
        ORDER BY DataSource_ID
    """
    cursor.execute(
        find_sql,
        ds_row.get("datasource_name"),
        ds_row.get("connection_string"),
        ds_row.get("connection_username"),
        ds_row.get("server_instance")
    )
    existing = cursor.fetchone()
    return existing[0] if existing else None


def ensure_datasource_map(cursor, rpt_id, datasource_id):
    find_sql = f"""
        SELECT 1
        FROM {TARGET_RPT_DATASOURCE_MAP}
        WHERE RPT_ID = ? AND DataSource_ID = ?
    """
    cursor.execute(find_sql, rpt_id, datasource_id)
    if cursor.fetchone():
        return

    insert_sql = f"""
        INSERT INTO {TARGET_RPT_DATASOURCE_MAP}
        (
            RPT_ID,
            DataSource_ID,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        VALUES (?, ?, USER_NAME(), GETDATE(), USER_NAME(), GETDATE())
    """
    cursor.execute(insert_sql, rpt_id, datasource_id)


def insert_query(cursor, rpt_id, datasource_id, query_row):
    sql = f"""
        INSERT INTO {TARGET_RPT_QUERIES}
        (
            RPT_ID,
            DataSource_ID,
            Dataset_Name,
            Query_Text,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        OUTPUT INSERTED.Query_ID
        VALUES (?, ?, ?, ?, USER_NAME(), GETDATE(), USER_NAME(), GETDATE())
    """
    cursor.execute(
        sql,
        rpt_id,
        datasource_id,
        query_row.get("dataset_name"),
        query_row.get("command_text")
    )
    return cursor.fetchone()[0]


def insert_object(cursor, rpt_id, query_id, datasource_id, report_name, dataset_name,
                  database_name, schema_name, object_name):
    sql = f"""
        INSERT INTO {TARGET_RPT_OBJECTS}
        (
            Query_ID,
            RPT_ID,
            DataSource_ID,
            RPT_Name,
            Dataset_Name,
            Database_Name,
            Schema_Name,
            Object_Name,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, USER_NAME(), GETDATE(), USER_NAME(), GETDATE())
    """
    cursor.execute(
        sql,
        query_id,
        rpt_id,
        datasource_id,
        report_name,
        dataset_name,
        database_name,
        schema_name,
        object_name
    )


def insert_rpt_servers(cursor):
    sql = f"""
        INSERT INTO {TARGET_RPT_SERVERS}
        (
            Server_Name,
            Database_Name,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        SELECT DISTINCT
            ds.Server_Instance,
            o.Database_Name,
            USER_NAME(),
            GETDATE(),
            USER_NAME(),
            GETDATE()
        FROM {TARGET_RPT_DATASOURCES} ds
        JOIN {TARGET_RPT_OBJECTS} o
            ON ds.DataSource_ID = o.DataSource_ID
        WHERE ds.Server_Instance IS NOT NULL
          AND o.Database_Name IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {TARGET_RPT_SERVERS} s
              WHERE ISNULL(s.Server_Name, '') = ISNULL(ds.Server_Instance, '')
                AND ISNULL(s.Database_Name, '') = ISNULL(o.Database_Name, '')
          )
    """
    cursor.execute(sql)


def process_report(cursor, source_row, shared_datasource_lookup, rpt_datasource_columns):
    report_name = source_row.Name
    report_type = str(source_row.Type) if source_row.Type is not None else None
    report_path = source_row.Path
    content = source_row.Content

    print(f"\nProcessing report: {report_path}")

    xml_text = decode_rdl_content(content)
    if not xml_text:
        raise ValueError("Could not decode Content field into RDL XML.")

    datasources, queries = parse_rdl(xml_text)

    # Keep this lookup. It is needed to resolve the report datasource name to the
    # already-loaded Catalog.Type = 5 shared datasource row.
    datasources = apply_shared_datasource_values(source_row.ItemID, datasources, shared_datasource_lookup)

    report_row = {
        "report_name": report_name,
        "report_type": report_type,
        "business_suite": BUSINESS_SUITE_VALUE,
        "report_path": report_path
    }

    rpt_id = insert_rpt_catalog(cursor, report_row)
    print(f"  Inserted RPT_Catalog row. RPT_ID = {rpt_id}")

    datasource_id_map = {}
    datasource_info_map = {}
    datasource_count = 0
    map_count = 0
    query_count = 0
    object_count = 0

    for ds in datasources:
        datasource_name = ds.get("datasource_name")
        report_datasource_name = ds.get("report_datasource_name") or datasource_name

        # IMPORTANT:
        # Do NOT call get_or_create_datasource() here.
        # Calling get_or_create_datasource() during Type=2 report processing can insert
        # embedded/report datasource rows into dbo.RPT_DataSources.
        # dbo.RPT_DataSources should only be loaded from Catalog.Type = 5 by
        # load_catalog_type5_datasources().
        if ds.get("is_shared_datasource") == 1:
            datasource_id = get_existing_datasource_id(cursor, ds)

            if datasource_id is None:
                print(
                    f"  WARNING: Shared datasource '{datasource_name}' was referenced by report "
                    f"but was not found in RPT_DataSources. It will not be inserted from Type=2 processing."
                )
            else:
                # Store both the report-local datasource name and the actual shared datasource name.
                # Datasets usually reference the report-local name from the RDL.
                datasource_id_map[report_datasource_name] = datasource_id
                datasource_id_map[datasource_name] = datasource_id
                datasource_info_map[report_datasource_name] = ds
                datasource_info_map[datasource_name] = ds

                ensure_datasource_map(cursor, rpt_id, datasource_id)
                map_count += 1
                print(f"  Mapped shared datasource: {report_datasource_name} -> {datasource_name} -> DataSource_ID = {datasource_id}")
        else:
            # Embedded datasource from Catalog.Type = 2 report XML.
            # Skip inserting it into dbo.RPT_DataSources to keep that table Type=5 only.
            datasource_id_map[report_datasource_name] = None
            datasource_info_map[report_datasource_name] = ds
            print(f"  Skipped embedded/report datasource from Type=2 XML: {report_datasource_name}")

    for q in queries:
        datasource_name = q.get("datasource_name")
        datasource_id = datasource_id_map.get(datasource_name)
        datasource_info = datasource_info_map.get(datasource_name, {})

        if datasource_id is None and len([v for v in datasource_id_map.values() if v is not None]) == 1:
            # If there is exactly one resolved shared datasource, use it as fallback.
            resolved_items = [(k, v) for k, v in datasource_id_map.items() if v is not None]
            datasource_id = resolved_items[0][1]
            datasource_info = datasource_info_map.get(resolved_items[0][0], {})

        if datasource_id is None:
            print(
                f"  WARNING: Datasource could not be resolved for dataset '{q.get('dataset_name')}'. "
                "Query/Object rows will be inserted with DataSource_ID = NULL if the target table allows it."
            )

        query_id = insert_query(cursor, rpt_id, datasource_id, q)
        query_count += 1
        print(f"  Inserted query: Dataset = {q.get('dataset_name')} , Query_ID = {query_id}")
        print(f"  Objects found for dataset {q.get('dataset_name')}: {q.get('objects', [])}")

        database_name = datasource_info.get("database_name")

        for schema_name, object_name in q.get("objects", []):
            insert_object(
                cursor,
                rpt_id,
                query_id,
                datasource_id,
                report_name,
                q.get("dataset_name"),
                database_name,
                schema_name,
                object_name
            )
            object_count += 1

    return {
        "catalog": 1,
        "datasources": datasource_count,
        "maps": map_count,
        "queries": query_count,
        "objects": object_count
    }


def main():
    source_conn = None
    target_conn = None

    total_catalog = 0
    total_datasources = 0
    total_maps = 0
    total_queries = 0
    total_objects = 0

    target_tables_text = ", ".join([
        TARGET_RPT_CATALOG,
        TARGET_RPT_DATASOURCES,
        TARGET_RPT_DATASOURCE_MAP,
        TARGET_RPT_QUERIES,
        TARGET_RPT_OBJECTS,
        TARGET_RPT_SERVERS
    ])

    try:
        print("ETL started")
        print(f"Start time: {datetime.now()}")

        print(f"Connecting to source: {SOURCE_SERVER} / {SOURCE_DATABASE}")
        source_conn = get_connection(SOURCE_SERVER, SOURCE_DATABASE)
        print("Connected to source.")

        print(f"Connecting to target: {TARGET_SERVER} / {TARGET_DATABASE}")
        target_conn = get_connection(TARGET_SERVER, TARGET_DATABASE)
        print("Connected to target.")

        source_rows = fetch_catalog_rows(source_conn)
        if not source_rows:
            print("No source rows found. Nothing to load.")
            return

        shared_datasource_lookup = fetch_shared_datasource_lookup(source_conn)
        catalog_type5_datasources = fetch_catalog_type5_datasources(source_conn)

        if CLEAR_TARGET_TABLES_BEFORE_LOAD:
            clear_target_tables(target_conn)

        cursor = target_conn.cursor()
        rpt_datasource_columns = get_table_columns(cursor, TARGET_RPT_DATASOURCES)

        try:
            print("\nLoading all Catalog.Type = 5 shared datasources into RPT_DataSources...")
            standalone_ds_count = load_catalog_type5_datasources(cursor, catalog_type5_datasources, rpt_datasource_columns)
            target_conn.commit()
            total_datasources += standalone_ds_count
            print(f"Catalog.Type = 5 shared datasource load completed. Rows processed: {standalone_ds_count}")
        except Exception as ex:
            target_conn.rollback()
            print("Catalog.Type = 5 shared datasource load failed.")
            print_error(ex)
            log_error_to_table(
                target_conn=target_conn,
                process_name=PROCESS_NAME,
                source_table=f"{SOURCE_DATASOURCE_TABLE}, {SOURCE_TABLE}",
                target_table=TARGET_RPT_DATASOURCES,
                report_path=None,
                report_name=None,
                error_type=type(ex).__name__,
                error_message=str(ex),
                error_details=traceback.format_exc()
            )

        for row in source_rows:
            try:
                counts = process_report(cursor, row, shared_datasource_lookup, rpt_datasource_columns)
                target_conn.commit()

                total_catalog += counts["catalog"]
                total_datasources += counts["datasources"]
                total_maps += counts["maps"]
                total_queries += counts["queries"]
                total_objects += counts["objects"]

            except Exception as ex:
                target_conn.rollback()
                print(f"  Report failed and rolled back: {row.Path}")
                print_error(ex)

                log_error_to_table(
                    target_conn=target_conn,
                    process_name=PROCESS_NAME,
                    source_table=SOURCE_TABLE,
                    target_table=target_tables_text,
                    report_path=getattr(row, "Path", None),
                    report_name=getattr(row, "Name", None),
                    error_type=type(ex).__name__,
                    error_message=str(ex),
                    error_details=traceback.format_exc()
                )

        try:
            print("\nInserting rows into dbo.RPT_Servers...")
            insert_rpt_servers(cursor)
            target_conn.commit()
            print("RPT_Servers load completed.")
        except Exception as ex:
            target_conn.rollback()
            print("RPT_Servers load failed.")
            print_error(ex)
            log_error_to_table(
                target_conn=target_conn,
                process_name=PROCESS_NAME,
                source_table=f"{TARGET_RPT_DATASOURCES}, {TARGET_RPT_OBJECTS}",
                target_table=TARGET_RPT_SERVERS,
                report_path=None,
                report_name=None,
                error_type=type(ex).__name__,
                error_message=str(ex),
                error_details=traceback.format_exc()
            )

        print("\nLOAD SUMMARY")
        print("-" * 60)
        print(f"RPT_Catalog rows inserted       : {total_catalog}")
        print(f"Datasource rows processed       : {total_datasources}")
        print(f"Datasource map rows inserted    : {total_maps}")
        print(f"Query rows inserted             : {total_queries}")
        print(f"Object rows inserted            : {total_objects}")
        print("-" * 60)
        print(f"End time: {datetime.now()}")
        print("ETL completed.")

    except Exception as ex:
        print("\nFatal error occurred.")
        print_error(ex)
        if target_conn:
            try:
                log_error_to_table(
                    target_conn=target_conn,
                    process_name=PROCESS_NAME,
                    source_table=SOURCE_TABLE,
                    target_table=target_tables_text,
                    report_path=None,
                    report_name=None,
                    error_type=type(ex).__name__,
                    error_message=str(ex),
                    error_details=traceback.format_exc()
                )
            except Exception:
                pass

    finally:
        if source_conn:
            source_conn.close()
            print("Source connection closed.")
        if target_conn:
            target_conn.close()
            print("Target connection closed.")


if __name__ == "__main__":
    main()
