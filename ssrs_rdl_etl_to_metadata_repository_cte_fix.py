# Loads SSRS RDL metadata into repository tables and logs errors into dbo.RPT_Error_Log
# Error log column names aligned to actual table structure.
# Updated to better parse CTE-based SQL, bracketed identifiers, and 3-part object names.

import pyodbc
import xml.etree.ElementTree as ET
import re
import traceback
from datetime import datetime

SOURCE_SERVER = "Server1"
SOURCE_DATABASE = "AdventureWorks2019"
SOURCE_TABLE = "dbo.Catalog"

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

            conn_props = ds.find(ns_tag(namespace, "ConnectionProperties"))
            if conn_props is not None:
                cs_node = conn_props.find(ns_tag(namespace, "ConnectString"))
                un_node = conn_props.find(ns_tag(namespace, "UserName"))

                if cs_node is not None and cs_node.text:
                    connect_string = cs_node.text.strip()

                if un_node is not None and un_node.text:
                    user_name = un_node.text.strip()

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
                "datasource_reference": datasource_reference
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
                "datasource_reference": None
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


def get_or_create_datasource(cursor, ds_row):
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

    insert_sql = f"""
        INSERT INTO {TARGET_RPT_DATASOURCES}
        (
            DataSource_Name,
            Connection_String_Text,
            Connection_UserName,
            Server_Instance,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        OUTPUT INSERTED.DataSource_ID
        VALUES (?, ?, ?, ?, USER_NAME(), GETDATE(), USER_NAME(), GETDATE())
    """
    cursor.execute(
        insert_sql,
        ds_row.get("datasource_name"),
        ds_row.get("connection_string"),
        ds_row.get("connection_username"),
        ds_row.get("server_instance")
    )
    return cursor.fetchone()[0]


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


def process_report(cursor, source_row):
    report_name = source_row.Name
    report_type = str(source_row.Type) if source_row.Type is not None else None
    report_path = source_row.Path
    content = source_row.Content

    print(f"\nProcessing report: {report_path}")

    xml_text = decode_rdl_content(content)
    if not xml_text:
        raise ValueError("Could not decode Content field into RDL XML.")

    datasources, queries = parse_rdl(xml_text)

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
        datasource_id = get_or_create_datasource(cursor, ds)
        datasource_name = ds.get("datasource_name")
        datasource_id_map[datasource_name] = datasource_id
        datasource_info_map[datasource_name] = ds
        datasource_count += 1

        ensure_datasource_map(cursor, rpt_id, datasource_id)
        map_count += 1
        print(f"  Mapped datasource: {datasource_name} -> DataSource_ID = {datasource_id}")

    for q in queries:
        datasource_name = q.get("datasource_name")
        datasource_id = datasource_id_map.get(datasource_name)
        datasource_info = datasource_info_map.get(datasource_name, {})

        if datasource_id is None and len(datasource_id_map) == 1:
            datasource_id = list(datasource_id_map.values())[0]
            datasource_info = list(datasource_info_map.values())[0]

        if datasource_id is None:
            raise ValueError(f"Datasource could not be resolved for dataset: {q.get('dataset_name')}")

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

        if CLEAR_TARGET_TABLES_BEFORE_LOAD:
            clear_target_tables(target_conn)

        cursor = target_conn.cursor()

        for row in source_rows:
            try:
                counts = process_report(cursor, row)
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
