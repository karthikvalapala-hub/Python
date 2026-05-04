# Loads SSRS RDL metadata into repository tables and logs errors into dbo.RPT_Error_Log
# Updated so dbo.RPT_DataSources is loaded primarily from dbo.Catalog Type = 5 (shared data sources)
# instead of embedded report data sources from Type = 2 RDL files.
# Also keeps the CTE/object parsing fix and loads Database_Name and DataSource_Type.

import pyodbc
import xml.etree.ElementTree as ET
import re
import traceback
from datetime import datetime

# ============================================================
# ENVIRONMENT CONFIGURATION
# Update the server/database values below for your environments.
# During script execution, user will enter: Dev, Test, or Prod.
# ============================================================
ENVIRONMENT_CONFIG = {
    "DEV": {
        "SOURCE_SERVER": "DevSourceServer",
        "SOURCE_DATABASE": "AdventureWorks2019",
        "TARGET_SERVER": "DevTargetServer",
        "TARGET_DATABASE": "ReportMetaDataRepository"
    },
    "TEST": {
        "SOURCE_SERVER": "TestSourceServer",
        "SOURCE_DATABASE": "AdventureWorks2019",
        "TARGET_SERVER": "TestTargetServer",
        "TARGET_DATABASE": "ReportMetaDataRepository"
    },
    "PROD": {
        "SOURCE_SERVER": "ProdSourceServer",
        "SOURCE_DATABASE": "AdventureWorks2019",
        "TARGET_SERVER": "ProdTargetServer",
        "TARGET_DATABASE": "ReportMetaDataRepository"
    }
}

# These are table names only. Server/database names are selected from ENVIRONMENT_CONFIG.
SOURCE_TABLE = "dbo.Catalog"
SOURCE_DATASOURCE_TABLE = "dbo.DataSource"

SOURCE_SERVER = None
SOURCE_DATABASE = None
TARGET_SERVER = None
TARGET_DATABASE = None

TARGET_RPT_CATALOG = "dbo.RPT_Catalog"
TARGET_RPT_DATASOURCES = "dbo.RPT_DataSources"
TARGET_RPT_DATASOURCE_MAP = "dbo.RPT_DataSource_Map"
TARGET_RPT_QUERIES = "dbo.RPT_Queries"
TARGET_RPT_OBJECTS = "dbo.RPT_Objects"
TARGET_RPT_SERVERS = "dbo.RPT_Servers"
TARGET_ERROR_LOG = "dbo.RPT_Error_Log"

ODBC_DRIVER = "ODBC Driver 17 for SQL Server"
CLEAR_TARGET_TABLES_BEFORE_LOAD = False
BUSINESS_SUITE_VALUE = "SSRS"
PROCESS_NAME = "SSRS_RDL_ETL_Load"

REPORT_TYPE = 2
SHARED_DATASOURCE_TYPE = 5


def get_environment_input():
    """Prompt user for environment and return normalized environment name."""
    allowed_envs = ", ".join(ENVIRONMENT_CONFIG.keys())

    while True:
        env_value = input(f"Enter environment ({allowed_envs}): ").strip().upper()

        if env_value in ENVIRONMENT_CONFIG:
            return env_value

        print(f"Invalid environment: {env_value}")
        print(f"Please enter one of: {allowed_envs}")


def apply_environment_config(environment_name):
    """Set source/target server and database globals based on selected environment."""
    global SOURCE_SERVER, SOURCE_DATABASE, TARGET_SERVER, TARGET_DATABASE

    config = ENVIRONMENT_CONFIG[environment_name]
    SOURCE_SERVER = config["SOURCE_SERVER"]
    SOURCE_DATABASE = config["SOURCE_DATABASE"]
    TARGET_SERVER = config["TARGET_SERVER"]
    TARGET_DATABASE = config["TARGET_DATABASE"]

    print("\nSelected environment configuration")
    print("-" * 60)
    print(f"Environment     : {environment_name}")
    print(f"Source Server   : {SOURCE_SERVER}")
    print(f"Source Database : {SOURCE_DATABASE}")
    print(f"Target Server   : {TARGET_SERVER}")
    print(f"Target Database : {TARGET_DATABASE}")
    print("-" * 60)


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


def fetch_reports(source_conn):
    sql = f"""
        SELECT
            ItemID,
            Name,
            Type,
            Path,
            Content
        FROM {SOURCE_TABLE}
        WHERE Content IS NOT NULL
          AND Type = {REPORT_TYPE}
        ORDER BY Path
    """
    cursor = source_conn.cursor()
    print("Reading source report rows (Type = 2)...")
    cursor.execute(sql)
    rows = cursor.fetchall()
    print(f"Report rows fetched: {len(rows)}")
    return rows


def fetch_shared_datasources(source_conn):
    sql = f"""
        SELECT
            c.ItemID,
            c.Name,
            c.Type,
            c.Path,
            c.Content,
            ds.Extension,
            ds.ConnectString,
            ds.OriginalConnectString,
            ds.Link,
            ds.CredentialRetrieval,
            ds.UserName
        FROM {SOURCE_TABLE} c
        LEFT JOIN {SOURCE_DATASOURCE_TABLE} ds
            ON c.ItemID = ds.ItemID
        WHERE c.Type = {SHARED_DATASOURCE_TYPE}
        ORDER BY c.Path
    """
    cursor = source_conn.cursor()
    print("Reading shared datasource rows (Type = 5) from Catalog + DataSource...")
    cursor.execute(sql)
    rows = cursor.fetchall()
    print(f"Shared datasource rows fetched: {len(rows)}")
    return rows


def decode_xml_content(content_bytes):
    if content_bytes is None:
        return None

    if isinstance(content_bytes, memoryview):
        content_bytes = content_bytes.tobytes()

    for enc in ["utf-8", "utf-16", "utf-16-le", "utf-16-be"]:
        try:
            text = content_bytes.decode(enc)
            if "<" in text and ">" in text:
                return text
        except Exception:
            pass

    try:
        return content_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return None


def clean_xml_text(xml_text):
    if not xml_text:
        return None

    text = xml_text.replace("\x00", "").strip()
    text = text.lstrip("\ufeff").strip()

    first_lt = text.find("<")
    if first_lt > 0:
        text = text[first_lt:]

    if not text.startswith("<"):
        return None

    return text

def get_namespace(root):
    match = re.match(r"\{(.*)\}", root.tag)
    return match.group(1) if match else ""


def ns_tag(namespace, tag_name):
    return f"{{{namespace}}}{tag_name}" if namespace else tag_name


def node_text(parent, namespace, tag_name):
    if parent is None:
        return None
    node = parent.find(ns_tag(namespace, tag_name))
    if node is not None and node.text:
        return node.text.strip()
    return None


def extract_connect_property(connect_string, property_names):
    if not connect_string:
        return None

    for prop in property_names:
        match = re.search(
            rf"(?:^|;)\s*{re.escape(prop)}\s*=\s*([^;]+)",
            connect_string,
            flags=re.IGNORECASE
        )
        if match:
            return match.group(1).strip()
    return None


def extract_server_instance(connect_string):
    return extract_connect_property(
        connect_string,
        ["Data Source", "Server", "Address", "Addr", "Network Address", "Host"]
    )


def extract_database_name(connect_string, datasource_type=None):
    database_name = extract_connect_property(
        connect_string,
        ["Initial Catalog", "Database", "DBQ"]
    )
    if database_name:
        return database_name

    ds_type = (datasource_type or "").strip().lower()

    # For Oracle, many SSRS connection strings only have Data Source=<tns/service>
    if ds_type == "oracle":
        return extract_connect_property(connect_string, ["Data Source"])

    # For DB2, current schema / db aliases may be the best available value
    if ds_type == "db2":
        return extract_connect_property(connect_string, ["Database", "CurrentSchema", "DefaultSchema", "Data Source"])

    # For Excel, DBQ or Data Source often holds the file path
    if ds_type == "excel":
        return extract_connect_property(connect_string, ["DBQ", "Data Source"])

    return None


def normalize_datasource_type(data_provider, connect_string=None):
    provider = (data_provider or "").strip().lower()
    conn = (connect_string or "").strip().lower()

    if provider in {"sql", "sqlclient", "system.data.sqlclient", "sql server"}:
        return "SQL"
    if provider in {"oracle", "oracleclient", "odp.net", "system.data.oracleclient", "oracle database"}:
        return "Oracle"
    if provider in {"db2", "ibmdb2", "ibm db2", "ibm.data.db2"}:
        return "DB2"
    if provider in {"excel", "microsoft excel"}:
        return "Excel"
    if provider in {"sapbw", "sap bw", "sap"}:
        return "SAP"
    if provider in {"saphana", "sap hana"}:
        return "SAP HANA"
    if provider in {"xml"}:
        return "XML"
    if provider in {"odbc"}:
        if "oracle" in conn:
            return "Oracle"
        if "db2" in conn:
            return "DB2"
        if "excel" in conn:
            return "Excel"
        if "sap" in conn:
            return "SAP"
        if "sql server" in conn or "initial catalog=" in conn:
            return "SQL"
        return "ODBC"
    if provider in {"oledb", "ole db"}:
        if "oracle" in conn:
            return "Oracle"
        if "db2" in conn:
            return "DB2"
        if "excel" in conn or "microsoft.ace.oledb" in conn or "jet.oledb" in conn:
            return "Excel"
        if "sql server" in conn or "sqloledb" in conn or "sqlncli" in conn:
            return "SQL"
        return "OLEDB"

    if "initial catalog=" in conn or ("server=" in conn and "database=" in conn):
        return "SQL"
    if "oracle" in conn:
        return "Oracle"
    if "db2" in conn:
        return "DB2"
    if "excel" in conn or ".xlsx" in conn or ".xls" in conn:
        return "Excel"
    if "sap" in conn:
        return "SAP"

    return (data_provider or None)


def _normalize_sql_for_parsing(command_text):
    if not command_text:
        return ""

    sql_text = re.sub(r"--.*?$", " ", command_text, flags=re.MULTILINE)
    sql_text = re.sub(r"/\*.*?\*/", " ", sql_text, flags=re.DOTALL)
    sql_text = re.sub(r"\s+", " ", sql_text).strip()
    return sql_text


def _extract_cte_names(sql_text):
    cte_names = set()
    if not sql_text:
        return cte_names

    for match in re.finditer(r"\bwith\s+([A-Za-z_][\w]*)\s+as\s*\(", sql_text, flags=re.IGNORECASE):
        cte_names.add(match.group(1).lower())

    for match in re.finditer(r",\s*([A-Za-z_][\w]*)\s+as\s*\(", sql_text, flags=re.IGNORECASE):
        cte_names.add(match.group(1).lower())

    return cte_names


def extract_schema_objects(command_text):
    if not command_text:
        return []

    sql_text = _normalize_sql_for_parsing(command_text)
    cte_names = _extract_cte_names(sql_text)
    found = set()

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
            if schema_name.lower() in cte_names or object_name.lower() in cte_names:
                continue
            if not schema_name.startswith("#") and not object_name.startswith("#"):
                found.add((schema_name, object_name))

    return sorted(found)


def parse_shared_datasource(xml_text=None, item_name=None, item_path=None, row=None):
    connect_string = None
    data_provider = None
    user_name = None

    # Prefer dbo.DataSource table values for Type = 5 items
    if row is not None:
        connect_string = getattr(row, "ConnectString", None) or getattr(row, "OriginalConnectString", None)
        data_provider = getattr(row, "Extension", None)
        user_name = getattr(row, "UserName", None)

    # Fallback to XML only when DataSource table values are missing
    if (not connect_string and not data_provider) and xml_text:
        cleaned_xml = clean_xml_text(xml_text)
        if cleaned_xml:
            root = ET.fromstring(cleaned_xml)
            namespace = get_namespace(root)
            connect_string = node_text(root, namespace, "ConnectString")
            data_provider = node_text(root, namespace, "Extension") or node_text(root, namespace, "DataProvider")
            user_name = node_text(root, namespace, "UserName")

    datasource_type = normalize_datasource_type(data_provider, connect_string)
    database_name = extract_database_name(connect_string, datasource_type)
    server_instance = extract_server_instance(connect_string)

    return {
        "datasource_name": item_name,
        "datasource_path": item_path,
        "connection_string": connect_string,
        "connection_username": user_name,
        "server_instance": server_instance,
        "database_name": database_name,
        "datasource_type": datasource_type,
        "datasource_reference": item_path,
        "data_provider": data_provider,
        "is_shared": True
    }


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
            data_provider = None
            datasource_type = None

            conn_props = ds.find(ns_tag(namespace, "ConnectionProperties"))
            if conn_props is not None:
                connect_string = node_text(conn_props, namespace, "ConnectString")
                user_name = node_text(conn_props, namespace, "UserName")
                data_provider = node_text(conn_props, namespace, "DataProvider") or node_text(conn_props, namespace, "Extension")
                datasource_type = normalize_datasource_type(data_provider, connect_string)
                server_instance = extract_server_instance(connect_string)
                database_name = extract_database_name(connect_string, datasource_type)

            ds_ref_node = ds.find(ns_tag(namespace, "DataSourceReference"))
            datasource_reference = ds_ref_node.text.strip() if ds_ref_node is not None and ds_ref_node.text else None

            datasource_map[ds_name] = {
                "datasource_name": ds_name,
                "datasource_path": datasource_reference,
                "connection_string": connect_string,
                "connection_username": user_name,
                "server_instance": server_instance,
                "database_name": database_name,
                "datasource_type": datasource_type,
                "datasource_reference": datasource_reference,
                "data_provider": data_provider,
                "is_shared": bool(datasource_reference)
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

    return list(datasource_map.values()), queries


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


def get_existing_datasource_id(cursor, ds_row):
    find_sql = f"""
        SELECT TOP 1 DataSource_ID
        FROM {TARGET_RPT_DATASOURCES}
        WHERE ISNULL(DataSource_Name, '') = ISNULL(?, '')
          AND ISNULL(Connection_String_Text, '') = ISNULL(?, '')
          AND ISNULL(Connection_UserName, '') = ISNULL(?, '')
          AND ISNULL(Server_Instance, '') = ISNULL(?, '')
          AND ISNULL(Database_Name, '') = ISNULL(?, '')
          AND ISNULL(DataSource_Type, '') = ISNULL(?, '')
        ORDER BY DataSource_ID
    """
    cursor.execute(
        find_sql,
        ds_row.get("datasource_name"),
        ds_row.get("connection_string"),
        ds_row.get("connection_username"),
        ds_row.get("server_instance"),
        ds_row.get("database_name"),
        ds_row.get("datasource_type")
    )
    existing = cursor.fetchone()
    return existing[0] if existing else None


def insert_datasource(cursor, ds_row):
    existing_id = get_existing_datasource_id(cursor, ds_row)
    if existing_id:
        return existing_id

    insert_sql = f"""
        INSERT INTO {TARGET_RPT_DATASOURCES}
        (
            DataSource_Name,
            Connection_String_Text,
            Connection_UserName,
            Server_Instance,
            Database_Name,
            DataSource_Type,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        OUTPUT INSERTED.DataSource_ID
        VALUES (?, ?, ?, ?, ?, ?, USER_NAME(), GETDATE(), USER_NAME(), GETDATE())
    """
    cursor.execute(
        insert_sql,
        ds_row.get("datasource_name"),
        ds_row.get("connection_string"),
        ds_row.get("connection_username"),
        ds_row.get("server_instance"),
        ds_row.get("database_name"),
        ds_row.get("datasource_type")
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


def load_shared_datasources(cursor, shared_rows):
    datasource_id_by_path = {}
    datasource_id_by_name = {}
    datasource_info_by_path = {}
    datasource_info_by_name = {}
    count = 0

    for row in shared_rows:
        print(f"\nLoading shared datasource: {row.Path}")
        try:
            xml_text = decode_xml_content(row.Content)
            ds_row = parse_shared_datasource(xml_text, row.Name, row.Path, row=row)

            # Skip rows where we still could not identify datasource details
            if not ds_row.get("connection_string") and not ds_row.get("datasource_type"):
                print(f"  Skipped shared datasource because connection details were not found: {row.Path}")
                continue

            datasource_id = insert_datasource(cursor, ds_row)
            count += 1

            if row.Path:
                datasource_id_by_path[row.Path] = datasource_id
                datasource_info_by_path[row.Path] = ds_row

            if row.Name:
                datasource_id_by_name[row.Name] = datasource_id
                datasource_info_by_name[row.Name] = ds_row

            print(
                f"  Inserted/Found DataSource_ID = {datasource_id}, "
                f"Name = {ds_row.get('datasource_name')}, "
                f"Type = {ds_row.get('datasource_type')}, "
                f"Database = {ds_row.get('database_name')}"
            )
        except Exception as ex:
            print(f"  Shared datasource failed and was skipped: {row.Path}")
            print_error(ex)

    return {
        "count": count,
        "id_by_path": datasource_id_by_path,
        "id_by_name": datasource_id_by_name,
        "info_by_path": datasource_info_by_path,
        "info_by_name": datasource_info_by_name
    }


def resolve_datasource_for_report(cursor, report_ds, shared_lookup):
    ds_reference = report_ds.get("datasource_reference")
    ds_name = report_ds.get("datasource_name")

    if ds_reference and ds_reference in shared_lookup["id_by_path"]:
        return (
            shared_lookup["id_by_path"][ds_reference],
            shared_lookup["info_by_path"][ds_reference]
        )

    # Sometimes the report datasource name matches a shared datasource name.
    if ds_name and ds_name in shared_lookup["id_by_name"]:
        return (
            shared_lookup["id_by_name"][ds_name],
            shared_lookup["info_by_name"][ds_name]
        )

    # Fallback for embedded datasource only when no shared datasource match exists.
    datasource_id = insert_datasource(cursor, report_ds)
    return datasource_id, report_ds


def process_report(cursor, source_row, shared_lookup):
    report_name = source_row.Name
    report_type = str(source_row.Type) if source_row.Type is not None else None
    report_path = source_row.Path
    content = source_row.Content

    print(f"\nProcessing report: {report_path}")

    xml_text = decode_xml_content(content)
    if not xml_text:
        raise ValueError("Could not decode Content field into RDL XML.")

    report_datasources, queries = parse_rdl(xml_text)

    report_row = {
        "report_name": report_name,
        "report_type": report_type,
        "business_suite": BUSINESS_SUITE_VALUE,
        "report_path": report_path
    }

    rpt_id = insert_rpt_catalog(cursor, report_row)
    print(f"  Inserted RPT_Catalog row. RPT_ID = {rpt_id}")

    resolved_id_map = {}
    resolved_info_map = {}
    map_count = 0
    query_count = 0
    object_count = 0

    for ds in report_datasources:
        datasource_id, datasource_info = resolve_datasource_for_report(cursor, ds, shared_lookup)
        datasource_name = ds.get("datasource_name")
        resolved_id_map[datasource_name] = datasource_id
        resolved_info_map[datasource_name] = datasource_info

        ensure_datasource_map(cursor, rpt_id, datasource_id)
        map_count += 1
        print(
            f"  Mapped datasource: {datasource_name} -> DataSource_ID = {datasource_id}"
            f" (Reference = {ds.get('datasource_reference')})"
        )

    for q in queries:
        datasource_name = q.get("datasource_name")
        datasource_id = resolved_id_map.get(datasource_name)
        datasource_info = resolved_info_map.get(datasource_name, {})

        if datasource_id is None and len(resolved_id_map) == 1:
            datasource_id = list(resolved_id_map.values())[0]
            datasource_info = list(resolved_info_map.values())[0]

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

        environment_name = get_environment_input()
        apply_environment_config(environment_name)

        print(f"Connecting to source: {SOURCE_SERVER} / {SOURCE_DATABASE}")
        source_conn = get_connection(SOURCE_SERVER, SOURCE_DATABASE)
        print("Connected to source.")

        print(f"Connecting to target: {TARGET_SERVER} / {TARGET_DATABASE}")
        target_conn = get_connection(TARGET_SERVER, TARGET_DATABASE)
        print("Connected to target.")

        report_rows = fetch_reports(source_conn)
        shared_datasource_rows = fetch_shared_datasources(source_conn)

        if not report_rows and not shared_datasource_rows:
            print("No source rows found. Nothing to load.")
            return

        if CLEAR_TARGET_TABLES_BEFORE_LOAD:
            clear_target_tables(target_conn)

        cursor = target_conn.cursor()

        try:
            shared_lookup = load_shared_datasources(cursor, shared_datasource_rows)
            target_conn.commit()
            total_datasources += shared_lookup["count"]
        except Exception as ex:
            target_conn.rollback()
            print("Shared datasource load failed.")
            print_error(ex)
            log_error_to_table(
                target_conn=target_conn,
                process_name=PROCESS_NAME,
                source_table=SOURCE_TABLE,
                target_table=TARGET_RPT_DATASOURCES,
                report_path=None,
                report_name=None,
                error_type=type(ex).__name__,
                error_message=str(ex),
                error_details=traceback.format_exc()
            )
            raise

        for row in report_rows:
            try:
                counts = process_report(cursor, row, shared_lookup)
                target_conn.commit()

                total_catalog += counts["catalog"]
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
