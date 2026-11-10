
import pyodbc
import xml.etree.ElementTree as ET
import re
import os
import subprocess
import traceback
import socket
import ipaddress
from datetime import datetime

# ==============================
# SQL SERVER ENVIRONMENT CONFIGURATION
# ==============================
# Note: Add an extra esacpe character "\" in the Server name
ENVIRONMENT_CONFIG = {
    "DEV": {
        "SOURCES": [
            {
                "SOURCE_SERVER" : "DEVSQL\\STSEA01",
                "SOURCE_DATABASE" : "ReportServerDev",    
            }
        ],
        "TARGET_SERVER" : "DEV436LQC\\SQL2019",
        "TARGET_DATABASE" : "AdventureWorks2019"
        },
    
    "TEST": {
        "SOURCES": [
            {
                "SOURCE_SERVER" : "TESTSQL\\SBEA02",
                "SOURCE_DATABASE" : "ReportServerTST",   
            }
        ],
        "TARGET_SERVER" : "TST126LQC\\SQL2019",
        "TARGET_DATABASE" : "ReportMetadataRepository"
        },

    "PROD": {
        "SOURCES": [
            {
                "SOURCE_SERVER" : "PRDSQL\\SFALC02",
                "SOURCE_DATABASE" : "ReportServerProd",   
            }
        ],
        "TARGET_SERVER" : "PROD43YHS\\SQL2019",
        "TARGET_DATABASE" : "ReportMetadataRepositoryPROD"
        },
    }



# SOURCE AND TARGET TABLES
SOURCE_TABLE = "dbo.Catalog"
SOURCE_DATASOURCE_TABLE = "dbo.DataSource"

TARGET_RPT_CATALOG = "dbo.RPT_Catalog"
TARGET_RPT_PROGRAM_AREA = "dbo.RPT_Program_Area"
TARGET_RPT_DATASOURCES = "dbo.RPT_DataSources"
TARGET_RPT_DATASOURCE_MAP = "dbo.RPT_DataSource_Map"
TARGET_RPT_QUERIES = "dbo.RPT_Queries"
TARGET_RPT_OBJECTS = "dbo.RPT_Objects"
TARGET_RPT_SERVERS = "dbo.RPT_Servers"
TARGET_ERROR_LOG = "dbo.RPT_Error_Log"

ODBC_DRIVER = "ODBC Driver 17 for SQL Server"
LOAD_REPORTS_ONLY = True
CLEAR_TARGET_TABLES_BEFORE_LOAD = False
PROCESS_NAME = "SSRS_RDL_ETL_Load"

# Oracle client configuration used to resolve TNS aliases for non-SQL datasources.
# Update these paths if the Oracle client is installed in a different location.
ORACLE_HOME = r"C:\oracle\WINDOWS.X64_193000_client_home"
TNS_ADMIN = r"C:\oracle\WINDOWS.X64_193000_client_home\network\admin"


# prompts user to enter environment details
def get_environment_input():
    allowed_envs = ", ".join(ENVIRONMENT_CONFIG.keys())

    while True:
        env_value = input(f"Enter environment ({allowed_envs}): ").strip().upper()

        if env_value in ENVIRONMENT_CONFIG:
            return env_value

        print(f"Invalid environament: {env_value}")
        print(f"Please enter one of: {allowed_envs}")

# set Source and Target server and database details based on the selected environment
def apply_environment_config(environment_name):
    global SOURCES, SOURCE_DATABASE, TARGET_SERVER, TARGET_DATABASE

    config = ENVIRONMENT_CONFIG[environment_name]
    SOURCES = config["SOURCES"]
    TARGET_SERVER = config["TARGET_SERVER"]
    TARGET_DATABASE = config["TARGET_DATABASE"]

    print("\nSelected environment configuration")
    print("-" * 60)
    print(f" Environment : {environment_name}")
    print(f" Target Server : {TARGET_SERVER}")
    print(f" Target Database : {TARGET_DATABASE}")

    for source in SOURCES:
        print(f" - {source['SOURCE_SERVER']} / {source['SOURCE_DATABASE']}")
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

def extract_description_credentials(description):
    """Read connection info only from dbo.Catalog.Description.

    Supported examples:
      Data Source=TBQTEST; USER_NAME=TEST_USER
      Data Source=TBQTEST; Username:USER_NAME=TEST_USER
      USER NAME=TEST_USER
    Missing keys return None (SQL NULL). Do not use RDL credentials as fallback.
    """
    if description is None:
        return None, None
    text = str(description).replace("\u00a0", " ")
    if not text.strip():
        return None, None

    conn_match = re.search(r"\bData\s*Source\s*=\s*([^;\r\n]+)", text, re.I)
    connection_string = (
        "Data Source=" + conn_match.group(1).strip()
        if conn_match and conn_match.group(1).strip() else None
    )

    # Match USER_NAME as a key, not as a literal username value.
    user_match = re.search(
        r"\bUSER(?:[ _]*NAME)\s*[:=]\s*([^;\r\n]+)", text, re.I
    )
    if not user_match:
        user_match = re.search(
            r"\bUsername\s*[:=]\s*(?!USER(?:[ _]*NAME)\s*=)([^;\r\n]+)",
            text, re.I
        )
    username = user_match.group(1).strip() if user_match else None
    return connection_string, (username or None)


def extract_catalog_datasource_credentials(description, parsed):
    """Prefer Catalog.Description, then shared datasource XML ConnectString.

    Missing username in both sources is None (SQL NULL). Never use
    stored-credential status markers as usernames.
    """
    desc_conn, desc_user = extract_description_credentials(description)
    xml_conn = parsed.get("connection_string") if parsed else None
    xml_user = extract_description_credentials(xml_conn)[1]
    return desc_conn or xml_conn, desc_user or xml_user


def fetch_catalog_rows(source_conn):
    where_clause = "WHERE Content IS NOT NULL"
    if LOAD_REPORTS_ONLY:
        where_clause += " AND Type in (2, 5)"

    sql = f"""
        SELECT
            ItemID,
            Name,
            Type,
            Path,
            Content,
            Description
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

# Reverse DNS cache avoids repeated lookups for the same IP during an ETL run.
_SQL_IP_HOST_CACHE = {}


def normalize_sql_datasource_dns(ds_row):
    """Replace SQL Server Data Source IP with its PTR hostname in both target fields.

    Keeps connection-string options, tcp: prefix, port, and named instance.
    Leaves data unchanged if reverse DNS has no PTR record or the provider is not SQL.
    """
    provider = str(ds_row.get("datasource_type") or "").strip().upper()
    if provider not in {"SQL", "SQL SERVER", "SQLSERVER", "SQLAZURE", "AZURESQL"}:
        return ds_row

    connection = ds_row.get("connection_string")
    if not connection:
        return ds_row

    pattern = re.compile(
        r"(?i)(\b(?:Data\s+Source|Datasource|Server|Address|Addr|Network\s+Address)\s*=\s*)"
        r"(?P<tcp>tcp:)?(?P<ip>\d{1,3}(?:\.\d{1,3}){3})"
        r"(?P<suffix>(?:\\[^;,]+|,\d+)*)"
        r"(?=\s*(?:;|$))"
    )
    match = pattern.search(connection)
    if not match:
        return ds_row

    ip = match.group("ip")
    try:
        ipaddress.IPv4Address(ip)
    except ValueError:
        print(f"  WARNING: Invalid SQL datasource IP {ip}; retaining original values")
        return ds_row

    if ip not in _SQL_IP_HOST_CACHE:
        try:
            hostname = socket.gethostbyaddr(ip)[0].rstrip(".")
            _SQL_IP_HOST_CACHE[ip] = hostname if hostname and hostname != ip else None
        except (OSError, socket.error) as ex:
            print(f"  WARNING: Reverse DNS lookup failed for {ip}: {ex}")
            _SQL_IP_HOST_CACHE[ip] = None

    hostname = _SQL_IP_HOST_CACHE[ip]
    if not hostname:
        return ds_row

    # Work on the original row so all downstream inserts and matching see the same data.
    ds_row["connection_string"] = (
        connection[:match.start("ip")] + hostname + connection[match.end("ip"):]
    )
    ds_row["server_instance"] = (
        (match.group("tcp") or "") + hostname + (match.group("suffix") or "")
    )
    print(f"  SQL datasource DNS resolved: {ip} -> {hostname}")
    return ds_row


def resolve_non_sql_server_instance(service_name, timeout=10):
    """Resolve an Oracle/TNS service alias and return HOST:PORT.

    Example: service alias JATP -> testa001.avc.dev:1234
    Returns None when the alias cannot be resolved so the ETL can continue.
    """
    if not service_name:
        return None

    service_name = str(service_name).strip()
    if not service_name:
        return None

    # If the connection string already contains an EZCONNECT host:port value,
    # preserve it without invoking tnsping.
    direct = re.search(r"(?:@|//)?([A-Za-z0-9_.-]+):(\d+)", service_name)
    if direct:
        return f"{direct.group(1)}:{direct.group(2)}"

    oracle_env = {
        **os.environ,
        "ORACLE_HOME": ORACLE_HOME,
        "TNS_ADMIN": TNS_ADMIN,
        "PATH": ORACLE_HOME + r"\bin;" + os.environ.get("PATH", "")
    }

    tnsping_exe = os.path.join(ORACLE_HOME, "bin", "tnsping.exe")

    try:
        result = subprocess.run(
            [tnsping_exe, service_name],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=oracle_env,
            check=False
        )

        output = (result.stdout or "") + (result.stderr or "")

        # Typical tnsping output contains:
        # (HOST = testa001.avc.dev)(PORT = 1234)
        match = re.search(
            r"\(HOST\s*=\s*([^\)\s]+)\).*?\(PORT\s*=\s*(\d+)\)",
            output,
            flags=re.IGNORECASE | re.DOTALL
        )

        if match:
            host = match.group(1).strip()
            port = match.group(2).strip()
            return f"{host}:{port}"

        print(f"  WARNING: Could not resolve HOST:PORT for service '{service_name}' using tnsping.")
        return None

    except Exception as ex:
        print(f"  WARNING: tnsping failed for service '{service_name}': {ex}")
        return None

def extract_database_name(connect_string):
    if not connect_string:
        return None

    patterns = [
        r"Initial\s+Catalog\s*=\s*([^;]+)",
        r"Database\s*=\s*([^;]+)",
        r"Catalog\s*=\s*([^;]+)",
        r"DBQ\s*=\s*([^;]+)",
        r"Service Name\s*=\s*([^;]+)",
        r"SID\s*=\s*([^;]+)"
        ]

    for pattern in patterns:
        match = re.search(pattern, connect_string, re.IGNORECASE)
        if match:
            return match.group(1).strip()    
    return None

def normalize_datasource_type(datasource_type):
    if not datasource_type:
        return none

    datasource_type = datasource_type.strip()

    if datasource_type.upper() == "SQL":
        return "SQL SERVER"

    return datasource_type.upper()

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
                result["datasource_type"] = normalize_datasource_type(value)

    # First extract the Data Source / Server value from the connection string.
    # For SQL Server this value is the Server_Instance as-is.
    connection_server = extract_server_instance(result["connection_string"])
    result["database_name"] = extract_database_name(result["connection_string"])

    datasource_type_upper = (result["datasource_type"] or "").strip().upper()

    # SQL Server datasource types retain the existing server/instance value.
    sql_types = {"SQL", "SQLSERVER", "SQL SERVER", "SQLAZURE", "AZURESQL"}

    if datasource_type_upper in sql_types:
        result["server_instance"] = connection_server
    else:
        # For Oracle/non-SQL datasource types, the Data Source is commonly a
        # TNS/service alias. Resolve it and store HOST:PORT in Server_Instance.
        resolved_host_port = resolve_non_sql_server_instance(connection_server)
        result["server_instance"] = resolved_host_port or connection_server

    if (
        datasource_type_upper == "ORACLE"
        and not result["database_name"]
    ):
        # Preserve the Oracle service/database identifier separately.
        result["database_name"] = connection_server
        
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
            shared_cat.Content AS SharedDataSourceContent,
            shared_cat.Description AS SharedDataSourceDescription
        FROM {SOURCE_DATASOURCE_TABLE} ds
        INNER JOIN {SOURCE_TABLE} report_cat
            ON ds.ItemID = report_cat.ItemID
           AND report_cat.Type = 2
        INNER JOIN {SOURCE_TABLE} shared_cat
            ON ds.Link = shared_cat.ItemID
           AND shared_cat.Type = 5
    """

    cursor = source_conn.cursor()
    print("Reading shared datasource definitions from dbo.DataSource.Link -> dbo.Catalog.ItemID...")
    cursor.execute(sql)
    rows = cursor.fetchall()

    lookup = {}
    for row in rows:
        parsed = parse_shared_datasource_content(row.SharedDataSourceContent)
        description_conn, description_user = extract_catalog_datasource_credentials(
            row.SharedDataSourceDescription, parsed
        )
        report_item_id = str(row.ReportItemID).upper()
        report_ds_name = row.ReportDataSourceName

        lookup[(report_item_id, report_ds_name)] = {
            "datasource_name": row.SharedDataSourceName,
            "datasource_name": (
                row.SharedDataSourceName.upper()
                if row.SharedDataSourceName
                else None
                ),
            "report_datasource_name": report_ds_name,
            "shared_datasource_path": row.SharedDataSourcePath,
            "shared_datasource_item_id": str(row.SharedDataSourceItemID).upper() if row.SharedDataSourceItemID else None,
            "connection_string": (
                description_conn.upper()
                if description_conn
                else None
                ),
            "connection_username": description_user,
            #"server_instance": parsed.get("server_instance"),
            "server_instance": (
                parsed.get("server_instance").upper()
                if parsed.get("server_instance")
                else None
                ),
            #"database_name": parsed.get("database_name"),
            "database_name": (
                parsed.get("database_name").upper()
                if parsed.get("database_name")
                else None
                ),
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
            shared_cat.Content AS SharedDataSourceContent,
            shared_cat.Description AS SharedDataSourceDescription
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
        description_conn, description_user = extract_catalog_datasource_credentials(
            row.SharedDataSourceDescription, parsed
        )

        # Prefer dbo.Catalog.Name because this is the actual shared datasource object name.
        datasource_name = row.SharedDataSourceName or row.DataSourceTableName

        datasources.append({
            "datasource_name": datasource_name,
            "report_datasource_name": row.DataSourceTableName,
            "shared_datasource_path": row.SharedDataSourcePath,
            "shared_datasource_item_id": str(row.SharedDataSourceItemID).upper() if row.SharedDataSourceItemID else None,
            "connection_string": description_conn,
            "connection_username": description_user,
            "server_instance": parsed.get("server_instance"),
            "database_name": parsed.get("database_name"),
            "datasource_type": parsed.get("datasource_type"),
            "datasource_reference": row.SharedDataSourcePath,
            "is_shared_datasource": 1
        })

    print(f"Standalone Catalog.Type = 5 shared datasources fetched: {len(datasources)}")
    return datasources

def apply_shared_datasource_values(report_item_id, datasources, shared_datasource_lookup):
    """Replace report-level shared datasource placeholders with actual Catalog.Type=5 details."""
    report_item_id = str(report_item_id).upper()
    updated = []

    for ds in datasources:
        report_ds_name = ds.get("datasource_name")
        shared = shared_datasource_lookup.get((report_item_id, report_ds_name))

        if not shared and report_ds_name:
            shared = shared_datasource_lookup.get((report_item_id, report_ds_name.upper()))

        if shared:
            merged = dict(ds)
            merged.update(shared)

            merged["report_datasource_name"] = report_ds_name
            merged["shared_datasource_name"] = shared.get("datasource_name")
            merged["datasource_name"] = shared.get("datasource_name")
            merged["is_shared_datasource"] = 1
            
            updated.append(merged)
            
            print(
                f"  Resolved shared datasource: report DS '{report_ds_name}' "
                f"-> shared DS '{shared.get('datasource_name')}'"
            )
        else:
            ds["report_datasource_name"] = report_ds_name
            ds["shared_datasource_name"] = None
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

    # Normalize CTE names for consistent comparison
    cte_names = {
        name.lower()
        for name in _extract_cte_names(sql_text)
    }

    found = set()

    # =========================================================
    # 1. Collect table aliases
    #
    # Examples:
    #   FROM dbo.Customer AS C
    #   FROM dbo.Customer C
    #   JOIN sales.Orders O
    #
    # These aliases are used only for filtering.
    # They are NOT loaded into RPT_Objects.
    # =========================================================

    alias_names = set()

    alias_pattern = r"""
        \b
        (?:
            from
            |join
            |inner\s+join
            |left\s+(?:outer\s+)?join
            |right\s+(?:outer\s+)?join
            |full\s+(?:outer\s+)?join
            |cross\s+join
        )
        \s+

        # Optional database
        (?:
            \[?[A-Za-z_][\w$#]*\]?
            \s*\.\s*
        )?

        # Schema
        \[?[A-Za-z_][\w$#]*\]?
        \s*\.\s*

        # Object
        \[?[A-Za-z_][\w$#]*\]?

        # Optional AS
        \s+
        (?:AS\s+)?

        # Alias
        \[?([A-Za-z_][\w$#]*)\]?
    """

    sql_keywords = {
        "where",
        "join",
        "inner",
        "left",
        "right",
        "full",
        "cross",
        "outer",
        "on",
        "group",
        "order",
        "having",
        "union",
        "except",
        "intersect",
        "apply"
    }

    for alias_match in re.finditer(
        alias_pattern,
        sql_text,
        flags=re.IGNORECASE | re.VERBOSE
    ):
        alias_name = alias_match.group(1)

        if alias_name:
            alias_name = alias_name.strip("[] ").lower()

            if alias_name not in sql_keywords:
                alias_names.add(alias_name)

    # =========================================================
    # 2. Collect derived-table / subquery aliases
    #
    # Examples:
    #
    # FROM (
    #     SELECT ...
    # ) xx
    #
    # LEFT JOIN (
    #     SELECT ...
    # ) yy
    #
    # FROM (
    #     SELECT ...
    # ) AS x
    #
    # xx / yy / x are aliases and should NOT become schemas.
    # =========================================================

    derived_alias_pattern = r"""
        \)
        \s*
        (?:AS\s+)?
        \[?([A-Za-z_][\w$#]*)\]?
    """

    for alias_match in re.finditer(
        derived_alias_pattern,
        sql_text,
        flags=re.IGNORECASE | re.VERBOSE
    ):
        alias_name = alias_match.group(1)

        if alias_name:
            alias_name = alias_name.strip("[] ").lower()

            if alias_name not in sql_keywords:
                alias_names.add(alias_name)

    # =========================================================
    # 3. Standard SQL database-object patterns
    #
    # Supports:
    #
    #   schema.object
    #   [schema].[object]
    #   database.schema.object
    #   [database].[schema].[object]
    #
    # Captures schema + object only.
    # =========================================================

    patterns = [

        # FROM / JOIN / APPLY
        r"""
        \b
        (
            from
            |join
            |inner\s+join
            |left\s+(?:outer\s+)?join
            |right\s+(?:outer\s+)?join
            |full\s+(?:outer\s+)?join
            |cross\s+join
            |cross\s+apply
            |outer\s+apply
        )
        \s+
        (?:\[?[\w$#]+\]?\s*\.\s*)?
        \[?([\w$#]+)\]?
        \s*\.\s*
        \[?([\w$#]+)\]?
        """,

        # UPDATE
        r"""
        \bupdate\s+
        (?:\[?[\w$#]+\]?\s*\.\s*)?
        \[?([\w$#]+)\]?
        \s*\.\s*
        \[?([\w$#]+)\]?
        """,

        # INSERT INTO
        r"""
        \binsert\s+into\s+
        (?:\[?[\w$#]+\]?\s*\.\s*)?
        \[?([\w$#]+)\]?
        \s*\.\s*
        \[?([\w$#]+)\]?
        """,

        # MERGE
        r"""
        \bmerge(?:\s+into)?\s+
        (?:\[?[\w$#]+\]?\s*\.\s*)?
        \[?([\w$#]+)\]?
        \s*\.\s*
        \[?([\w$#]+)\]?
        """,

        # DELETE
        r"""
        \bdelete\s+from\s+
        (?:\[?[\w$#]+\]?\s*\.\s*)?
        \[?([\w$#]+)\]?
        \s*\.\s*
        \[?([\w$#]+)\]?
        """,

        # EXEC / EXECUTE stored procedure
        r"""
        \bexec(?:ute)?\s+
        (?:\[?[\w$#]+\]?\s*\.\s*)?
        \[?([\w$#]+)\]?
        \s*\.\s*
        \[?([\w$#]+)\]?
        """
    ]

    # =========================================================
    # 4. Process standard SQL patterns
    # =========================================================

    for pattern in patterns:

        matches = re.findall(
            pattern,
            sql_text,
            flags=re.IGNORECASE | re.VERBOSE
        )

        for match in matches:

            # FROM/JOIN regex contains:
            # keyword + schema + object
            if len(match) == 3:
                schema_name = match[1].strip("[] ")
                object_name = match[2].strip("[] ")

            else:
                schema_name = match[0].strip("[] ")
                object_name = match[1].strip("[] ")

            schema_lower = schema_name.lower()
            object_lower = object_name.lower()

            # Ignore temporary objects
            if (
                schema_name.startswith("#")
                or object_name.startswith("#")
            ):
                continue

            # Ignore CTE references
            if (
                schema_lower in cte_names
                or object_lower in cte_names
            ):
                continue

            # Ignore alias.column references
            #
            # Example:
            #   xx.abcid
            #   yy.barcode
            #   x.first_name
            #
            # xx / yy / x are aliases, not schemas.
            if schema_lower in alias_names:
                continue

            found.add(
                (
                    schema_name,
                    object_name
                )
            )

    # =========================================================
    # Handle unqualified SQL Server objects
    # Default schema = dbo
    # =========================================================

    unqualified_pattern = r"""
        \b
        (
            from
            |join
            |inner\s+join
            |left\s+(?:outer\s+)?join
            |right\s+(?:outer\s+)?join
            |full\s+(?:outer\s+)?join
            |cross\s+join
        )
        \s+
        \[?([A-Za-z_][\w$#]*)\]?
        (?=
            \s
            |$
            |,
        )
    """

    for match in re.finditer(
        unqualified_pattern,
        sql_text,
        flags=re.IGNORECASE | re.VERBOSE
    ):

        object_name = match.group(2).strip("[] ")
        object_lower = object_name.lower()

        # Ignore temp tables
        if object_name.startswith("#"):
            continue

        # Ignore CTE names
        if object_lower in cte_names:
            continue

        # Ignore SQL keywords
        if object_lower in sql_keywords:
            continue

        # Ignore derived/subquery syntax
        if object_name.startswith("("):
            continue

        # Default SQL Server schema
        schema_name = "dbo"

        found.add(
            (
                schema_name,
                object_name
            )
        )

    # =========================================================
    # 5. Handle direct stored procedure notation
    #
    # Examples:
    #
    #   database.schema.proc_name
    #   schema.proc_name
    # =========================================================

    sql_clean = sql_text.strip().rstrip(";")

    direct_sp_match = re.match(
        r"""
        ^
        (?:\[?([\w$#]+)\]?\.)?
        \[?([\w$#]+)\]?
        \.
        \[?([\w$#]+)\]?
        $
        """,
        sql_clean,
        flags=re.IGNORECASE | re.VERBOSE
    )

    if direct_sp_match:

        database_name = direct_sp_match.group(1)
        schema_name = direct_sp_match.group(2)
        object_name = direct_sp_match.group(3)

        schema_name = schema_name.strip("[] ")
        object_name = object_name.strip("[] ")

        schema_lower = schema_name.lower()
        object_lower = object_name.lower()

        if not (
            schema_name.startswith("#")
            or object_name.startswith("#")
        ):

            if (
                schema_lower not in cte_names
                and object_lower not in cte_names
                and schema_lower not in alias_names
            ):
                found.add(
                    (
                        schema_name,
                        object_name
                    )
                )

    # =========================================================
    # 6. Parenthesis-aware Oracle FROM parsing
    #
    # Important:
    #
    # The old logic used:
    #
    #     from_block.split(",")
    #
    # which caused SELECT columns inside nested subqueries to
    # be interpreted as tables.
    #
    # Example:
    #
    #   xx.abcid,
    #   xx.barcode,
    #   xx.last_name
    #
    # could incorrectly become:
    #
    #   Schema_Name = xx
    #   Object_Name = abcid
    #
    # The new logic splits commas only at the current query level.
    # =========================================================

    def get_from_blocks(sql):
        """
        Find FROM blocks while respecting nested parentheses.
        """

        blocks = []

        from_matches = re.finditer(
            r"\bfrom\b",
            sql,
            flags=re.IGNORECASE
        )

        for from_match in from_matches:

            start = from_match.end()
            i = start
            depth = 0
            in_string = False

            while i < len(sql):

                char = sql[i]

                # ---------------------------------------------
                # Handle SQL string literals
                # ---------------------------------------------
                if char == "'":

                    # SQL escaped quote:
                    # 'John''s'
                    if (
                        in_string
                        and i + 1 < len(sql)
                        and sql[i + 1] == "'"
                    ):
                        i += 2
                        continue

                    in_string = not in_string
                    i += 1
                    continue

                if in_string:
                    i += 1
                    continue

                # ---------------------------------------------
                # Track nested parentheses
                # ---------------------------------------------
                if char == "(":
                    depth += 1
                    i += 1
                    continue

                if char == ")":

                    # Closing parenthesis belonging to the
                    # query that contains this FROM
                    if depth == 0:
                        break

                    depth -= 1
                    i += 1
                    continue

                # ---------------------------------------------
                # Stop only at clauses at this same query level
                # ---------------------------------------------
                if depth == 0:

                    remaining = sql[i:]

                    clause_match = re.match(
                        r"""
                        \s*
                        (
                            where\b
                            |group\s+by\b
                            |order\s+by\b
                            |having\b
                            |union\b
                            |intersect\b
                            |except\b
                            |inner\s+join\b
                            |left\s+(?:outer\s+)?join\b
                            |right\s+(?:outer\s+)?join\b
                            |full\s+(?:outer\s+)?join\b
                            |cross\s+join\b
                            |cross\s+apply\b
                            |outer\s+apply\b
                            |join\b
                        )
                        """,
                        remaining,
                        flags=re.IGNORECASE | re.VERBOSE
                    )

                    if clause_match:
                        break

                i += 1

            block = sql[start:i].strip()

            if block:
                blocks.append(block)

        return blocks

    def split_top_level_commas(text):
        """
        Split comma-separated FROM objects only when the comma
        occurs outside nested parentheses.
        """

        entries = []
        current = []
        depth = 0
        in_string = False
        i = 0

        while i < len(text):

            char = text[i]

            # ---------------------------------------------
            # Handle SQL string literals
            # ---------------------------------------------
            if char == "'":

                if (
                    in_string
                    and i + 1 < len(text)
                    and text[i + 1] == "'"
                ):
                    current.append(char)
                    current.append(text[i + 1])
                    i += 2
                    continue

                in_string = not in_string
                current.append(char)
                i += 1
                continue

            if not in_string:

                if char == "(":
                    depth += 1

                elif char == ")":
                    depth = max(
                        0,
                        depth - 1
                    )

                elif char == "," and depth == 0:

                    entry = "".join(current).strip()

                    if entry:
                        entries.append(entry)

                    current = []
                    i += 1
                    continue

            current.append(char)
            i += 1

        entry = "".join(current).strip()

        if entry:
            entries.append(entry)

        return entries

    oracle_from_matches = get_from_blocks(sql_text)

    for from_block in oracle_from_matches:

        entries = split_top_level_commas(
            from_block
        )

        for entry in entries:

            entry = entry.strip()

            single_source_alias_match = re.match(
                r"""
                ^
                \[?([A-Za-z_][\w$#]*)\]?
                \s+
                (?:AS\s+)?
                \[?([A-Za-z_][\w$#]*)\]?
                (?=
                    \s
                    |$
                )
                """,
                entry,
                flags=re.IGNORECASE | re.VERBOSE
            )

            if single_source_alias_match:

                source_name = single_source_alias_match.group(1)
                alias_name = single_source_alias_match.group(2)

                source_lower = source_name.lower()
                alias_lower = alias_name.lower()

                # Avoid SQL keywords being treated as aliases
                if alias_lower not in sql_keywords:
                    alias_names.add(alias_lower)

                if source_lower in cte_names:
                    continue


            # ---------------------------------------------
            # Ignore derived tables / subqueries
            #
            # Example:
            #
            # FROM (
            #     SELECT ...
            # ) xx
            #
            # xx is an alias, not a physical DB object.
            # ---------------------------------------------

            if entry.startswith("("):
                continue

            # ---------------------------------------------
            # Match physical DB objects
            #
            # Supports:
            #
            #   schema.table
            #   schema.table alias
            #   schema.table AS alias
            #
            #   database.schema.table
            #   database.schema.table alias
            #
            # Alias is intentionally ignored.
            # ---------------------------------------------

            match = re.match(
                r"""
                ^
                (?:
                    \[?([A-Za-z_][\w$#]*)\]?
                    \s*\.\s*
                )?
                \[?([A-Za-z_][\w$#]*)\]?
                \s*\.\s*
                \[?([A-Za-z_][\w$#]*)\]?
                (?=
                    \s
                    |$
                )
                """,
                entry,
                flags=re.IGNORECASE | re.VERBOSE
            )

            if not match:
                continue

            database_name = match.group(1)
            schema_name = match.group(2)
            object_name = match.group(3)

            schema_name = schema_name.strip("[] ")
            object_name = object_name.strip("[] ")

            schema_lower = schema_name.lower()
            object_lower = object_name.lower()

            # Ignore temporary objects
            if (
                schema_name.startswith("#")
                or object_name.startswith("#")
            ):
                continue

            # Ignore CTE references
            if (
                schema_lower in cte_names
                or object_lower in cte_names
            ):
                continue

            # Ignore alias.column references
            if schema_lower in alias_names:
                continue

            found.add(
                (
                    schema_name,
                    object_name
                )
            )

    # Final protection against alias.column being stored
    found = {
        (schema_name, object_name)
        for schema_name, object_name in found
        if schema_name.lower() not in alias_names
    }

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
                    datasource_type = normalize_datasource_type(dp_node.text)
                elif ext_node is not None and ext_node.text:
                    datasource_type = normalize_datasource_type(dp_node.text)

                server_instance = extract_server_instance(connect_string)
                database_name = extract_database_name(connect_string)

            ds_ref_node = ds.find(ns_tag(namespace, "DataSourceReference"))
            datasource_reference = ds_ref_node.text.strip() if ds_ref_node is not None and ds_ref_node.text else None

            datasource_map[ds_name.upper()] = {
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
                else:
                    datasource_name = None

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
        if dsn:
            dsn_key = dsn.upper()

            if dsn_key not in datasource_map:
                raise ValueError(
                    f"Datasource could not be resolved for dataset: {q.get('dataset_name')}. "
                    f"Dataset DataSourceName={dsn}. "
                    f"Available datasources={[ds.get('datasource_name') for ds in datasources]}"
                    )            

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

""" Use this when we need to delete tables data instead of Truncating tables"""
def clear_target_tables(target_conn):
    cursor = target_conn.cursor()
    print("Clearing target tables in dependency order...")
    cursor.execute(f"DELETE FROM {TARGET_RPT_OBJECTS}")
    cursor.execute(f"DELETE FROM {TARGET_RPT_QUERIES}")
    cursor.execute(f"DELETE FROM {TARGET_RPT_DATASOURCE_MAP}")
    # RPT_DataSources is a preloaded lookup table; never delete it.
    cursor.execute(f"DELETE FROM {TARGET_RPT_CATALOG}")
    target_conn.commit()
    print("Target tables cleared.")


# Source database -> SSRS access method. Used for read-only datasource
# lookups and to disambiguate DATA_SOURCES folders by RPT_Program_Area.
# Verify these values against the actual source environments before execution.
RPT_ACCESS_METHOD_BY_SOURCE_DATABASE = {
    "REPORTSERVERDEV": "AD",
    "REPORTSERVERTST": "DEVDS",
    "REPORTSERVERTEST": "NYSDS",  # Retained from original script
    # Add actual ReportServer$SSRS19DEV source with VERIFIED AD/DEVDS mapping:
    # "REPORTSERVER$SSRS19DEV": "DEVDS",
    # Add REPORTSERVERPROD here with its verified access method if needed.
}


def get_rpt_access_method(source_database):
    if not source_database:
        raise ValueError("Source database name cannot be blank")
    source_key = str(source_database).strip().upper()
    access_method = RPT_ACCESS_METHOD_BY_SOURCE_DATABASE.get(source_key)
    if access_method is None:
        raise ValueError(
            f"No SSRS access-method mapping configured for source database {source_database!r}. "
            "Check RPT_ACCESS_METHOD_BY_SOURCE_DATABASE."
        )
    return access_method

def resolve_rpt_folder_id(cursor, report_path, source_database):
    """
    Resolve RPT_Folder_ID from RPT_Program_Area.
    Missing or duplicate folder mappings raise an error.
    """

    # -------------------------------------------------------
    # STEP 1: Validate and normalize report path
    # -------------------------------------------------------

    if not report_path:
        raise ValueError(
            "Cannot resolve RPT_Folder_ID: report path is blank"
        )

    normalized_path = str(report_path).replace("\\", "/").strip()

    path_parts = [
        part.strip()
        for part in normalized_path.split("/")
        if part.strip()
    ]

    if len(path_parts) < 2:
        raise ValueError(
            f"Invalid SSRS report path: {report_path!r}"
        )

    # -------------------------------------------------------
    # STEP 2: Identify top-level folder
    # -------------------------------------------------------

    top_folder = path_parts[0].strip().upper()

    # -------------------------------------------------------
    # STEP 3: Resolve the source's access method only when required.
    # DATA_SOURCES has two entries in RPT_Program_Area; other folders are unique.
    # No RPT_ACCESS_METHOD / SSRS_Access_Method column is read from this table.
    # -------------------------------------------------------

    def lookup_folder(folder_name):
        folder_name = str(folder_name).strip()
        if folder_name.upper() == "DATA_SOURCES":
            access_method = get_rpt_access_method(source_database).strip().upper()
            program_area = f"REPORTING SERVICES ({access_method})"
            sql = f"""
                SELECT RPT_Folder_ID
                FROM {TARGET_RPT_PROGRAM_AREA}
                WHERE UPPER(LTRIM(RTRIM(RPT_Folder_Name))) = UPPER(?)
                  AND UPPER(LTRIM(RTRIM(RPT_Program_Area))) = UPPER(?)
            """
            cursor.execute(sql, folder_name, program_area)
        else:
            sql = f"""
                SELECT RPT_Folder_ID
                FROM {TARGET_RPT_PROGRAM_AREA}
                WHERE UPPER(LTRIM(RTRIM(RPT_Folder_Name))) = UPPER(?)
            """
            cursor.execute(sql, folder_name)

        matches = [row[0] for row in cursor.fetchall()]
        if len(matches) > 1:
            raise ValueError(
                f"Duplicate folder mapping: Folder={folder_name!r}, "
                f"Source_Database={source_database!r}, IDs={matches!r}"
            )
        if not matches and folder_name.upper() == "DATA_SOURCES":
            raise ValueError(
                f"No DATA_SOURCES folder found for Source_Database={source_database!r}, "
                f"RPT_Program_Area={program_area!r}. "
                "Verify the database-to-access-method mapping."
            )
        return matches[0] if matches else None

    # -------------------------------------------------------
    # STEP 6: Determine candidate folders
    # -------------------------------------------------------

    candidates = []

    print("\n===== STEP 6 CONDITION CHECK =====")
    print("top_folder:", repr(top_folder))
    print("Normalized Top Folder:", repr(top_folder.replace(" ", "").upper()))
    print("Path parths:", path_parts)
    print("Number of Path Parts:", len(path_parts))
    print("SBT Condition Result:",
          top_folder.replace(" ", "").upper()
          == "REPORTSFOREXTERNALCUSTOMERS"
          )
    print("=======================================\n")

    if top_folder == "BAQA":

        if len(path_parts) == 2:

            # Root-level BAQA report
            # /BAQA/sample_report -> BAQA

            candidates = [
                path_parts[0]
            ]

        else:

            # BAQA report inside a subfolder
            # /BAQA/AUDIT/test_report -> BAQA/AUDIT

            candidates = [
                "/".join(path_parts[:2])
            ]

    elif top_folder.strip().upper() in (
        "REPORTS FOR EXTERNAL CUSTOMERS"
    ):

        print("*** SREPORTS for EXTERNAL CUSTOMERS CONDITION MATCHED ***")

        if len(path_parts) == 2:

            # Report directly under root folder
            candidates = [path_parts[0]]

        else:

        # Subfolder report
        # Exclude the last component (report name)
            candidates = [
                "/".join(path_parts[:depth])
                for depth in range(len(path_parts) - 1, 1, -1)
            ]
        
        print("REPORTS for EXTERNAL CUSTOMERS:", candidates)

    else:

        # All other reports use the top-level folder.
        # /Finance/Revenue/Report -> Finance

        candidates = [
            path_parts[0]
        ]

    # -------------------------------------------------------
    # STEP 7: Debug folder selection
    # -------------------------------------------------------

    print("\n========== FOLDER RESOLUTION DEBUG ==========")
    print("Report Path:", repr(report_path))
    print("Path Parts:", path_parts)
    print("Top Folder:", repr(top_folder))
    print("Candidate Folders:", candidates)
    print("Source Database:", repr(source_database))
    print("Folder lookup: DATA_SOURCES uses RPT_Program_Area; other folders use RPT_Folder_Name")

    # -------------------------------------------------------
    # STEP 8: Resolve RPT_Folder_ID
    # -------------------------------------------------------

    for folder_name in candidates:

        folder_id = lookup_folder(folder_name)

        print(
            f"Checking Folder={folder_name!r}, "
            f"Found ID={folder_id!r}"
        )

        if folder_id is not None:

            print(
                f"RESOLVED RPT_Folder_ID: "
                f"Report={report_path} | "
                f"Folder={folder_name} | "
                f"Folder_ID={folder_id}"
            )

            print("=============================================\n")

            return folder_id

    # -------------------------------------------------------
    # STEP 9: Raise error if no matching folder found
    # -------------------------------------------------------

    raise ValueError(
        f"No matching RPT_Folder_ID found for "
        f"RPT_Path={report_path!r}, "
        f"Candidate_Folders={candidates!r}, "
        f"Source_Database={source_database!r}, "
        f"Lookup=folder name (and program area for DATA_SOURCES). "
        f"Verify {TARGET_RPT_PROGRAM_AREA}."
    )

def insert_rpt_catalog(cursor, report_row, source_database):
    folder_id = resolve_rpt_folder_id(
        cursor, report_row["report_path"], source_database
    )
    sql = f"""
        INSERT INTO {TARGET_RPT_CATALOG}
        (
            RPT_Name,
            RPT_Type,
            RPT_Folder_ID,
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
        folder_id,
        report_row["report_path"]
    )
    return cursor.fetchone()[0]


def _first_existing_column(table_columns, *candidate_names):
    for name in candidate_names:
        if name.lower() in table_columns:
            return name
    return None



def lookup_existing_datasource(cursor, ds_row, rpt_datasource_columns, source_database):
    """Read-only lookup of a preloaded RPT_DataSources row.

    Resolve by datasource name and source access method (when available).
    For shared datasources, also use DataSource_Path when that column exists.
    Never insert, update, or choose an arbitrary duplicate datasource.
    """
    name = ds_row.get("datasource_name") or ds_row.get("shared_datasource_name")
    if not name or not str(name).strip():
        raise ValueError("Datasource lookup failed: datasource name is blank")

    sql = f"""
        SELECT DataSource_ID
        FROM {TARGET_RPT_DATASOURCES}
        WHERE UPPER(LTRIM(RTRIM(DataSource_Name))) = UPPER(?)
    """
    params = [str(name).strip()]

    access_col = _first_existing_column(rpt_datasource_columns, "SSRS_Access_Method")
    if access_col:
        sql += f" AND UPPER(LTRIM(RTRIM([{access_col}]))) = UPPER(?)"
        params.append(get_rpt_access_method(source_database))

    shared_path = ds_row.get("shared_datasource_path")
    path_col = _first_existing_column(
        rpt_datasource_columns, "DataSource_Path", "Shared_DataSource_Path"
    )
    if shared_path and path_col:
        sql += f" AND UPPER(LTRIM(RTRIM([{path_col}]))) = UPPER(?)"
        params.append(str(shared_path).strip())

    cursor.execute(sql, *params)
    ids = [row[0] for row in cursor.fetchall()]
    if not ids:
        raise ValueError(
            f"No preloaded RPT_DataSources match: name={name!r}, "
            f"source_database={source_database!r}, "
            f"access_method={get_rpt_access_method(source_database) if access_col else 'N/A'}, "
            f"shared_path={shared_path!r}. "
            "Populate/correct the lookup table before rerunning the ETL."
        )
    if len(ids) != 1:
        raise ValueError(
            f"Ambiguous preloaded datasource lookup: name={name!r}, "
            f"source_database={source_database!r}, "
            f"shared_path={shared_path!r}, matching DataSource_IDs={ids!r}"
        )
    return ids[0]


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
            Server_Instance,
            Database_Name,
            DataSource_Type,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        SELECT DISTINCT
            ds.Server_Instance,
            o.Database_Name,
            ds.DataSource_Type,
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
              WHERE ISNULL(s.Server_Instance, '') = ISNULL(ds.Server_Instance, '')
                AND ISNULL(s.Database_Name, '') = ISNULL(o.Database_Name, '')
          )
    """
    cursor.execute(sql)

def normalize_datasource_key(value):
    if not value: return None
    return value.strip().upper().replace(" ", "")

def process_report(cursor, source_row, shared_datasource_lookup, type5_datasource_id_map, rpt_datasource_columns, source_database):
    report_name = source_row.Name
    report_type = str(source_row.Type) if source_row.Type is not None else None
    report_path = source_row.Path
    content = source_row.Content

    print(f"\nProcessing report: {report_path}")

    xml_text = decode_rdl_content(content)
    if not xml_text:
        raise ValueError("Could not decode Content field into RDL XML.")

    datasources, queries = parse_rdl(xml_text)
    datasources = apply_shared_datasource_values(source_row.ItemID, datasources, shared_datasource_lookup)

    # Embedded report datasources have no Type 5 Catalog item.
    # Apply the owning Type 2 report description, including NULL when absent.
    report_desc_conn, report_desc_user = extract_description_credentials(
        source_row.Description
    )
    for ds in datasources:
        if not ds.get("is_shared_datasource"):
            ds["connection_string"] = report_desc_conn or ds.get("connection_string")
            ds["connection_username"] = (
                report_desc_user
                or extract_description_credentials(ds.get("connection_string"))[1]
            )


    report_row = {
        "report_name": report_name,
        "report_type": report_type,
        "report_path": report_path
    }

    rpt_id = insert_rpt_catalog(cursor, report_row, source_database)
    print(f"  Inserted RPT_Catalog row. RPT_ID = {rpt_id}")

    datasource_id_map = {}
    datasource_info_map = {}
    datasource_count = 0
    map_count = 0
    query_count = 0
    object_count = 0

    for ds in datasources:
        datasource_name = ds.get("datasource_name")
        shared_item_id = ds.get("shared_datasource_item_id")
        shared_item_key = (
            str(shared_item_id).upper()
            if shared_item_id
            else None
        )

        # Shared datasource:
        # reuse the DataSource_ID created from this source's Type 5 Catalog row.
        if shared_item_key and shared_item_key in type5_datasource_id_map:
            datasource_id = type5_datasource_id_map[shared_item_key]
            print(
                f"  Reusing Type 5 datasource by ItemID: "
                f"{datasource_name} -> DataSource_ID = {datasource_id}"
            )
        else:
            # Resolve embedded or unmatched shared datasource from the lookup table.
            datasource_id = lookup_existing_datasource(
                cursor,
                ds,
                rpt_datasource_columns,
                source_database
            )

        for name in [
            ds.get("datasource_name"),
            ds.get("report_datasource_name"),
            ds.get("shared_datasource_name")
        ]:

            if name:
                key = normalize_datasource_key(name)
                datasource_id_map[key] = datasource_id
                datasource_info_map[key] = ds

        datasource_count += 1

        ensure_datasource_map(cursor, rpt_id, datasource_id)
        map_count += 1
        print(f"  Mapped datasource: {datasource_name} -> DataSource_ID = {datasource_id}")

    for q in queries:
        datasource_name = q.get("datasource_name")
        if not datasource_name and len(datasource_id_map) ==1:
            datasource_id = list(datasource_id_map.values())[0]
            datasource_info = list(datasource_info_map.values())[0]
        elif not datasource_name:
            print(f" Skipping dataset with no DataSourceName: {q.get('dataset_name')}. "
                  f"Avaiable datasource keys = {list(datasource_id_map.keys())}"
            )
            continue
        else:
            datasource_key = normalize_datasource_key(datasource_name)        
            datasource_id = datasource_id_map.get(datasource_key)
            datasource_info = datasource_info_map.get(datasource_key, {})

        if datasource_id is None and len(datasource_id_map) == 1:
            datasource_id = list(datasource_id_map.values())[0]
            datasource_info = list(datasource_info_map.values())[0]

        if datasource_id is None:
            raise ValueError(f"Datasource could not be resolved for dataset: {q.get('dataset_name')}. "
                             f"Dataset datasource name = {datasource_name}. "
                             f"Avaiable datasource keys = {list(datasource_id_map.keys())}"
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

def build_type5_datasources_from_catalog(type5_rows):
    """Convert every dbo.Catalog.Type = 5 row into a datasource metadata dictionary."""
    datasources = []

    for row in type5_rows:
        parsed = parse_shared_datasource_content(row.Content)
        description_conn, description_user = extract_catalog_datasource_credentials(row.Description, parsed)

        datasources.append({
            "datasource_name": row.Name.upper() if row.Name else None,
            "report_datasource_name": None,
            "shared_datasource_path": row.Path,
            "shared_datasource_item_id": str(row.ItemID).upper() if row.ItemID else None,
            "connection_string": description_conn,
            "connection_username": description_user,
            "database_name": (
                parsed.get("database_name").upper()
                if parsed.get("database_name") else None
            ),
            "datasource_type": parsed.get("datasource_type"),
            "datasource_reference": row.Path,
            "is_shared_datasource": 1
        })

    return datasources


def process_type5_catalog_row(cursor, source_row, rpt_datasource_columns, source_database):
    """Type 5: load Catalog and Map; use preloaded RPT_DataSources as lookup."""
    report_row = {
        "report_name": source_row.Name,
        "report_type": str(source_row.Type) if source_row.Type is not None else None,
        "report_path": source_row.Path
    }
    rpt_id = insert_rpt_catalog(cursor, report_row, source_database)

    ds_row = {
        "datasource_name": source_row.Name,
        "shared_datasource_path": source_row.Path,
        "is_shared_datasource": 1
    }
    datasource_id = lookup_existing_datasource(
        cursor, ds_row, rpt_datasource_columns, source_database
    )
    ensure_datasource_map(cursor, rpt_id, datasource_id)
    print(
        f"  Type 5 lookup: {source_row.Path} "
        f"-> RPT_ID={rpt_id}, existing DataSource_ID={datasource_id}"
    )
    return {
        "catalog": 1,
        "datasources": 0,
        "maps": 1,
        "queries": 0,
        "objects": 0,
        "shared_datasource_item_id": (
            str(source_row.ItemID).upper() if source_row.ItemID else None
        ),
        "datasource_id": datasource_id
    }


def normalize_environment_values(cursor):
    """PROD-only normalization of RPT_Objects; datasource lookup is read-only."""
    # ---------------------------------------------------------
    # Update RPT_Objects
    # ---------------------------------------------------------
    update_objects_sql = """
        UPDATE RPT_Objects
        SET
            Database_Name =
                CASE
                    WHEN Database_Name LIKE 'TSTIJ%'
                      OR Database_Name LIKE 'STGIJ%'
                    THEN STUFF(Database_Name, 1, 5, 'PRDIJ')
                    ELSE Database_Name
                END

        WHERE
               Database_Name LIKE 'TSTIJ%'
            OR Database_Name LIKE 'STGIJ%';
    """

    cursor.execute(update_objects_sql)

    print(
        "RPT_Objects Database_Name updated. "
        f"Rows affected: {cursor.rowcount}"
    )

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
        TARGET_RPT_DATASOURCE_MAP,
        TARGET_RPT_QUERIES,
        TARGET_RPT_OBJECTS,
        TARGET_RPT_SERVERS
    ])

    try:
        print("ETL started")
        print(f"Start time: {datetime.now()}")

        # 1. Select environment and connect to target
        environment_name = get_environment_input()
        apply_environment_config(environment_name)

        print(f"Connecting to target: {TARGET_SERVER} / {TARGET_DATABASE}")
        target_conn = get_connection(TARGET_SERVER, TARGET_DATABASE)
        print("Connected to target.")

        if CLEAR_TARGET_TABLES_BEFORE_LOAD:
            clear_target_tables(target_conn)

        cursor = target_conn.cursor()
        rpt_datasource_columns = get_table_columns(
            cursor,
            TARGET_RPT_DATASOURCES
        )

        # 2. Process each configured ReportServer source
        for source in SOURCES:
            source_conn = None
            source_server = source["SOURCE_SERVER"]
            source_database = source["SOURCE_DATABASE"]

            try:
                print("\n" + "=" * 80)
                print(f"Connecting to source: {source_server} / {source_database}")
                print("=" * 80)

                source_conn = get_connection(source_server, source_database)
                print("Connected to source.")

                # fetch_catalog_rows() returns Catalog Type 2 and Type 5.
                source_rows = fetch_catalog_rows(source_conn)

                if not source_rows:
                    print("No source rows found. Nothing to load.")
                    continue

                report_rows = [
                    row for row in source_rows
                    if row.Type == 2
                ]

                type5_rows = [
                    row for row in source_rows
                    if row.Type == 5
                ]

                print(f"Report rows (Type 2): {len(report_rows)}")
                print(f"Shared datasource rows (Type 5): {len(type5_rows)}")

                # Resolve report datasource references to shared Type 5 datasources.
                shared_datasource_lookup = fetch_shared_datasource_lookup(
                    source_conn
                )

                # 2A. Process Catalog Type 5 shared datasources.
                # The map is recreated for each ReportServer source so ItemIDs
                # and DataSource_IDs never leak between DEV/TST/PROD sources.
                type5_datasource_id_map = {}

                # Type 5 -> RPT_Catalog + RPT_DataSource_Map (datasource lookup only)
                for row in type5_rows:
                    try:
                        counts = process_type5_catalog_row(
                            cursor,
                            row,
                            rpt_datasource_columns,
                            source_database
                        )

                        target_conn.commit()

                        total_catalog += counts["catalog"]
                        total_datasources += counts["datasources"]
                        total_maps += counts["maps"]

                        shared_item_id = counts.get(
                            "shared_datasource_item_id"
                        )
                        if shared_item_id:
                            type5_datasource_id_map[shared_item_id] = (
                                counts["datasource_id"]
                            )

                    except Exception as ex:
                        target_conn.rollback()

                        print(
                            "  Type 5 datasource failed and rolled back: "
                            f"{getattr(row, 'Path', None)}"
                        )
                        print_error(ex)

                        log_error_to_table(
                            target_conn=target_conn,
                            process_name=PROCESS_NAME,
                            source_table=SOURCE_TABLE,
                            target_table=(
                                f"{TARGET_RPT_CATALOG}, "
                                f"{TARGET_RPT_DATASOURCE_MAP}"
                            ),
                            report_path=getattr(row, "Path", None),
                            report_name=getattr(row, "Name", None),
                            error_type=type(ex).__name__,
                            error_message=str(ex),
                            error_details=traceback.format_exc()
                        )

                # 2B. Process Catalog Type 2 reports.
                for row in report_rows:
                    try:
                        counts = process_report(
                            cursor,
                            row,
                            shared_datasource_lookup,
                            type5_datasource_id_map,
                            rpt_datasource_columns,
                            source_database
                        )

                        target_conn.commit()

                        total_catalog += counts["catalog"]
                        total_datasources += counts["datasources"]
                        total_maps += counts["maps"]
                        total_queries += counts["queries"]
                        total_objects += counts["objects"]

                    except Exception as ex:
                        target_conn.rollback()

                        print(
                            "  Report failed and rolled back: "
                            f"{getattr(row, 'Path', None)}"
                        )
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

            except Exception as ex:
                if target_conn:
                    target_conn.rollback()

                print(
                    f"\nSource processing failed: "
                    f"{source_server} / {source_database}"
                )
                print_error(ex)

                if target_conn:
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

            finally:
                if source_conn:
                    source_conn.close()
                    print(
                        "Source connection closed: "
                        f"{source_server} / {source_database}"
                    )

        # 3. Populate RPT_Servers after all report metadata is loaded.
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

        # 4. Normalize TST/STG values only for PROD.
        if environment_name.upper() == "PROD":
            try:
                print("\nNormalizing TST/STG values to PRD...")

                normalize_environment_values(cursor)

                target_conn.commit()
                print("Environment normalization is completed.")

            except Exception as ex:
                target_conn.rollback()
                print("Environment normalization failed.")
                print_error(ex)
                raise
        else:
            print(
                f"\nEnvironment is {environment_name}. "
                "TST/STG to PRD normalization skipped."
            )

        # 5. Final execution summary.
        print("\nLOAD SUMMARY")
        print("-" * 60)
        print(f"RPT_Catalog rows inserted       : {total_catalog}")
        print("RPT_DataSources                 : Lookup only (0 rows changed)")
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
        if target_conn:
            target_conn.close()
            print("Target connection closed.")


if __name__ == "__main__":
    main()
