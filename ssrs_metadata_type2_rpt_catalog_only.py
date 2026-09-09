import pyodbc
import traceback
from datetime import datetime

# ============================================================
# SQL SERVER ENVIRONMENT CONFIGURATION
# ============================================================
ENVIRONMENT_CONFIG = {
    "DEV": {
        "SOURCES": [
            {
                "SOURCE_SERVER": "DEVSQL\\STSEA01",
                "SOURCE_DATABASE": "ReportServerDev",
            }
        ],
        "TARGET_SERVER": "DEV436LQC\\SQL2019",
        "TARGET_DATABASE": "AdventureWorks2019",
    },
    "TEST": {
        "SOURCES": [
            {
                "SOURCE_SERVER": "TESTSQL\\SBEA02",
                "SOURCE_DATABASE": "ReportServerTST",
            }
        ],
        "TARGET_SERVER": "TST126LQC\\SQL2019",
        "TARGET_DATABASE": "ReportMetadataRepository",
    },
    "PROD": {
        "SOURCES": [
            {
                "SOURCE_SERVER": "PRDSQL\\SFALC02",
                "SOURCE_DATABASE": "ReportServerProd",
            }
        ],
        "TARGET_SERVER": "PROD43YHS\\SQL2019",
        "TARGET_DATABASE": "ReportMetadataRepositoryPROD",
    },
}

# ============================================================
# SOURCE / TARGET
# ============================================================
SOURCE_TABLE = "dbo.Catalog"
TARGET_RPT_CATALOG = "dbo.RPT_Catalog"
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"
PROCESS_NAME = "SSRS_Type2_RPT_Catalog_Only_Load"


# ============================================================
# ENVIRONMENT INPUT
# ============================================================
def get_environment_input():
    allowed_envs = ", ".join(ENVIRONMENT_CONFIG.keys())

    while True:
        env_value = input(f"Enter environment ({allowed_envs}): ").strip().upper()

        if env_value in ENVIRONMENT_CONFIG:
            return env_value

        print(f"Invalid environment: {env_value}")
        print(f"Please enter one of: {allowed_envs}")


def apply_environment_config(environment_name):
    global SOURCES, TARGET_SERVER, TARGET_DATABASE

    config = ENVIRONMENT_CONFIG[environment_name]
    SOURCES = config["SOURCES"]
    TARGET_SERVER = config["TARGET_SERVER"]
    TARGET_DATABASE = config["TARGET_DATABASE"]

    print("\nSelected environment configuration")
    print("-" * 60)
    print(f" Environment      : {environment_name}")
    print(f" Target Server    : {TARGET_SERVER}")
    print(f" Target Database  : {TARGET_DATABASE}")

    for source in SOURCES:
        print(
            f" Source           : {source['SOURCE_SERVER']} / "
            f"{source['SOURCE_DATABASE']}"
        )
    print("-" * 60)


# ============================================================
# SQL CONNECTION
# ============================================================
def get_connection(server_name, database_name):
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# ============================================================
# ERROR DISPLAY
# ============================================================
def print_error(ex):
    print("\nERROR DETAILS")
    print("-" * 80)
    print(f"Type : {type(ex).__name__}")
    print(f"Text : {str(ex)}")
    if hasattr(ex, "args"):
        print("Args :")
        for i, arg in enumerate(ex.args, start=1):
            print(f"  [{i}] {arg}")
    print("-" * 80)


# ============================================================
# BUSINESS SUITE
# Example:
#   /Datasource/subfolder/reportname -> DATASOURCE
#   /EFS/reportname                  -> EFS
# ============================================================
def get_business_suite(rpt_path):
    if not rpt_path:
        return None

    parts = rpt_path.strip("/").split("/")
    if parts and parts[0]:
        return parts[0].upper()

    return None


# ============================================================
# SOURCE EXTRACTION
# ONLY dbo.Catalog.Type = 2 REPORTS
# No RDL XML parsing is required because this script loads only
# report-level Catalog metadata into dbo.RPT_Catalog.
# ============================================================
def fetch_report_catalog_rows(source_conn):
    sql = f"""
        SELECT
            ItemID,
            Name,
            Type,
            Path
        FROM {SOURCE_TABLE}
        WHERE Type = 2
        ORDER BY Path;
    """

    cursor = source_conn.cursor()
    print("Reading report rows from dbo.Catalog where Type = 2...")
    cursor.execute(sql)
    rows = cursor.fetchall()
    print(f"Type 2 report rows fetched: {len(rows)}")
    return rows


# ============================================================
# TARGET INSERT
# ONLY dbo.RPT_Catalog
# ============================================================
def insert_rpt_catalog(cursor, source_row, source_database):
    report_name = source_row.Name
    report_type = str(source_row.Type) if source_row.Type is not None else None
    report_path = source_row.Path
    business_suite = get_business_suite(report_path)

    sql = f"""
        INSERT INTO {TARGET_RPT_CATALOG}
        (
            RPT_Name,
            RPT_Type,
            RPT_Business_Suite,
            RPT_Path,
            SSRS_Access_Method,
            Created_By,
            Created_Date,
            Updated_By,
            Updated_Date
        )
        OUTPUT INSERTED.RPT_ID
        VALUES
        (
            ?, ?, ?, ?, ?,
            USER_NAME(), GETDATE(),
            USER_NAME(), GETDATE()
        );
    """

    cursor.execute(
        sql,
        report_name,
        report_type,
        business_suite,
        report_path,
        source_database,
    )

    return cursor.fetchone()[0]


# ============================================================
# MAIN
# ============================================================
def main():
    target_conn = None
    total_catalog = 0

    try:
        print("TYPE 2 -> RPT_CATALOG ONLY LOAD started")
        print(f"Start time: {datetime.now()}")

        environment_name = get_environment_input()
        apply_environment_config(environment_name)

        print(f"\nConnecting to target: {TARGET_SERVER} / {TARGET_DATABASE}")
        target_conn = get_connection(TARGET_SERVER, TARGET_DATABASE)
        print("Connected to target.")

        target_cursor = target_conn.cursor()

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

                report_rows = fetch_report_catalog_rows(source_conn)

                if not report_rows:
                    print("No dbo.Catalog Type = 2 reports found.")
                    continue

                for row in report_rows:
                    try:
                        rpt_id = insert_rpt_catalog(
                            target_cursor,
                            row,
                            source_database,
                        )
                        target_conn.commit()
                        total_catalog += 1

                        print(
                            f"Inserted RPT_Catalog: "
                            f"RPT_ID={rpt_id}, Path={row.Path}"
                        )

                    except Exception as ex:
                        target_conn.rollback()
                        print(f"Failed report: {getattr(row, 'Path', None)}")
                        print_error(ex)
                        print(traceback.format_exc())

            finally:
                if source_conn:
                    source_conn.close()
                    print(
                        f"Source connection closed: "
                        f"{source_server} / {source_database}"
                    )

        print("\nTYPE 2 -> RPT_CATALOG LOAD SUMMARY")
        print("-" * 60)
        print(f"RPT_Catalog rows inserted : {total_catalog}")
        print("-" * 60)
        print(f"End time: {datetime.now()}")
        print("RPT_Catalog-only load completed.")

    except Exception as ex:
        print("\nFatal error occurred.")
        print_error(ex)
        print(traceback.format_exc())

    finally:
        if target_conn:
            target_conn.close()
            print("Target connection closed.")


if __name__ == "__main__":
    main()
