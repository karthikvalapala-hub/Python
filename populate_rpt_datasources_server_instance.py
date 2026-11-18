"""One-time backfill of RPT_DataSources.Server_Instance from Connection_String_Text.

Requires: pip install pyodbc
Python 3.8+

Set SERVER and DATABASE below. By default DRY_RUN=True (preview only).
Set DRY_RUN=False to perform updates. No source ReportServer join is needed.

This reproduces the basic SQL Server extraction rule used in the metadata ETL:
Data Source / Server / Address / Addr / Network Address = value until semicolon.
It does not resolve Oracle TNS aliases or DNS hostnames, and it does not
apply environment-specific prefix substitutions.
"""
import re
import pyodbc

SERVER = r"YOUR_SQL_SERVER"
DATABASE = "YOUR_METADATA_DATABASE"
ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
DRY_RUN = True
# True: only fill blank Server_Instance; False: overwrite existing values too.
ONLY_MISSING = True


def extract_server_instance(connection_string):
    if connection_string is None:
        return None
    match = re.search(
        r"(?:Data Source|Server|Address|Addr|Network Address)\s*=\s*([^;]+)",
        str(connection_string),
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else None


def main():
    conn_string = (
        "DRIVER={" + ODBC_DRIVER + "};"
        "SERVER=" + SERVER + ";"
        "DATABASE=" + DATABASE + ";"
        "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
    )
    connection = pyodbc.connect(conn_string, autocommit=False)
    try:
        cursor = connection.cursor()
        # Snapshot records, avoiding an UPDATE cursor on the same resultset.
        cursor.execute("""
            SELECT DataSource_ID, DataSource_Name, Connection_String_Text,
                   Server_Instance, DataSource_Type
            FROM dbo.RPT_DataSources
            ORDER BY DataSource_ID
        """)
        rows = cursor.fetchall()
        updates = []
        print("Rows examined: {}".format(len(rows)))
        for row in rows:
            ds_id, ds_name, conn_text, current_server, ds_type = row
            extracted = extract_server_instance(conn_text)
            if extracted is None:
                continue
            if ONLY_MISSING and current_server is not None and str(current_server).strip():
                continue
            if current_server is not None and str(current_server).strip() == extracted:
                continue
            print("ID={} | Name={} | Type={} | Current={!r} | Extracted={!r}".format(
                ds_id, ds_name, ds_type, current_server, extracted))
            updates.append((extracted, ds_id))

        print("Rows eligible for update: {}".format(len(updates)))
        if DRY_RUN:
            print("DRY RUN: no changes made. Set DRY_RUN=False to apply.")
            connection.rollback()
            return
        try:
            for extracted, ds_id in updates:
                cursor.execute("""
                    UPDATE dbo.RPT_DataSources
                    SET Server_Instance = ?,
                        Updated_By = USER_NAME(),
                        Updated_Date = GETDATE()
                    WHERE DataSource_ID = ?
                """, extracted, ds_id)
            connection.commit()
            print("Committed {} updates.".format(len(updates)))
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
