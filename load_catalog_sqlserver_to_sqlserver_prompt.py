import pyodbc
import traceback
from getpass import getpass

def get_connection_inputs(role):
    print(f"\nEnter {role} connection details")
    server = input(f"{role} server name: ").strip()
    database = input(f"{role} database name: ").strip()
    auth_type = input(f"{role} authentication type (trusted/sql): ").strip().lower()

    while auth_type not in ("trusted", "sql"):
        print("Invalid authentication type. Enter 'trusted' or 'sql'.")
        auth_type = input(f"{role} authentication type (trusted/sql): ").strip().lower()

    username = ""
    password = ""

    if auth_type == "sql":
        username = input(f"{role} SQL username: ").strip()
        password = getpass(f"{role} SQL password: ")

    return server, database, auth_type, username, password


def build_conn_str(server, database, auth_type, username="", password=""):
    if auth_type == "sql":
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )
    else:
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )


def main():
    source_con = None
    target_con = None

    try:
        source_server, source_db, source_auth, source_user, source_pwd = get_connection_inputs("Source")
        target_server, target_db, target_auth, target_user, target_pwd = get_connection_inputs("Target")

        source_conn_str = build_conn_str(source_server, source_db, source_auth, source_user, source_pwd)
        target_conn_str = build_conn_str(target_server, target_db, target_auth, target_user, target_pwd)

        print("\nConnecting to source...")
        source_con = pyodbc.connect(source_conn_str)
        source_cur = source_con.cursor()
        source_cur.execute("SELECT @@SERVERNAME, DB_NAME()")
        print("Connected to source:", source_cur.fetchone())

        print("Connecting to target...")
        target_con = pyodbc.connect(target_conn_str)
        target_cur = target_con.cursor()
        target_cur.execute("SELECT @@SERVERNAME, DB_NAME()")
        print("Connected to target:", target_cur.fetchone())

        print("\nFetching source rows...")
        source_cur.execute("""
            SELECT
                ItemID, Path, Name, ParentID, Type, Content, Intermediate,
                SnapshotDataID, LinkSourceID, Property, Description, Hidden,
                CreatedByID, CreationDate, ModifiedByID, ModifiedDate,
                MimeType, SnapshotLimit, Parameter, PolicyID, PolicyRoot,
                ExecutionFlag, ExecutionTime, SubType, ComponentID, ContentSize
            FROM dbo.Catalog
        """)
        rows = source_cur.fetchall()
        print(f"Rows fetched from source: {len(rows)}")

        insert_sql = """
            INSERT INTO dbo.Catalog (
                ItemID, Path, Name, ParentID, Type, Content, Intermediate,
                SnapshotDataID, LinkSourceID, Property, Description, Hidden,
                CreatedByID, CreationDate, ModifiedByID, ModifiedDate,
                MimeType, SnapshotLimit, Parameter, PolicyID, PolicyRoot,
                ExecutionFlag, ExecutionTime, SubType, ComponentID, ContentSize
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """

        print("Starting insert...")
        inserted = 0
        for i, row in enumerate(rows, start=1):
            try:
                target_cur.execute(insert_sql, row)
                inserted += 1

                if i % 100 == 0:
                    target_con.commit()
                    print(f"{i} rows processed, {inserted} inserted")
            except Exception as row_err:
                print(f"\nError inserting row {i}")
                try:
                    print(f"ItemID={row.ItemID}, Path={row.Path}, LinkSourceID={row.LinkSourceID}")
                except Exception:
                    pass
                print(str(row_err))
                traceback.print_exc()
                raise

        target_con.commit()
        print(f"\nData load completed successfully. Total inserted: {inserted}")

    except Exception as e:
        print("\nERROR OCCURRED")
        print(str(e))
        traceback.print_exc()
        if target_con:
            target_con.rollback()

    finally:
        try:
            if source_con:
                source_con.close()
        except Exception:
            pass
        try:
            if target_con:
                target_con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
