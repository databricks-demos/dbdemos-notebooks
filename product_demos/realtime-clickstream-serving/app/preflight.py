"""Startup pre-flight: prove the app service principal can connect to Lakebase
and read live.sessions, BEFORE Streamlit launches. Runs in the real app runtime
as the app SP. Logs a single [PREFLIGHT] line to stdout (visible in app logs).
Never fails the boot - Streamlit starts regardless so the UI can show its own
graceful 'connection not ready' state if something is off.
"""
import sys
import uuid


def main() -> None:
    try:
        import psycopg
        from psycopg.rows import dict_row
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        inst = w.database.get_database_instance(name="clickstream-sessions")
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=["clickstream-sessions"]
        )
        import os

        user = os.getenv("PGUSER") or w.current_user.me().user_name
        conn = psycopg.connect(
            f"host={inst.read_write_dns} dbname=databricks_postgres "
            f"user={user} password={cred.token} sslmode=require",
            row_factory=dict_row,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n, "
                "COUNT(*) FILTER (WHERE status='online') AS online, "
                "EXTRACT(EPOCH FROM (now() - MAX(last_updated))) AS fresh "
                "FROM live.sessions"
            )
            row = cur.fetchone()
        conn.close()
        print(
            f"[PREFLIGHT] Lakebase OK as role={user} "
            f"total_rows={row['n']} online={row['online']} "
            f"freshest_secs={float(row['fresh']):.2f}",
            flush=True,
        )
    except Exception as e:  # noqa: BLE001
        print(f"[PREFLIGHT] Lakebase FAILED: {type(e).__name__}: {e}", flush=True)


if __name__ == "__main__":
    main()
    sys.exit(0)
