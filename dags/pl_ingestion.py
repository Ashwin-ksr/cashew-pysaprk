import logging
from datetime import datetime
from airflow.sdk import dag, task, get_current_context

logger = logging.getLogger(__name__)


@dag(
    dag_id="ingestion",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    params={"full_refresh": False, "table_names": []},
    default_args={"retries": 0},
    tags=["ingest", "bronze"],
)
def ingestion():

    @task
    def get_new_file() -> str | None:
        from cashew import ingest
        return ingest.get_new_db_file()


    @task.short_circuit
    def has_file(db_file: str | None) -> bool:
        if db_file is None:
            logger.info("Nothing new to load")
        return db_file is not None

    @task
    def resolve_tables(db_file: str) -> list[str]:
        from cashew import ingest

        params = get_current_context().get("params", {})
        tables = ingest.resolve_sqlite_tables(
            db_file,
            force_full_refresh=bool(params.get("full_refresh", False)),
            table_names=params.get("table_names", []),
        )
        logger.info("Tables to load from %s: %s", db_file, tables)
        return tables

    @task(max_active_tis_per_dag=1)
    def load_table(db_file: str, table: str) -> int:
        from cashew import ingest
        from cashew.spark import create_spark_session

        params = get_current_context().get("params", {})
        spark = create_spark_session()
        try:
            return ingest.load_sqlite_table(
                spark,
                db_file,
                table,
                force_full_refresh=bool(params.get("full_refresh", False)),
            )
        finally:
            spark.stop()

    # wiring
    db_file = get_new_file()
    gate = has_file(db_file)

    tables = resolve_tables(db_file)
    gate >> tables

    load_table.partial(db_file=db_file).expand(table=tables)

ingestion()