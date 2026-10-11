from collections.abc import Sequence
from datetime import datetime, timezone
import logging
import pandas as pd
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, functions as F
import re
import sqlite3
from cashew.config import METADATA_XL, WAREHOUSE_DIR
from cashew.models import SQLiteLoadResult
from cashew.utils import load_table_to_lakehouse

__all__ = ["get_new_db_files", "get_sqlite_tables_list", "load_sqlite_to_raw"]

logger = logging.getLogger(__name__)

def get_new_db_files(spark: SparkSession, fetch_latest: bool = True) -> list[str]:
    """
    Return new SQLite files from the landing zone.

    Args:
        spark: Active Spark session.
        fetch_latest: Whether to return only the latest new file.

    Returns:
        List of new SQLite file names, or the latest new file when requested.
    """
    
    # get files from landing zone
    landing_zone = WAREHOUSE_DIR / "landing"

    # early exit if no file is present
    if not any(landing_zone.iterdir()):
        logger.info("No files found in landing zone")
        return []

    landing_zone_files: dict[str, datetime] = {}

    FILENAME_PATTERN = re.compile(
        r"^cashew-db-v\d+-.*?(?P<timestamp>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-\d{3})Z\.sqlite$"
    )
    for file in landing_zone.iterdir():
        if file.is_file() and file.name.endswith(".sqlite"):
            match = FILENAME_PATTERN.match(file.name)
            if match:
                # raw time stamp
                raw_ts = match.group("timestamp")

                # parsed time stamp
                parsed_ts = datetime.strptime(raw_ts, "%Y-%m-%d-%H-%M-%S-%f").replace(
                    tzinfo=timezone.utc
                )

                landing_zone_files[file.name] = parsed_ts
    
    loaded_files: set[str] = {row.file_name 
        for row in (
            spark.table('raw.loaded_files')
            .filter(F.col("is_active"))
            .select(F.col("file_name"))
            .collect()
        )
    }

    if fetch_latest:
        latest_file: str = max(landing_zone_files, key=landing_zone_files.__getitem__)
        
        if latest_file not in loaded_files:
            logger.info("Latest file is new: %s", latest_file)
            return [latest_file]
        
        logger.info("Latest file has already been loaded: %s", latest_file)
        return [] 
            
    else:
        new_files = list(landing_zone_files.keys() - loaded_files)
        logger.info(
            "Found a total of %d file(s) of which %d file(s) are new",
            len(landing_zone_files.keys()),
            len(new_files)
        )
        return new_files


def get_sqlite_tables_list(db_file_name: str) -> list[str]:
    """Return a list of table names in the given SQLite database file.

    Args:
        db_file_name: Name of the SQLite database file located in the
            landing data directory.

    Returns:
        A list containing the names of all tables in the SQLite database.

    Raises:
        sqlite3.Error: If an error occurs while connecting to or querying
            the SQLite database.
    """

    db_file_path = WAREHOUSE_DIR / "landing" / db_file_name

    conn = sqlite3.connect(database=db_file_path)
    try:
        tables_df = pd.read_sql_query(
            sql = """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
            """,
            con=conn
        )
    finally:
        conn.close()

    
    logger.info(
        "Found a total of %d table(s) in db %s",
        len(tables_df),
        db_file_name,
    )
    tables: list[str] = tables_df["name"].to_list()

    logger.debug(
        "List of table found in the file:\n%s",
        ", \n".join(tables)
    )
    
    return tables


def read_sqlite_table_to_df(
    spark: SparkSession,
    db_file_path: Path,
    table_name: str,
    metadata: pd.Series,
    force_full_refresh: bool = False
) -> DataFrame:
    """Read a SQLite table into a Spark DataFrame using JDBC.

    The query is built from the table's metadata row. If ``required_cols`` is
    set, only those columns are selected. For tables with an ``incremental``
    refresh type, rows are filtered to those whose watermark column is on or
    after the latest value already present in the ``raw`` Delta table. The
    filter is skipped when ``force_full_refresh`` is set or the target table
    does not exist yet. A missing ``refresh_type`` falls back to ``full`` with
    a warning.

    Args:
        spark: Active Spark session.
        db_file_path: Path to the SQLite database file.
        table_name: Name of the SQLite table to read.
        metadata: Metadata row for the table (for example, from the
            ``raw_config`` sheet). May be empty, in which case all columns and
            all rows are read.
        force_full_refresh: If True, ignore the incremental watermark and read
            the entire table.

    Returns:
        A Spark DataFrame containing the selected columns and rows of the
        SQLite table.

    Raises:
        FileNotFoundError: If the `db_file_path` does not exist.
        ValueError: If the refresh type is ``incremental`` but no
            ``last_modified_column`` is configured.
        pyspark.errors.AnalysisException: If the target Delta table cannot be
            read when computing the incremental watermark.
        Exception: If the JDBC read fails when the DataFrame is evaluated
            (for example, an invalid column or table name).
    """
    def _normalize_string_columns(df: DataFrame) -> DataFrame:
        """Drop char/varchar length metadata so Delta doesn't enforce a bogus limit."""
        for field in df.schema.fields:
            if "__CHAR_VARCHAR_TYPE_STRING" in field.metadata:
                df = df.withColumn(field.name, F.col(f"`{field.name}`").cast("string"))
        return df

    if not db_file_path.exists():
        raise FileNotFoundError(f"SQLite file not found: {db_file_path}")

    selected_cols = "*"
    where_sql = "WHERE 1 = 1"

    def _clean(value) -> str:
        """Return a stripped string, treating None/NaN as empty."""
        return "" if pd.isna(value) else str(value).strip()

    if not metadata.empty:
        source = metadata.get("source", "unknown_source")
        
        # build column select based on metadata
        required_cols = _clean(metadata.get("required_cols"))
        if required_cols:
            # overwrite selected cols to contain only required cols
            selected_cols = ", ".join(
                f'"{c.strip()}"' for c in required_cols.split(",") if c.strip()
            )

        refresh_type = _clean(metadata.get("refresh_type"))
        last_modified_column = _clean(metadata.get("last_modified_column"))

        # Missing or empty refresh_type; raise WARNING and fallback to full refresh
        if not refresh_type:
            logger.warning(
                "[%s.%s] Missing 'refresh_type'; defaulting to 'full' refresh.",
                source, table_name,
            )
            refresh_type = "full"

        # Incremental configured without a watermark; raise ERROR and FAIL copy table
        if refresh_type == "incremental" and not last_modified_column:
            raise ValueError(
                f"[{source}.{table_name}] 'incremental' requires 'last_modified_column'"
            )

        # build the incremental refresh condition
        if (
            not force_full_refresh
            and refresh_type == "incremental"
            and spark.catalog.tableExists(f"`raw`.`{table_name}`")
        ):
            # get the max value of watermark column already in the delta table
            # and parse it to sqlite time standard
            latest_updated_unix_epoch: str | None = (
                max_val
                if (max_val := spark.table(f"`raw`.`{table_name}`")
                    .agg(F.max(F.col(last_modified_column)).alias("max_val"))
                    .collect()[0]["max_val"])
                else None
            )
            if latest_updated_unix_epoch is not None:
                where_sql += (
                    f""" AND "{last_modified_column}" >= {latest_updated_unix_epoch}"""
                )


    sqlite_query = f"""
        SELECT
            {selected_cols}
        FROM
            {table_name}
        {where_sql}
            
    """

    logger.debug(
        "SQLITE LOAD QUERY:\n%s",
        sqlite_query
    )

    df = _normalize_string_columns(
        spark.read.format("jdbc")
        .option("driver", "org.sqlite.JDBC")
        .option("url", f"jdbc:sqlite:{db_file_path}")
        .option("query", sqlite_query)
        .load()
    )

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "[%s] DataFrame shape: (%d rows, %d columns)",
            table_name,
            df.count(),
            len(df.columns),
        )

    return df

def load_sqlite_to_raw(
    spark: SparkSession, 
    db_file_name: str, 
    force_full_refresh: bool = False,
    table_names: str | Sequence[str] = ''
) -> SQLiteLoadResult:
    """Load tables from a SQLite database file into the ``raw`` Delta schema.

    Tables to load are resolved from the optional ``table_names`` input, the
    active SQLite entries in the metadata workbook, and the tables present in
    the SQLite file. Tables that are requested or configured but missing from
    SQLite (or, unless ``force_full_refresh`` is set, from the metadata) are
    skipped with a warning. Each table is written in ``overwrite`` mode for
    full refreshes or ``append`` mode for incremental refreshes. A failure in
    one table is recorded and does not stop the remaining tables.

    Args:
        spark: Active Spark session.
        db_file_name: Name of the SQLite database file located in the
            landing data directory.
        force_full_refresh: If True, overwrite every table with a full read
            and allow tables that are not listed in the metadata.
        table_names: Optional table name or sequence of table names to load.
            If empty, all active SQLite tables in the metadata are loaded.

    Returns:
        A SQLiteLoadResult. ``success`` is True only if every selected table
        was loaded. Otherwise it is False and ``failed_tables`` maps each
        failed table to its error message. Setup failures (such as a missing
        SQLite file or unreadable metadata) also return ``success=False``
        instead of raising.
    """

    # result set
    rows_processed = 0
    tables_processed = 0
    failed_tables: dict[str, str] = {}

    try:
        db_file_path = WAREHOUSE_DIR / "landing" / db_file_name
        metadata_file_path = WAREHOUSE_DIR / "metadata" / METADATA_XL

        if not db_file_path.exists():
            raise FileNotFoundError(f"SQLite file not found: {db_file_path}")
        
        # normalize table name input to set
        if isinstance(table_names, str):
            table_list: set[str] = set(table_names) if table_names.strip() else set()
        else:
            table_list: set[str] = set(table_names)

        # load metadata config of sqlite tables
        raw_config = pd.read_excel(metadata_file_path, sheet_name="raw_config")
        metadata_tables: set[str] = set(
            raw_config[
                (raw_config["source"] == 'sqlite') & 
                (raw_config["is_active"] == 1)
            ]
            ["table_name"].to_list()
        )   

        # tables from the sqlite file
        sqlite_tables: set[str] = set(get_sqlite_tables_list(db_file_name))

        # any table appended to this set will be copied to raw
        tables_to_load: set[str] = set()

        if table_list: # if user has a given a list of table(s) as input
            missing_in_sqlite = table_list - sqlite_tables
            if missing_in_sqlite:
                logger.warning(
                    "Skipping %d table(s) listed in input but not found in SQLite: %s",
                    len(missing_in_sqlite),
                    ", ".join(sorted(missing_in_sqlite)),
                )
            missing_in_metadata = table_list - metadata_tables
            if missing_in_metadata and not force_full_refresh:
                logger.warning(
                    "Skipping %d table(s) listed in input but not found in metadata: %s",
                    len(missing_in_metadata),
                    ", ".join(sorted(missing_in_metadata)),
                )
            valid_tables = table_list & sqlite_tables          # must exist in SQLite
            if not force_full_refresh:
                valid_tables &= metadata_tables                 # and in metadata, unless forcing

            tables_to_load.update(valid_tables)
        
        else:
            missing_in_sqlite = metadata_tables - sqlite_tables
            if missing_in_sqlite:
                logger.warning(
                    "Skipping %d table(s) listed in metadata but not found in SQLite: %s",
                    len(missing_in_sqlite),
                    ", ".join(sorted(missing_in_sqlite)),
                )
            
            valid_tables = metadata_tables & sqlite_tables   # must exist in sqlite
            
            tables_to_load.update(valid_tables)

    except Exception as exc:
        logger.exception("Setup failed while preparing SQLite load")
        return SQLiteLoadResult(
            success=False,
            message=f"Setup failed: {type(exc).__name__}: {exc}",
        )

    if not tables_to_load:
        logger.warning("No valid tables found to load")
        return SQLiteLoadResult(success=True, message="No tables to load")

    try:
        for table in sorted(tables_to_load):
            matched = raw_config[
                (raw_config["source"] == "sqlite")
                & (raw_config["is_active"] == 1)
                & (raw_config["table_name"] == table)
            ]

            table_metadata: pd.Series = (
                matched.iloc[0]
                if not matched.empty
                else pd.Series(index=raw_config.columns, dtype=object)
            )
            try:
                copied_df = read_sqlite_table_to_df(
                    spark, 
                    db_file_path, 
                    table, 
                    table_metadata, 
                    force_full_refresh
                )

                raw_refresh_type = table_metadata.get("refresh_type")
                refresh_type = None if pd.isna(raw_refresh_type) else str(raw_refresh_type).strip().lower()

                is_full = force_full_refresh or refresh_type == "full" or refresh_type is None
                write_mode = "overwrite" if is_full else "append"
                schema_option = "overwriteSchema" if is_full else "mergeSchema"
                
                copy_result = load_table_to_lakehouse(
                    spark,
                    copied_df,
                    "raw",
                    table,
                    write_mode,
                    schema_option
                ).raise_if_failed()

                rows_processed += copy_result.rows_processed
                tables_processed += 1

            except Exception as exc:
                logger.exception("Failed to load table %s", table)
                failed_tables[table] = f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        logger.exception("Unexpected failure during load")
        return SQLiteLoadResult(
            success=False,
            rows_processed=rows_processed,
            tables_processed=tables_processed,
            message=f"Load aborted: {type(exc).__name__}: {exc}",
            failed_tables=failed_tables,
        )
    if failed_tables:
        return SQLiteLoadResult(
            success=False,
            rows_processed=rows_processed,
            tables_processed=tables_processed,
            message=(
                f"{len(failed_tables)} of {len(tables_to_load)} table(s) failed: "
                f"{', '.join(sorted(failed_tables))}"
            ),
            failed_tables=failed_tables,
        )

    return SQLiteLoadResult(
        success=True,
        rows_processed=rows_processed,
        tables_processed=tables_processed,
        message="All tables have been loaded into raw!",
    )

