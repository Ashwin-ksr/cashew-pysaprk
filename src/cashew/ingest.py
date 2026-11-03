from collections.abc import Sequence
from datetime import datetime, timezone
import logging
import pandas as pd
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, functions as F
import re
import sqlite3
from cashew.config import METADATA_XL, WAREHOUSE_DIR
from cashew.utils import load_table_to_lakehouse

__all__ = [
    "get_new_db_file", 
    "get_sqlite_tables_list", 
    "resolve_sqlite_tables", 
    "load_sqlite_table",
]

logger = logging.getLogger(__name__)

def _load_raw_config() -> pd.DataFrame:
    """Return the active SQLite rows of the ``raw_config`` metadata sheet."""
    config = pd.read_excel(
        WAREHOUSE_DIR / "metadata" / METADATA_XL, sheet_name="raw_config"
    )
    return config[(config["source"] == "sqlite") & (config["is_active"] == 1)]

def get_new_db_file() -> str | None:
    """
    Return the latest SQLite file from the landing zone that is not loaded.
    """
    
    # get files from landing zone
    landing_zone = WAREHOUSE_DIR / "landing"

    # early exit if no file is present
    if not any(landing_zone.iterdir()):
        logger.info("No files found in landing zone")
        return

    landing_zone_files: dict[str, datetime] = {}
    FILENAME_PATTERN = re.compile(
        r"^cashew-db-v\d+-.*?(?P<timestamp>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-\d{3})Z\.sqlite$"
    )
    
    for file in landing_zone.iterdir():
        if file.is_file() and file.name.endswith(".sqlite"):
            match = FILENAME_PATTERN.match(file.name)
            if match:
                raw_ts = match.group("timestamp")
                parsed_ts = datetime.strptime(raw_ts, "%Y-%m-%d-%H-%M-%S-%f").replace(
                    tzinfo=timezone.utc
                )
                landing_zone_files[file.name] = parsed_ts

    if not landing_zone_files:
        logger.info("No files matching the Cashew pattern in landing zone")
        return

    latest_file: str = max(landing_zone_files, key=landing_zone_files.__getitem__)    
    return latest_file

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
    def _clean(value) -> str:
        """Return a stripped string, treating None/NaN as empty."""
        return "" if pd.isna(value) else str(value).strip()
    
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

def resolve_sqlite_tables(
    db_file_name: str,
    force_full_refresh: bool = False,
    table_names: str | Sequence[str] = (),
) -> list[str]:
    """Return the sorted list of tables to load from a SQLite file.

    Tables requested (or configured in metadata) but missing from SQLite
    are skipped with a warning. Unless ``force_full_refresh`` is set,
    requested tables must also be present in the metadata.

    Args:
        db_file_name: Name of the SQLite file in the landing directory.
        force_full_refresh: If True, tables not listed in the metadata are
            allowed when requested explicitly.
        table_names: Optional table name or names to restrict the load to.

    Returns:
        Sorted table names that should be loaded. May be empty.

    Raises:
        FileNotFoundError: If the SQLite file does not exist.
        sqlite3.Error: If the SQLite file cannot be queried.
        Exception: If the metadata workbook cannot be read.
    """
    if not (WAREHOUSE_DIR / "landing" / db_file_name).exists():
        raise FileNotFoundError(f"SQLite file not found: {db_file_name}")

    if isinstance(table_names, str):
        requested: set[str] = {t for t in [table_names.strip()] if t}
    else:
        requested: set[str] = {t.strip() for t in table_names if t.strip()}

    # gets the set of all active sqlite tables from metadata
    metadata_tables = set(_load_raw_config()["table_name"].to_list())
    # gets the set tables in the sqlite file
    sqlite_tables = set(get_sqlite_tables_list(db_file_name))

    wanted = requested or metadata_tables
    source_label = "input" if requested else "metadata"

    missing_in_sqlite = wanted - sqlite_tables
    if missing_in_sqlite:
        logger.warning(
            "Skipping %d table(s) listed in %s but not found in SQLite: %s",
            len(missing_in_sqlite), source_label, ", ".join(sorted(missing_in_sqlite)),
        )

    valid = wanted & sqlite_tables
    if requested and not force_full_refresh:
        missing_in_metadata = requested - metadata_tables
        if missing_in_metadata:
            logger.warning(
                "Skipping %d table(s) listed in input but not found in metadata: %s",
                len(missing_in_metadata), ", ".join(sorted(missing_in_metadata)),
            )
        valid &= metadata_tables

    return sorted(valid)

def load_sqlite_table(
    spark: SparkSession,
    db_file_name: str,
    table: str,
    force_full_refresh: bool = False,
) -> int:
    """Read one SQLite table and write it to the ``raw`` Delta schema.

    Uses ``overwrite`` for full refreshes (or when ``refresh_type`` is
    missing) and ``append`` for incremental refreshes.

    Args:
        spark: Active Spark session.
        db_file_name: Name of the SQLite file in the landing directory.
        table: Name of the table to load.
        force_full_refresh: If True, overwrite regardless of ``refresh_type``.

    Returns:
        The number of rows written.

    Raises:
        ValueError: If incremental is configured without a watermark column.
        Exception: If the read or the Delta write fails.
    """
    config = _load_raw_config()
    matched = config[config["table_name"] == table]
    table_metadata: pd.Series = (
        matched.iloc[0] if not matched.empty
        else pd.Series(index=config.columns, dtype=object)
    )

    df = read_sqlite_table_to_df(
        spark,
        WAREHOUSE_DIR / "landing" / db_file_name,
        table,
        table_metadata,
        force_full_refresh,
    )

    raw_type = table_metadata.get("refresh_type")
    refresh_type = None if pd.isna(raw_type) else str(raw_type).strip().lower()
    is_full = force_full_refresh or refresh_type in (None, "full")

    return load_table_to_lakehouse(
        spark,
        df,
        "raw",
        table,
        "overwrite" if is_full else "append",
        "overwriteSchema" if is_full else "mergeSchema",
    ).raise_if_failed().rows_processed
