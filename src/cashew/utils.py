import logging
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from cashew.models import OperationResult

logger = logging.getLogger(__name__)


VALID_WRITE_MODES = {"append", "overwrite", "errorifexists", "ignore"}
VALID_SCHEMA_OPTIONS = {"mergeSchema", "overwriteSchema"}

def load_table_to_lakehouse(
    spark: SparkSession,
    source_table: DataFrame,
    sink_schema: str,
    sink_table: str,
    write_mode: str = "append",
    schema_option: str = "mergeSchema",
) -> OperationResult:
    """Write a DataFrame to a Delta table in the lakehouse.

    The target schema is created if it does not exist. The DataFrame is
    written with the given save mode and schema option, and the row count is
    read back from the latest entry in the table's Delta commit history
    (``numOutputRows``). Invalid arguments and write failures are not raised;
    they are returned as a failed ``OperationResult`` with the exception
    attached in its ``error`` field.

    Args:
        spark: Active Spark session.
        source_table: DataFrame to write.
        sink_schema: Name of the target schema. Created if missing.
        sink_table: Name of the target Delta table.
        write_mode: Spark save mode, case-insensitive. One of ``append``,
            ``overwrite``, ``errorifexists`` or ``ignore``.
        schema_option: How to handle schema differences. ``mergeSchema`` adds
            new columns to the target schema. ``overwriteSchema`` replaces the
            target schema and is only valid with ``write_mode='overwrite'``.

    Returns:
        An ``OperationResult``. On success, ``success`` is True and
        ``rows_processed`` holds the number of rows written. On failure,
        ``success`` is False and ``error`` holds the exception. Failures
        include:

        - ``write_mode`` or ``schema_option`` is not a valid value
          (``ValueError``).
        - ``schema_option`` is ``overwriteSchema`` while ``write_mode`` is not
          ``overwrite`` (``ValueError``).
        - Any exception raised while creating the schema, writing the table,
          or reading the commit history (for example, ``AnalysisException``
          when ``errorifexists`` is used and the table already exists).
    """
    write_mode = write_mode.lower()
    full_table_name = f"`{sink_schema}`.`{sink_table}`"

    # config errors: return a failed result instead of raising
    if write_mode not in VALID_WRITE_MODES:
        msg = f"Invalid write_mode '{write_mode}'. Use one of {sorted(VALID_WRITE_MODES)}"
        return OperationResult(
            success=False,
            message=msg,
            error=ValueError(msg)
        )
    if schema_option not in VALID_SCHEMA_OPTIONS:
        msg = f"Invalid schema_option '{schema_option}'. Use one of {sorted(VALID_SCHEMA_OPTIONS)}"
        return OperationResult(
            success=False,
            message= msg,
            error=ValueError(msg)
        )
    if schema_option == "overwriteSchema" and write_mode != "overwrite":
        msg = "'overwriteSchema' can only be used with write_mode='overwrite'"
        return OperationResult(
            success=False,
            message=msg,
            error=ValueError(msg)
        )

    try:
        if not spark.catalog.databaseExists(sink_schema):
            logger.info("Schema %s not found; creating it", sink_schema)
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{sink_schema}`")

        logger.info(
            "Writing to %s (mode=%s, %s=true)", full_table_name, write_mode, schema_option
        )

        (
            source_table.write
            .format("delta")
            .mode(write_mode)
            .option(schema_option, "true")
            .saveAsTable(full_table_name)
        )

        # row count from the Delta commit log
        row = (
            DeltaTable.forName(spark, full_table_name)
            .history(1)
            .select("operationMetrics")
            .first()
        )

        metrics: dict[str, str] = row["operationMetrics"] if row else {}
        rows_written = int(metrics.get("numOutputRows", 0))

        return OperationResult(
            success=True,
            rows_processed=rows_written,
            message=f"Wrote {rows_written} rows to {full_table_name}",
        )

    except Exception as exc:
        logger.exception("Failed to write to %s", full_table_name)
        return OperationResult(
            success=False,
            rows_processed=0,
            message=f"Write to {full_table_name} failed fatally",
            error=exc
        )