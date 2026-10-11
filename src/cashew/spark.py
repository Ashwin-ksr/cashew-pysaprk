from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from cashew.config import METASTORE_DIR, WAREHOUSE_DIR

def create_spark_session() -> SparkSession:
    """Create and return a local Spark session with Delta Lake configured."""

    METASTORE_DIR.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    
    builder = (
        SparkSession.builder
        .appName("Cashew")
        .master("local[*]")
        .config( # add delta extension
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config( # set delta catalog
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config( # set local warehouse directory
            "spark.sql.warehouse.dir", str(WAREHOUSE_DIR)
        )
        .config( # enable hive catalog in local via derby
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={METASTORE_DIR / 'metastore_db'};create=true"
        )
        .enableHiveSupport()
    )

    return configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.xerial:sqlite-jdbc:3.46.0.0"],  # sqlite support in spark
    ).getOrCreate()
