from delta import configure_spark_with_delta_pip
import os
from pyspark.sql import SparkSession
from cashew.config import WAREHOUSE_DIR

def create_spark_session() -> SparkSession:
    """Create and return a local Spark session with Delta Lake configured."""

    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    
    builder = (
        SparkSession.builder
        .appName("Cashew")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
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
        .config( # set the hive metastore thrift api (container: hive-metastore, host: localhost)
            "spark.hadoop.hive.metastore.uris",
            os.environ.get("METASTORE_URI", "thrift://localhost:9083"),
        )
        .config( # set hive version
            "spark.sql.hive.metastore.version", 
            "4.0.1"
        )
        .config( # set metastore jar to download form maven
            "spark.sql.hive.metastore.jars", 
            "maven"
        )
        .enableHiveSupport()
    )

    return configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.xerial:sqlite-jdbc:3.46.0.0"],  # sqlite support in spark
    ).getOrCreate()
