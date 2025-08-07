# utils/sparkConfig.py

from pyspark.sql import SparkSession
from contextlib import contextmanager

@contextmanager
def initialize_spark_session(script_name: str):
    """
    Initializes a Spark session with predefined settings optimized for performance and usability.
    This function is a context manager to ensure that the Spark session is properly closed after use.
    
    Parameters:
        script_name (str): The name of the application, useful for identifying the Spark application on the cluster.
    
    Usage:
        with initialize_spark_session('MySparkApp') as spark:
            # Your Spark code here
    """

    # Get the existing Spark session or create a new one if it doesn't exist
    spark = SparkSession.builder.appName(script_name).getOrCreate()

    # Apply custom configurations
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    spark.conf.set("spark.dynamicAllocation.enabled", "true")
    spark.conf.set("spark.dynamicAllocation.initialExecutors", 2)
    spark.conf.set("spark.dynamicAllocation.minExecutors", 1)
    spark.conf.set("spark.dynamicAllocation.maxExecutors", 10)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.executor.memory", "20g")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    spark.conf.set("spark.speculation", "true")
    spark.conf.set("spark.sql.session.timeZone", "America/Chicago")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")
    spark.conf.set("spark.rdd.compress", "true")
    spark.conf.set("spark.shuffle.compress", "true")
    spark.cof.set()
    spark.conf.set("spark.shuffle.spill.compress", "true")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "true")  # Enable Hive support

    spark.sparkContext.setLogLevel('WARN')  # Set log level to WARN to reduce verbosity

    successfully_closed = False
    try:
        yield spark  # Yield the spark session to be used in the 'with' block
    finally:
        if spark is not None:
            try:
                spark.stop()  # Stop the Spark session
                successfully_closed = True
            except Exception as e:
                print(f"Error closing Spark session: {e}")

        if not successfully_closed:
            print("Spark session wasn't closed successfully. Additional cleanup or retry logic here.")