from pyspark.sql import SparkSession

def main():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("PySpark Test") \
        .getOrCreate()

    # Print Spark version to verify it’s working
    print("Spark version:", spark.version)

    # Create a simple DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, schema=columns)

    # Show the DataFrame
    print("DataFrame content:")
    df.show()

    # Perform a basic transformation and action
    df_filtered = df.filter(df.Age > 30)
    print("Filtered DataFrame content (Age > 30):")
    df_filtered.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
