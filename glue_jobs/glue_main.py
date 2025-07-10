from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create simple DataFrame
data = [("Alice", 29), ("Bob", 31), ("Charlie", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()
