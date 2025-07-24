# DataFrame-related code snippets will go here.

# --- snippet: DataFrame Creation, tags: DataFrame / Spark SQL ---
# Create a DataFrame from a list of tuples
from pyspark.sql import Row
df = spark.createDataFrame([
    Row(name='Alice', age=5, height=80),
    Row(name='Bob', age=5, height=80),
    Row(name='Charlie', age=5, height=80)
]) 