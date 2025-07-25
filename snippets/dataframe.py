# DataFrame-related code snippets will go here.

# --- snippet: DataFrame Creation, tags: DataFrame / Spark SQL ---
# Create a DataFrame from a list of tuples
from pyspark.sql import Row
df = spark.createDataFrame([
    Row(name='Alice', age=5, height=80),
    Row(name='Bob', age=5, height=80),
    Row(name='Charlie', age=5, height=80)
])

# --- snippet: DataFrame Aggregation, tags: DataFrame ---
# Group by and aggregate
result = df.groupBy("age").agg({"height": "avg"})
result.show()

# --- snippet: DataFrame Join, tags: DataFrame ---
# Join two DataFrames
df2 = spark.createDataFrame([
    Row(name='Alice', city='NY'),
    Row(name='Bob', city='SF')
])
joined = df.join(df2, on="name", how="inner")
joined.show()