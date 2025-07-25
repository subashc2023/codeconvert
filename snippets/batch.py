# Batch processing code snippets will go here.

# --- snippet: Simple Batch Job, tags: Batch ---
# This is a placeholder for a batch processing job.
# Typically, this would involve reading from a source, transforming, and writing to a sink.
df.write.format("parquet").save("/tmp/output")

# --- snippet: Read CSV and Show, tags: Batch ---
# Read a CSV file and show the first 5 rows
df = spark.read.option("header", True).csv("/path/to/file.csv")
df.show(5)

# --- snippet: DataFrame Transformations, tags: Batch ---
# Filter and select columns in a DataFrame
filtered = df.filter(df.age > 21).select("name", "age")
filtered.write.mode("overwrite").parquet("/tmp/adults")