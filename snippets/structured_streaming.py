# Structured Streaming code snippets will go here.

# --- snippet: Structured Streaming from Socket, tags: Structured Streaming ---
from pyspark.sql.functions import explode, split

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

query = wordCounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()

# --- snippet: Structured Streaming File Sink, tags: Structured Streaming ---
# Write streaming output to Parquet files
query = wordCounts.writeStream.outputMode("append").format("parquet").option("path", "/tmp/stream_output").option("checkpointLocation", "/tmp/checkpoint").start()

# --- snippet: Structured Streaming with Watermark, tags: Structured Streaming ---
from pyspark.sql.functions import window
events = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
events_with_ts = events.selectExpr("CAST(value AS STRING)", "timestamp")
windowed_counts = events_with_ts.withWatermark("timestamp", "10 minutes").groupBy(window("timestamp", "5 minutes")).count()