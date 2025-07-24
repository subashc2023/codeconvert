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