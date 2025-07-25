# DStream-related code snippets will go here.

# --- snippet: DStream from Socket, tags: DStream ---
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination()

# --- snippet: DStream File Stream, tags: DStream ---
# Monitor a directory for new text files
text_dstream = ssc.textFileStream("/tmp/streaming_data")
text_dstream.pprint()

# --- snippet: DStream Window Operations, tags: DStream ---
# Count words over a window
windowed_counts = wordCounts.reduceByKeyAndWindow(lambda x, y: x + y, windowDuration=30, slideDuration=10)
windowed_counts.pprint()