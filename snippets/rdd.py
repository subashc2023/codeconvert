# RDD-related code snippets will go here.

# --- snippet: RDD Creation, tags: RDD ---
# Create an RDD from a list
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# --- snippet: RDD Map and Reduce, tags: RDD ---
# Map and reduce on RDD
result = rdd.map(lambda x: x * 2).reduce(lambda a, b: a + b)

# --- snippet: RDD Filter and Collect, tags: RDD ---
# Filter and collect results
filtered = rdd.filter(lambda x: x % 2 == 0).collect()