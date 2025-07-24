# RDD-related code snippets will go here.

# --- snippet: RDD Creation, tags: RDD ---
# Create an RDD from a list
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data) 