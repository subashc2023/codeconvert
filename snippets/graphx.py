# GraphX (GraphFrames) related code snippets will go here.

# --- snippet: GraphFrames Creation, tags: GraphX ---
from pyspark.sql import Row
from graphframes import *

# Create a Vertex DataFrame with unique ID column "id"
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])

# Create a GraphFrame
g = GraphFrame(v, e)

# --- snippet: GraphFrames PageRank, tags: GraphX ---
# Run PageRank
results = g.pageRank(resetProbability=0.15, maxIter=10)
results.vertices.show()

# --- snippet: GraphFrames Motif Finding, tags: GraphX ---
# Find motifs (patterns) in the graph
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()