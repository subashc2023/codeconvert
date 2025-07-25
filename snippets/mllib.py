# MLlib-related code snippets will go here.

# --- snippet: K-Means Clustering, tags: MLlib ---
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Loads data.
dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# --- snippet: Logistic Regression, tags: MLlib ---
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(maxIter=10, regParam=0.01)
model = lr.fit(dataset)
predictions = model.transform(dataset)

# --- snippet: ML Pipeline, tags: MLlib ---
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
pipeline = Pipeline(stages=[assembler, scaler, kmeans])
pipeline_model = pipeline.fit(dataset)