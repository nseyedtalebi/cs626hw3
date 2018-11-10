from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql.context import SparkSession
from pyspark.ml.feature import VectorAssembler
import os

os.environ["PYSPARK_PYTHON"]="/home/nima/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"]="/home/nima/anaconda3/bin/python"

spark = SparkSession.builder\
    .master("local")\
    .appName("Word Count")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()

patients = spark.read.csv("/home/nima/code/cs626/hw3/src/main/resources/patients3.csv",header=True,inferSchema=True)
assembler = VectorAssembler(inputCols=['height','weight','waist','diasbp','systbp'],outputCol='features')
patients_feats = assembler.transform(patients)
kmeans = KMeans(maxIter=100, k=15)
model = kmeans.fit(patients_feats)
costs = model.computeCost(patients_feats)
print(costs)
centers = model.clusterCenters()
print(centers)
transformed = model.transform(patients_feats)
transformed.show()

#height,weight, waist, diasbp (Diastolic Blood Pressure), and systbp (Systolic Blood Pressure)