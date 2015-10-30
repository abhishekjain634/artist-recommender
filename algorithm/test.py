__author__ = 'kunal'

import sys
import os
import textToRedis
from numpy import array

os.environ['SPARK_HOME'] = "../spark-1.5.1"
sys.path.append("../spark-1.5.1/python/")
sys.path.append("../spark-1.5.1/python/lib/py4j-0.8.2.1-src.zip")
sys.path.append("../spark-1.5.1/python/lib/py4j/")

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
    print ("Apache-Spark v1.5.1 said, \"All modules found and imported successfully.")

except ImportError as e:
    print ("Couldn't import Spark Modules", e)
    sys.exit(1)

# SETTING CONFIGURATION PARAMETERS
config = SparkConf()
sc = SparkContext(conf=config)

test = sc.textFile("../data/testdata10MB")
model = MatrixFactorizationModel.load(sc, "../TrainedModel")
ratings = test.map(lambda line: array([float(x) for x in line.split('\t')]))
testdata = ratings.map(lambda p: (int(p[0]), int(p[1])))
predictionsRDD = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))

predictions = predictionsRDD.collect()
print predictions

ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictionsRDD)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/ratesAndPreds.count()
print("Mean Squared Error = " + str(MSE))