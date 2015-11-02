__author__ = 'kunal'

import sys
import os

os.environ['SPARK_HOME'] = "../spark-1.5.1"
sys.path.append("../spark-1.5.1/python")
sys.path.append("../spark-1.5.1/python/lib/py4j-0.8.2.1-src.zip")
sys.path.append("../spark-1.5.1/python/lib/py4j")

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
    print ("Apache-Spark v1.5.1 said, \"All modules found and imported successfully.")

except ImportError as e:
    print ("Couldn't import Spark Modules", e)
    sys.exit(1)

# SETTING CONFIGURATION PARAMETERS
config = (SparkConf()
        .setMaster("local")
        .setAppName("Artist Recommender")
        .set("spark.executor.memory", "8G")
        .set("spark.driver.memory", "8G"))
sc = SparkContext(conf=config)

# Load and parse the data
data = sc.textFile('../data/10Mdata')
ratings = data.map(lambda l: l.split('\t')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

#Count
numRatings = ratings.count()
print "Got %d ratings" % (ratings.count())

# Build the recommendation model using Alternating Least Squares
model = ALS.train(ratings, rank=10, iterations=10)

# Evaluate the model on training data
testdata = ratings.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

# Save model
model.save(sc, "../TrainedModel")

