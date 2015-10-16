__author__ = 'kunal'

import sys
import os

os.environ['SPARK_HOME'] = "spark-1.5.1"
sys.path.append("spark-1.5.1/python/")
sys.path.append("spark-1.5.1/python/lib/py4j-0.8.2.1-src.zip")
sys.path.append("spark-1.5.1/python/lib/py4j/")

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
        .setAppName("Music Recommender")
        .set("spark.executor.memory", "16G")
        .set("spark.driver.memory", "16G")
        .set("spark.executor.cores", "8"))
sc = SparkContext(conf=config)

# Load and parse the data
data = sc.textFile("data/ydata-ymusic-user-artist-ratings-v1_0.txt")
ratings = data.map(lambda l: l.split('\t')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

#Count them
numRatings = ratings.count()
#numUsers = ratings.values().map(lambda r: r[0]).distinct().count()
#numMovies = ratings.values().map(lambda r: r[2]).distinct().count()

print "Got %d ratings" % (ratings.count())

# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 10
model = ALS.train(ratings, rank, numIterations)

# Evaluate the model on training data
testdata = ratings.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

# Save and load model
model.save(sc, "SavedModel")
#sameModel = MatrixFactorizationModel.load(sc, "/Users/kunal/Developer/MusicRecommender")