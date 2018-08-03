
from __future__ import print_function
from numpy import array
from math import sqrt
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel

if __name__ == "__main__":

    sc = SparkContext(appName="KMeansExample")  # SparkContext

    # Load and parse the data
    data = sc.textFile("habib/kmeans_data.txt")
    parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

    # Build the model (cluster the data)
    model = KMeans.train(parsedData, 3, maxIterations=20, initializationMode="random")

    # Evaluate clustering by computing Within Set Sum of Squared Errors
    def error(point):
        center = model.centers[model.predict(point)]
        return sqrt(sum([x**2 for x in (point - center)]))

    WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)

    # Save and load model
    #model.save(sc, "habib/KMeansModel")
    #loadedModel = KMeansModel.load(sc, "habib/KMeansModel")

    test_data_1 = array([0.2, 0.5, 0.9])
    test_data_2 = array([9, 9.6, 9.10])
    test_data_3 = array([18.7, 18.6, 18.10])

    predicted_class_1 = model.predict(test_data_1)
    predicted_class_2 = model.predict(test_data_2)
    predicted_class_3 = model.predict(test_data_3)

    number_of_clusters = model.k

    print ("=========================================================================")
    print ("Within Set Sum of Squared Error = " + str(WSSSE))
    print ("Number of clusters = " + str(number_of_clusters))
    print ("Predicted class = " + str(predicted_class_1) + " " + str(predicted_class_2) + " " + str(predicted_class_3))
    print ("=========================================================================")

    sc.stop()


