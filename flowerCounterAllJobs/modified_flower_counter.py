# Original Author: Javier Garcia
# Modified: Habib Sabiu
# Date: March 09, 2018
#

import os
import cv2
import sys
import glob
import pyspark
import operator
import subprocess
import numpy as np
import skimage.io as io


from time import time
from io import StringIO, BytesIO
from PIL import Image, ImageFile
from skimage.feature import blob_doh
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.linalg import Vectors
from pyspark import SparkConf, SparkContext

from timelapse_image import *


def computeHistogramsOnSingleImage(canolaTimelapseImage):
    plotMask = canolaTimelapseImage.getPlotMask()
    im_bgr = canolaTimelapseImage.readImage()
    im_gray = cv2.cvtColor(im_bgr, cv2.COLOR_BGR2GRAY)
    im_lab_plot = cv2.cvtColor(im_bgr, cv2.COLOR_BGR2Lab)
    im_gray = im_gray[plotMask > 0]
    im_lab_plot = im_lab_plot[plotMask > 0]
    hist_G, _ = np.histogram(im_gray, 256, [0, 256])
    hist_b, _ = np.histogram(im_lab_plot[:, 2], 256, [0, 256])

    histObject = canolaTimelapseImage.getHistogramObject()
    histObject.setHistogramB(hist_b)
    histObject.setHistogramG(hist_G)

    return canolaTimelapseImage


def udpateHistogramBShift(canolaTimelapseImageDic, additionalShift):
    key = next(iter(canolaTimelapseImageDic))
    value = canolaTimelapseImageDic[key]
    key.getHistogramObject().setHistogramBShift(value + additionalShift)
    return key


def CorruptedImagesDetector(canolaTimelapseImage):

    hist_g = canolaTimelapseImage.getHistogramObject().getHistogramG()
    if max( hist_g ) / np.sum( hist_g ) >= 0.2:
        canolaTimelapseImage.getClassifierObject().flagCorrupted()

    return canolaTimelapseImage


def calculateNumberOfFlowerPixels(canolaTimelapseImage):

    fileToFlowerPixelsDict = {}

    histObject = canolaTimelapseImage.getHistogramObject()
    hist_b = histObject.getHistogramB()
    hist_b_shift = histObject.getHistogramBShift()
    threshold = (155 + hist_b_shift) % 256
    n_flower_pixels = np.sum(hist_b[threshold:])
    canolaTimelapseImage.getClassifierObject().setNumberOfYellowPixels(n_flower_pixels)

    fileToFlowerPixelsDict[canolaTimelapseImage] = n_flower_pixels

    return fileToFlowerPixelsDict


def fitKmeans(points_rdd, num_clusters):

    model = KMeans.train(points_rdd, num_clusters, maxIterations=10, initializationMode="random")
    cluster_centers = model.clusterCenters

    return cluster_centers


def assignImagesToClusters(canolaTimelapseImage, km_centroids):

    final_img_clusters = {}

    key = next(iter(canolaTimelapseImage))
    value = canolaTimelapseImage[key]

    # Compute distance to each of the centroids
    dist = np.array([abs(value - q) for q in km_centroids])

    # Get the closest centroid
    centroid = int(dist.argmin())

    key.getClassifierObject().setCluster(centroid)

    final_img_clusters[key] = value

    return final_img_clusters


def assignBlobs(canolaTimelapseImage):

    blobs = flowerCountPipeline.run(canolaTimelapseImage)

    canolaTimelapseImage.getFlowerCountObject().setBlobs(blobs)

    return canolaTimelapseImage


def read_images(image_rawdata, corners):

    ImageFile.LOAD_TRUNCATED_IMAGES = True

    regionObject = CanolaPlotRegionObject()
    regionObject.setPlot('1237')
    regionObject.setCorners(corners)

    imgPath = image_rawdata[0]
    imgData = np.array(io.imread(BytesIO(image_rawdata[1])))

    img = CanolaTimelapseImage()
    img.setPath(imgPath)
    img.setRegionObject(regionObject)
    img.setImageSize((720,1280))
    img.setImageArray(imgData[:, :, ::-1].copy())

    return img


if __name__ == "__main__":

    application_start_time = time()

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    job_name = sys.argv[3]
    corners = [(10, 10), (1270, 10), (10, 710), (1270, 710)]

    # Remove the output directory and all it contents if it already exist
    subprocess.call(["hadoop", "fs", "-rm", "-r", output_path])

    flowerCountPipeline = ImageProcessingPipeline()
    flowerCountPipeline.addModule(CIELabColorspaceModule())
    flowerCountPipeline.addModule(SigmoidMapping())
    flowerCountPipeline.addModule(BlobDetection())

    sc = SparkContext(appName=job_name)

    raw_data_rdd = sc.binaryFiles(input_path)

    histResult_rdd = raw_data_rdd.map(lambda x: read_images(x, corners)) \
        .map(lambda x: computeHistogramsOnSingleImage(x))

    refHist = histResult_rdd.first().getHistogramObject().getHistogramB()

    seqOp = (lambda local_result, list_element: (local_result[0] + list_element, local_result[1] + 1) )
    combOp = (lambda some_local_result, another_local_result: (some_local_result[0] + another_local_result[0], some_local_result[1] + another_local_result[1]))
    aggregate = histResult_rdd.map(lambda x: x.getHistogramObject().getHistogramB()) \
        .aggregate( (0, 0), seqOp, combOp)
    average_histogram = aggregate[0]/aggregate[1]

    correlation_reference = np.correlate(refHist,average_histogram,"full")
    additionalShift = correlation_reference.argmax().astype(np.uint8)

    pipeline_rdd = histResult_rdd.zipWithIndex() \
        .map(lambda x: {x[0]: 0} if x[1] == 0 else {x[0]: (np.correlate(x[0].getHistogramObject().getHistogramB(), refHist, "full")).argmax().astype(np.int8)}) \
        .map(lambda x: udpateHistogramBShift(x, additionalShift)) \
        .map(lambda x: CorruptedImagesDetector(x)) \
        .filter(lambda x: x.isCorrupted() == False) \
        .map(lambda x: calculateNumberOfFlowerPixels(x)) \
        .map(lambda x: next(iter(x))) \
        .map(lambda x: assignBlobs(x)) \
        .map(lambda x: (x.getPath(),len(x.getFlowerCountObject().getBlobs()))) \
        .coalesce(1, shuffle=True) \
        .saveAsTextFile(output_path)

    application_end_time = time() - application_start_time

    sc.stop()

    print("---------------------------------------------------")
    print("SUCCESS: Images procesed in {} seconds".format(round(application_end_time, 3)))
    print("---------------------------------------------------")

