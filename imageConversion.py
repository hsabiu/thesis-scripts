# Author: Habib Sabiu
# Date: May 14, 2017
# Description: A Spark application to convert still camera images to .png image format.
#              The script simply reads images from HDFS directory and write the output
#              to a networked directory accessible on all the worker nodes
# Usage: spark-submit --master [spark master] [file name] [input path] [output_path] [job name]
#        [spark master] = Can be Spark's Standalone, Mesos, or YARN
#        To run on:-
#                 Standalone: spark://discus-p2irc-master:7077
#                 Mesos: mesos://discus-p2irc-master:5050
#                 YARN: yarn
#        [file name]   = Full path to the python script (../imageConversion.py)
#        [input_path]  = Full HDFS path to input images
#        [output_path] = A networked directory such as NFS3 that is accessible on all the worker nodes
#        [job_name]    = A nice name for the job. This will be displayed on the web UI
# Example usage: spark-submit --master spark://discus-p2irc-master:7077 imageConversion.py
#                hdfs://discus-p2irc-master:54310/user/hduser/habib/still_camera_images/
#                /data/mounted_hdfs_path/user/hduser/habib/converted_images_output/ imageConversion

import os
import cv2
import sys
import string
import random
import pyspark
import numpy as np
import skimage.io as io
import pydoop.hdfs as hdfs

from time import time
from StringIO import StringIO
from  PIL import Image, ImageFile
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

def read_images(rawdata):
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    try:
        return rawdata[0], np.array(io.imread(StringIO(rawdata[1])))
    except (ValueError, IOError, SyntaxError) as e:
        pass

def convertImages(single_image):

    image_full_path = single_image[0]
    image_value = single_image[1]
    last_slash_index = image_full_path.rfind('/')
    last_dot_index = image_full_path.rfind('.')

    image_name = image_full_path[last_slash_index + 1: last_dot_index] + '.png'
    image_path = output_path + '/' + image_name

    return image_path, image_value

def saveImage(image):

    try:
        ImageFile.LOAD_TRUNCATED_IMAGES = True
        io.imsave(image[0], image[1])
    except (ValueError, IOError, SyntaxError, TypeError) as e:
        pass


if __name__ == "__main__":

    application_start_time =  time()

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    job_name = sys.argv[3]

    sc = SparkContext(appName=job_name)

    reading_start_time = time()

    raw_data_rdd = sc.binaryFiles(input_path)
    images_rdd = raw_data_rdd.map(read_images)
    images_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    #images_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    #images_rdd.persist(pyspark.StorageLevel.MEMORY_ONLY_SER)
    #images_rdd.persist(pyspark.StorageLevel.MEMORY_ONLY)

    reading_end_time = time() - reading_start_time

    repartition_start_time = time()
    #repartitioned_rdd = images_rdd.repartition(588)
    #repartitioned_rdd = images_rdd.coalesce(588, shuffle=False)
    #repartitioned_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    repartition_end_time = time() - repartition_start_time

    writing_start_time = time()
    images_rdd.map(convertImages).foreach(saveImage)
    #repartitioned_rdd.map(convertImages).foreach(saveImage)
    writing_end_time = time() - writing_start_time

    #rdd_length = images_rdd.glom().map(len).collect()
    #num_partitions = repartitioned_rdd.getNumPartitions()

    application_end_time = time() - application_start_time

    print '====================================================================='
    print "SUCCESS: Images read into RDD in {} seconds".format(round(reading_end_time, 3))
    print "SUCCESS: RDD repartitioned in {} seconds".format(round(repartition_end_time, 3))
    print "SUCCESS: Images written to HDFS in {} seconds".format(round(writing_end_time, 3))
    print "SUCCESS: Total time = {} seconds".format(round(application_end_time, 3))
    #print(rdd_length[0], min(rdd_length), max(rdd_length), sum(rdd_length)/len(rdd_length), len(rdd_length))
    print '====================================================================='

    sc.stop()
