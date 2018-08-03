from pyspark import SparkContext
from StringIO import StringIO
from time import time
from  PIL import Image, ImageFilee

import pyspark
import numpy as np
import pydoop.hdfs as hdfs
import os
import string
import random
import cv2

sc = SparkContext("spark://discus-p2irc-master:7077", "imageSharpening")
#sc = SparkContext("local", "sharpenedImages")

processing_start_time = time()

images_rdd = sc.binaryFiles('hdfs://discus-p2irc-master:54310/user/hduser/landsat_images', 100)
#images_rdd = sc.binaryFiles('file:///sparkdata/p2irc-images', 264)

#ImageFile.LOAD_TRUNCATED_IMAGES = True
#images_to_bytes = lambda rawdata: Image.open(StringIO(rawdata)).convert('RGB')

def images_to_bytes(rawdata):
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    return (rawdata[0], Image.open(StringIO(rawdata[1])).convert('RGB'))

images_bytes = images_rdd.values().map(images_to_bytes)
images_bytes.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

processing_end_time = time() - processing_start_time

'''
def random_generator(size=20, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
'''

def sharpenImage(single_image):

    image_full_path = single_image[0]
    image_value = single_image[1]
    last_slash_index = image_full_path.rfind('/')
    last_dot_index = image_full_path.rfind('.')
    image_name = image_full_path[last_slash_index + 1: last_dot_index] + '.png'

    #imageName = 'sharpened_image_' + random_generator() + '.png'
    local_path = '/sparkdata/tmp-dir/' + image_name
    hdfs_path = 'hdfs://discus-p2irc-master:54310/user/hduser/processed_images/' + image_name

    blurred_image = ndimage.gaussian_filter(image_value, 3)
    filter_blurred_image = ndimage.gaussian_filter(blurred_image, 1)

    alpha = 30
    sharpened_image = blurred_image + alpha * (blurred_image - filter_blurred_image)

    try:
        ImageFile.LOAD_TRUNCATED_IMAGES = True
        cv_image = np.array(sharpend_image)
        cv_image = cv_image[:, :, ::-1].copy()
        cv2.imwrite(local_path, cv_image)

        hdfs.put(local_path, hdfs_path)
        os.remove(local_path)        

    except (ValueError, IOError, SyntaxError) as e:
        print '==============================='
        print 'ERROR: Bad Image Error Returned'
        print '==============================='
        os.remove(local_path)
        #pass
        return

writing_to_local_start_time = time()

images_bytes.foreach(sharpenImage)

writing_to_local_end_time = time() - writing_to_local_start_time

print '=========================================================================='
print "SUCCESS: Images procesed in {} seconds".format(round(processing_end_time, 3))
print "SUCCESS: Images written to HDFS in {} seconds".format(round(writing_to_local_end_time, 3))
print '=========================================================================='

