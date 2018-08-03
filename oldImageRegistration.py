# Original Author: Keenan
# Author: Habib Sabiu
# Date: May 14, 2017
# Description: A Spark application to register drone images. Images should be in
#              in a group of 5 chennels. For example, IMG_OOO1 group should have
#              5 images representing various chennels e.g IMG_OOO1_1.png to IMG_OOO1_5.png.
#              The output is a set of 5 registered images for each input group, and RGB of the
#              location, croped version of the RGB, and an NDVI.
# Usage: spark-submit --master [spark master] [file name] [input path] [output_path] [job name]
#        [spark master] = Can be Spark's Standalone, Mesos, or YARN
#        To run on:-
#                 Standalone: spark://discus-p2irc-master:7077
#                 Mesos: mesos://discus-p2irc-master:5050
#                 YARN: yarn
#        [file name]   = Full path to the python script (../imageRegistration.py)
#        [input_path]  = Full HDFS path to input images
#        [output_path] = A networked directory such as NFS3 that is accessible on all the worker nodes
#        [job_name]    = A nice name for the job. This will be displayed on the web UI
# Example usage: spark-submit --master spark://discus-p2irc-master:7077 imageRegistration.py
#                hdfs://discus-p2irc-master:54310/user/hduser/habib/drone_images_png/
#                /data/mounted_hdfs_path/user/hduser/habib/registered_images_output/ imageRegistration


import os
import cv2
import sys
import math
import string
import random
import pyspark
import os.path
import warnings
import argparse
import numpy as np
import skimage.io as io
import pydoop.hdfs as hdfs

from pyspark import SparkContext
from PIL import Image, ImageFile
from StringIO import StringIO
from time import time
from operator import add
from matplotlib import pyplot as plt
from skimage import img_as_ubyte

#from micasense_registrater import register_channels
#from micasense_registrater import transform_channels
#from micasense_registrater import decompose_homography
#import micasense_registrater as reg



#set numpy array to print all it values instead of 3 dots in the middle
#np.set_printoptions(threshold=np.nan)
# Ignore divide by zero warning 
np.seterr(divide='ignore', invalid='ignore')
# Ignore all user warnings
warnings.filterwarnings("ignore")

def find_keypoints_and_features(image):
    
    # Check that image is not invalid
    if image is None:
        raise TypeError("Invalid image in find_keypoints_and_features")   
    
    descriptor = cv2.xfeatures2d.SIFT_create(nfeatures=100000)

    #if fails means can't find similarities between two images
    (key_points, features) = descriptor.detectAndCompute(image, None)

    # IF YOU HAVE CV2 VERSION 2 USE THIS STUFF, INSTEAD OF THE ABOVE TWO LINES
    # #turn the image into greyscale to work with
    
    #grey = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    #detector = cv2.FeatureDetector_create("SURF")
    #key_points = detector.detect(grey)
    #extractor = cv2.DescriptorExtractor_create("SURF")
    #(key_points, features) = extractor.compute(grey, key_points)

    # Convert key_points from KeyPoint objects to numpy arrays
    key_points = np.float32([key_point.pt for key_point in key_points])
    return (key_points, features)


def match_key_points(right_key_points, left_key_points, right_features, left_features, ratio, reproj_thresh):
   
    # A cv2 class that matches keypoint descriptors
    # FLANN is a much faster method for large datasets, so it may be a good
    # idea to switch to that. However it is a very different code set up
    # that uses a couple dictionaries, so there's a bit that'll have to
    # change
    matcher = cv2.DescriptorMatcher_create("BruteForce")
    # knnMatch makes a whole bunch of matches (as a DMatch class)
    # The k stands for how large the tuple will be (because that's
    # basically what DMatches are)
    # i picked two because straight lines
    raw_matches = matcher.knnMatch(right_features, left_features, 2)

    # Turns the raw_matches into tuples we can work with, while also
    # filtering out matches that occurred on the outside edges of the
    # pictures where matches really shouldn't have occurred
    # Is equivalent to the following for loop
    #        matches = []
    #        for m in raw_matches:
    #            if len(m) == 2 and m[0].distance < m[1].distance * ratio:
    #                matches.append((m[0].trainIdx, m[0].queryIdx))
    matches = [(m[0].trainIdx, m[0].queryIdx)
               for m in raw_matches
               if len(m) == 2 and m[0].distance < m[1].distance * ratio]

    # Converts the tuples into a numpy array (for working with the
    # homograph), while also splitting up the right and left points
    # We are making a homograph of the matches to apply a ratio test, and
    # determine which of the matches are of a high quality. Typical ratio
    # values are between 0.7 and 0.8
    # Computing a homography requires at least 4 matches
    if len(matches) > 4:
        # Split right and left into numphy arrays
        src_pts = np.float32([right_key_points[i] for (_, i) in matches])
        dst_pts = np.float32([left_key_points[i] for (i, _) in matches])

        # Use the cv2 to actually connect the dots between the two pictures
        (H, status) = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, reproj_thresh)

        src_t = np.transpose(src_pts)
        dst_t = np.transpose(dst_pts)
        back_proj_error = 0
        inlier_count = 0
        # X coords are [0] and y are [1]
        for i in range(0, src_t.shape[1]):
            x_i = src_t[0][i]
            y_i = src_t[1][i]
            x_p = dst_t[0][i]
            y_p = dst_t[1][i]
            num1 = (H[0][0] * x_i + H[0][1] * y_i + H[0][2])
            num2 = (H[1][0] * x_i + H[1][1] * y_i + H[1][2])
            dnm = (H[2][0] * x_i + H[2][1] * y_i + H[2][2])

	    #print((x_p-num1/dnm)**2)
            tmp = (x_p - (num1 / dnm))**2 + (y_p - (num2 / dnm))**2
            if status[i] == 1:
                back_proj_error += tmp
                inlier_count += 1

        return (matches, H, status, back_proj_error, inlier_count)
    else:
        return None


def register_channels(C, idx=0, ratio=.75, reproj_thresh=4):
    
    # Check that the images in C are good images and not empty
    if C is None:
        raise TypeError("Invalid image set in register_channels")
    for i in C:
        if len(i.shape) > 2:
            raise TypeError("Images have greater depth than 1!")

    # Compute SIFT features for each channel.
    # Channel images are converted to unsigned byte.  All proper scaling
    # is done by image_as_ubyte regardless of dtype of the input images.
    keypoints_and_features = [find_keypoints_and_features(img_as_ubyte(chan)) for chan in C]

    # Generate list of indices excluding the target channel index.
    channels_to_register = list(range(len(C)))
    del channels_to_register[idx]

    # Generate keypoint matches between each channel to be registered
    # and the target image.
    matched_key_points = [match_key_points(keypoints_and_features[i][0], keypoints_and_features[idx][0], keypoints_and_features[i][1],
                                           keypoints_and_features[idx][1], ratio=ratio, reproj_thresh=reproj_thresh)
                          for i in channels_to_register]

    # extract the homography matrices from 'matched_key_points'.
    H = [x[1] for x in matched_key_points]
    BPError = [x[3] for x in matched_key_points]
    Inliers = [x[4] for x in matched_key_points]
    # Add the identity matrix for the target channel.
    H.insert(idx, np.identity(3))
    return H, BPError, Inliers


def warp_image(I, H): 
    return cv2.warpPerspective(I, H, (I.shape[1], I.shape[0]))


def transform_channels(C, H):
    return [warp_image(C[i], H[i]) for i in range(len(C))]


def decompose_homography(H):
   
    if H is None:
        raise TypeError("Invalid homogrpahy input in decompose_homogrphy")
    if H.shape != (3, 3):
        raise TypeError("Invalid homogrpahy shape in decompose_homogrphy")

    a = H[0, 0]
    b = H[0, 1]
    c = H[0, 2]
    d = H[1, 0]
    e = H[1, 1]
    f = H[1, 2]

    p = math.sqrt(a * a + b * b)
    r = (a * e - b * d) / (p)
    q = (a * d + b * e) / (a * e - b * d)

    translation = (c, f)
    scale = (p, r)
    shear = q
    theta = math.atan2(b, a)

    return (translation, theta, scale, shear)

def read_images(image_rawdata):
    print("=================================> Inside read_images <=============================")
    #return image_rawdata[0], np.array(io.imread((StringIO(image_rawdata[1])), as_grey=True) / 65535)
    return image_rawdata[0], np.array(io.imread(StringIO(image_rawdata[1]), as_grey=True))
    

def group_images(single_image):

    lambda rawdata: (rawdata[0][73:rawdata[0].rfind('_')], [rawdata[1]])

def register_group(images_group):   
    
    images_key = images_group[0]
    images_values = images_group[1]
    images_values = sorted(zip(images_values[0::2], images_values[1::2]))
    
    keys = [x[0] for x in images_values]
    values = [x[1] for x in images_values]
    
    # Get the images and store them in an array, then calculate their homographies and transform the images.
    # H, Back-proj-error and the inlier points are all calculated
    C = np.array(values, dtype=float)  / 65535

    H, BPError, Inliers = register_channels(C)
    # Add a 0 to the start of the list of back projection errors, since the
    # first image always has a BPError of 0 (This is for later where we need to print the BPErrors)
    
    BPError.insert(0, 0)
    T = transform_channels(C, H)

    # Decompose the homogrpahy and calculate the bounding box of the
    # good data, where all 5 channels are present
    max_x = []
    max_y = []
    max_theta = []

    for j in H:
        max_x.append(abs(decompose_homography(j)[0][0]))
        max_y.append(abs(decompose_homography(j)[0][1]))
        max_theta.append(abs(decompose_homography(j)[1]))

    rot = math.ceil(math.sin(max(max_theta)) * C[0].shape[1])
    crop_x = math.ceil(max(max_x))
    crop_y = math.ceil(max(max_y))

    border_x = (crop_x + rot, C[0].shape[1] - crop_x - rot)
    border_y = (crop_y + rot, C[0].shape[0] - crop_y - rot)

    # Loop through each subset of images and re-save them now that they are registered

    for j in range(len(T)):
	output_image_path = os.path.abspath(os.path.join(OUTPUT_FILE_PATH, "IMG_" + images_key + "_" + str(j + 1) + OUTPUT_FILE_TYPE))

        #Different ways to save the numpy array as image
        #io.imsave(output_image_path, T[j])

	#Here the array is first converted into a cv2 image and then saved
        cv_image = np.array(T[j]*255)
        cv2.imwrite(output_image_path, cv_image)

	#Here the array is first converted into a PIL image and then saved
	#im = Image.fromarray(T[j])
	#im.save(output_image_path)

    # Create and save the RGB image
    rgb = np.dstack([T[2], T[1], T[0]])
    output_rgb_path = os.path.abspath(os.path.join(OUTPUT_PROCESSED_PATH, "IMG_" + images_key + "_RGB" + OUTPUT_FILE_TYPE))

    #io.imsave(output_rgb_path, rgb)
    
    cv_image = np.array(rgb*255)
    cv2.imwrite(output_rgb_path, cv_image)

    #im = Image.fromarray(rgb)
    #im.save(output_rgb_path)
 
    # Crop images and save
    crop_img = np.dstack([T[2], T[1], T[0]])
    #crop_img = crop_img[border_y[0]:border_y[1], border_x[0]:border_x[1]]
    crop_img = crop_img[int(border_y[0]):int(border_y[1]), int(border_x[0]):int(border_x[1])]
    output_crop_path = os.path.abspath(os.path.join(OUTPUT_PROCESSED_PATH, "IMG_" + images_key  + "_RGB_CROPPED" + OUTPUT_FILE_TYPE))    
    #io.imsave(output_crop_path, crop_img)

    cv_image = np.array(crop_img*255)
    cv2.imwrite(output_crop_path, cv_image)

    #im = Image.fromarray(crop_img)
    #im.save(output_crop_path)

    # Create and save the NDVI image
    num = np.subtract(T[3], T[2])
    dnm = np.add(T[3], T[2])
    
    ndvi_img = np.divide(num, dnm)
    
    original_ndvi = ndvi_img

    output_ndvi_path = os.path.abspath(os.path.join(OUTPUT_PROCESSED_PATH, "IMG_" + images_key  + "_NDVI" + OUTPUT_FILE_TYPE))

    #io.imsave(output_ndvi_path, original_ndvi)

    cv_image = np.array(original_ndvi*255)
    cv2.imwrite(output_ndvi_path, cv_image)

    #im = Image.fromarray(original_ndvi)
    #im.save(output_ndvi_path)

    '''
    # create and save masked NDVI
    ndvi_img[ndvi_img > 0.6] = 0
    ndvi_img[ndvi_img < 0.3] = 0
    ndvi_img[ndvi_img > 0] = 1

    mask_ndvi = ndvi_img

    output_ndvi_mask_path = os.path.abspath(os.path.join(OUTPUT_PROCESSED_PATH, "IMG_" + images_key  + "_NDVI_mask" + OUTPUT_FILE_TYPE))

    #io.imsave(output_ndvi_mask_path, mask_ndvi)
    
    cv_image = np.array(mask_ndvi*255)
    cv2.imwrite(output_ndvi_mask_path, cv_image)

    #im = Image.fromarray(mask_ndvi)
    #im.save(output_ndvi_mask_path)
    '''

if __name__ == "__main__":

    application_start_time =  time()

    input_path = sys.argv[1]
    output_root_path = sys.argv[2]
    job_name = sys.argv[3]

    OUTPUT_FILE_TYPE = ".png"
    # Directory to store registered images
    OUTPUT_FILE_PATH = output_root_path + "/registered_images/"
    # Directory to store processed registered images
    OUTPUT_PROCESSED_PATH = output_root_path + "/registered_images_processed/"
    #OUTPUT_RGB_PATH = output_root_path + "/registered_images_RGB/"
    #OUTPUT_CROP_PATH = output_root_path + "/registered_images_crop/"
    #OUTPUT_NDVI_PATH = output_root_path + "/registered_images_NDVI/"

    # Set spark configurations
    sc = SparkContext(appName = job_name)

    #sc = SparkContext("spark://discus-p2irc-master:7077", "imageRegistration") 
    #sc = SparkContext("local", "imageRegistration")


    reading_start_time = time()

    # Sample input_path = hdfs://discus-p2irc-master:54310/user/hduser/habib/drone_images/06082016/png/IMG_0000_1.png
    # 'start_position' (from the begining of input_path to the IMG_) = 80. Therefore we'll add 1 to start from 81
    # To get image group name, find the substring  from 'start_position' to the location of the next '_" character 

    images_rdd = sc.binaryFiles(input_path)

    # When reading from local file system
    #images_rdd = sc.binaryFiles('file:///sparkdata/registration_images')

    images_group_rdd = images_rdd.map(read_images) \
        .map(lambda rawdata: (rawdata[0][81:rawdata[0].rfind('_')], (rawdata[0][81:], rawdata[1]))) \
        .reduceByKey(lambda first_image, second_image: (first_image + second_image)) 
    

    """
    # The old method using groupByKey(). This is the bad way to do it because it increase the running
    # time of the application

    images_group_rdd = images_rdd.map(read_images) \
        .map(lambda rawdata: (rawdata[0][78:rawdata[0].rfind('_')], rawdata[1])) \
        .groupByKey() \
        .mapValues(list)
    """

    #images_group_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    #images_group_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    #images_group_rdd.persist(pyspark.StorageLevel.MEMORY_ONLY_SER)
    #images_group_rdd.persist(pyspark.StorageLevel.MEMORY_ONLY)

    reading_end_time = time() - reading_start_time 

    processing_start_time = time()


    print("===================> Before registered group <======================")
    print(images_group_rdd.count())
    print("===================> Before registered group <======================")


    images_group_rdd.foreach(register_group)

    processing_end_time = time() - processing_start_time

    application_end_time = time() - application_start_time

    print ("=========================================================================")
    print ("SUCCESS: Images read from HDFS in {} seconds".format(round(reading_end_time, 3)))
    print ("SUCCESS: Images processed and written to HDFS in {} seconds".format(round(processing_end_time, 3)))
    print ("SUCCESS: Total time spent = {} seconds".format(round(application_end_time, 3)))
    print ("=========================================================================")


    """   
    # This is just for debugging purposes

    first_rdd_group_key = images_group_rdd.first()[0]
    first_rdd_tuple = images_group_rdd.first()[1]

    images_values = sorted(zip(first_rdd_tuple[0::2], first_rdd_tuple[1::2]))

    keys = [x[0] for x in images_values]
    values = [x[1] for x in images_values]

    first_rdd_image_name = keys[0]
    first_rdd_image_value = values[0]

    print "RDD partitions count ===> " + str(images_group_rdd.count())
    print "First RDD group key ===> " + str(first_rdd_group_key)
    print "First RDD keys ===> " + str(keys)
    print "First RDD values ===> " + str(values)
    print "Lenght of first RDD tuple ===> " + str(len(first_rdd_tuple))
    print "First RDD tuple ===> " + str(first_rdd_tuple)
    print "First RDD tuple sorted ===> " + str(images_values)
    print "First RDD image name ===> " + str(first_rdd_image_name)
    print "First image shape ===> " + str(np.array(first_rdd_image_value).shape)
    print "Lenght for first RDD image value ===> " + str(len(first_rdd_image_value))
    
    rdd_length = images_group_rdd.glom().map(len).collect()
    print(rdd_length[0], min(rdd_length), max(rdd_length), sum(rdd_length)/len(rdd_length), len(rdd_length))
    """

    sc.stop()    



