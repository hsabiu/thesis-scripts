# Original Author: --
# Modified: Habib Sabiu
# Date: May 14, 2017
# Description: A Spark application to stitch multiple drone images of different locations
#              of a farmland into a single panorama image
# NOTE: This application is currently not working. It does not give the correct output

import os
import cv2
import glob
import string
import pyspark
import os.path
import argparse
import numpy as np
import skimage.io as io

from time import time
from math import sqrt
from math import atan
from operator import add
from StringIO import StringIO
from PIL import Image, ImageFile
from pyspark import SparkContext

OUTPUT_FILE_TYPE = ".png"
OUTPUT_FILE_PATH = "/sparkdata/hduser_images/user/hduser/stitched_images/"

sc = SparkContext("spark://discus-p2irc-master:7077", "imageStitching")


def warp_perspective(img1, img2, H, affine=False, skip=False):

    # Gets the width and height of the current images for future point
    # reading use
    h1, w1 = img1.image.shape[:2]
    h2, w2 = img2.image.shape[:2]

    # Convert the top left, bottom left, bottom right, top left
    # (respectively) points of the image into numpy arrays
    pts1 = np.float32([[0, 0],
                       [0, h1],
                       [w1, h1],
                       [w1, 0]]
                      ).reshape(-1, 1, 2)
    pts2 = np.float32([[0, 0],
                       [0, h2],
                       [w2, h2],
                       [w2, 0]]
                      ).reshape(-1, 1, 2)

    # Find dimensions of the transformed right image with a homograph for
    # things to be the correct size for the future warp
    pts2_ = cv2.perspectiveTransform(pts2, H)

    # If the user wants an affine transformation (removes warping and
    # keeps parellel lines parellel), remove the bottom line of the
    # homograph. This is the quickest method to both warp the photo to the
    # best of SURF abilities and still keep it affine
    if affine:
        [[a, b, c], [d, e, f], [g, h, i]] = H
        H = [[a, b, c], [d, e, f], [0, 0, 1]]

    pts = np.concatenate((pts1, pts2_), axis=0)
    [xmin, ymin] = np.int32(pts.min(axis=0).ravel() - 0.5)
    [xmax, ymax] = np.int32(pts.max(axis=0).ravel() + 0.5)
    t = [-xmin, -ymin]

    # The homograph that will move the image around with the given
    # homograph so that no edges are cropped off, and most of the black
    # border is gone
    Ht = np.array([[1, 0, t[0]], [0, 1, t[1]], [0, 0, 1]])  # translate

    # Using the calculated homograph and the given homograph, warp the img
    result = Image(None, None)

    if skip:
        result.image = img2.image
    else:
        result.image = cv2.warpPerspective(img2.image,
                                           Ht.dot(H),
                                           (xmax - xmin, ymax - ymin))

    # Place the unwarped photo over top of the warped one. You now have a
    # stitched image!
    result.image[t[1]:h1 + t[1],
    t[0]:w1 + t[0]] = overlay_image(
        result.image[t[1]:h1 + t[1], t[0]:w1 + t[0]], img1.image)

    # Update the unwarped image's coordinates, since it is now a part of
    # the stitched picture
    img1.place_image(t[0], t[1], w1 + t[0], h1 + t[1])

    # Return the stitched image, the image that was placed on top of the
    # result (with an updated current coordinates), and the homograph for
    # bookkeeping reasons
    return result, img1, Ht.dot(H)


def apply_homograph_to_points(images, H):

    for img in images:
        img.warp_points(H)
    return images


def find_keypoints_and_features(image):

    descriptor = cv2.xfeatures2d.SURF_create(upright=False)

    # if fails means can't find similarities between two images
    (key_points, features) = descriptor.detectAndCompute(image, None)

    # =============================================================================
    #         IF YOU HAVE CV2 VERSION 2 USE THIS STUFF, INSTEAD OF THOSE TWO LINES
    #         #turn the image into greyscale to work with
    #         grey = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    #         detector = cv2.FeatureDetector_create("SURF")
    #         key_points = detector.detect(grey)
    #         extractor = cv2.DescriptorExtractor_create("SURF")
    #         (key_points, features) = extractor.compute(grey, key_points)
    # =============================================================================

    # Convert key_points from KeyPoint objects to numpy arrays
    key_points = np.float32([key_point.pt for key_point in key_points])

    return (key_points, features)


def match_key_points(right_key_points, left_key_points,
                     right_features, left_features, ratio, reproj_thresh):
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
    # filtering out matches that occured on the outside edges of the
    # pictures where matche really shouldn't have occured
    # Is equivilent to the following for loop
    #        matches = []
    #        for m in raw_matches:
    #            if len(m) == 2 and m[0].distance < m[1].distance * ratio:
    #                matches.append((m[0].trainIdx, m[0].queryIdx))
    matches = [(m[0].trainIdx, m[0].queryIdx)
               for m in raw_matches
               if len(m) == 2 and m[0].distance < m[1].distance * ratio]

    # Converts the tuples into a numphy array (for working with the
    # homograph), while also splitting up the right and left points
    # We are making a homograph of the matches to apply a ratio test, and
    # determine which of the matches are of a high quility. Typical ratio
    # values are between 0.7 and 0.8
    # Computing a homography requires at least 4 matches
    if len(matches) > 4:
        # Split right and left into numphy arrays
        right_points = np.float32([right_key_points[i]
                                   for (_, i) in matches])
        left_points = np.float32([left_key_points[i]
                                  for (i, _) in matches])

        # Use the cv2 to actually connect the dots between the two pictures
        (H, status) = cv2.findHomography(right_points,
                                         left_points,
                                         cv2.RANSAC,
                                         reproj_thresh)
        return (matches, H, status)
    else:
        return None


def draw_matches(left_image, right_image,
                 left_key_points, right_key_points, matches, status):
    # Shows all the little lines between the two pictures that were matches
    # Plays connect the dots with green lines and the keypoints

    (left_height, left_width) = left_image.shape[:2]
    (right_height, right_width) = right_image.shape[:2]
    vis = np.zeros((max(left_height, right_height),
                    left_width + right_width, 3), dtype="uint8")
    vis[0:left_height, 0:left_width] = left_image
    vis[0:right_height, left_width:] = right_image

    # loop over the matches
    for ((trainIdx, queryIdx), s) in zip(matches, status):
        # Only process the match if the keypoint was successfully matched
        if s == 1:
            # Draw the match
            left_point = (int(left_key_points[queryIdx][0]),
                          int(left_key_points[queryIdx][1]))
            right_point = (int(right_key_points[trainIdx][0]) + left_width,
                           int(right_key_points[trainIdx][1]))
            cv2.line(vis, left_point, right_point, (0, 255, 0), 1)

    # return the visualization
    img_vis = Image(None, None)
    img_vis.image = vis
    return img_vis


def overlay_image(img1, img2):
    """
     Turns the black on rotated images to be transparent when putting onto
     the result image
     http://docs.opencv.org/master/d0/d86/tutorial_py_image_arithmetics.html#gsc.tab=0
    """
    # I want to put logo on top-left corner, So I create a ROI
    rows, cols, channels = img2.shape
    roi = img1[0:rows, 0:cols]

    # Now create a mask of logo and create its inverse mask also
    img2gray = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
    ret, mask = cv2.threshold(img2gray, 10, 255, cv2.THRESH_BINARY)
    mask_inv = cv2.bitwise_not(mask)

    # Now black-out the area of logo in ROI
    img1_bg = cv2.bitwise_and(roi, roi, mask=mask_inv)

    # Take only region of logo from logo image.
    img2_fg = cv2.bitwise_and(img2, img2, mask=mask)

    # Put logo in ROI and modify the main image
    dst = cv2.add(img1_bg, img2_fg)
    img1[0:rows, 0:cols] = dst

    return img1


def fix_distortion(img, strength=0.5, zoom=1):
    """
     Corrects for possible fish-eye lens distortion that may be in the
     image. Slow
     http://www.tannerhelland.com/4743/simple-algorithm-correcting-lens-distortion/
    """
    half_width = img.shape[1] / 2
    half_height = img.shape[0] / 2

    if strength == 0:
        strength = 0.0001

    correction_radius = sqrt(img.shape[1] ** 2 + img.shape[0] ** 2) / strength

    new_image = img.copy()
    for y, Y in enumerate(img):
        for x, X in enumerate(Y):
            new_x = x - half_width
            new_y = y - half_height

            distance = sqrt(new_x * new_x + new_y * new_y)
            r = distance / correction_radius

            if r == 0:
                theta = 1
            else:
                theta = atan(r) / r

            source_x = half_width + theta * new_x * zoom
            source_y = half_height + theta * new_y * zoom

            new_image[y][x] = img[source_y][source_x]

    return new_image


def _crop_black(img):

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 1, 255, cv2.THRESH_BINARY)
    im2, contours, hierarchy = cv2.findContours(thresh,
                                                cv2.RETR_EXTERNAL,
                                                cv2.CHAIN_APPROX_SIMPLE)
    cnt = contours[0]
    x, y, w, h = cv2.boundingRect(cnt)
    crop = img
    crop = img[y:y + h + 1, x:x + w - 1]
    return crop


def fix_curvature(image):
    """
     fix_curvature, order_points, and four_point_transform methods deal
     with having the four corners tranformed to be flat. These methods
     should probably not be used for any automatic stitching programs
    """
    n = eval(input("Enter points: "))
    n = np.array(n)
    return four_point_transform(image, n)


def order_points(pts):
    """
     fix_curvature, order_points, and four_point_transform methods deal
     with having the four corners tranformed to be flat. These methods
     should probably not be used for any automatic stitching programs
    """
    # initialzie a list of coordinates that will be ordered
    # such that the first entry in the list is the top-left,
    # the second entry is the top-right, the third is the
    # bottom-right, and the fourth is the bottom-left
    rect = np.zeros((4, 2), dtype="float32")

    # the top-left point will have the smallest sum, whereas
    # the bottom-right point will have the largest sum
    s = pts.sum(axis=1)
    rect[0] = pts[np.argmin(s)]
    rect[2] = pts[np.argmax(s)]

    # now, compute the difference between the points, the
    # top-right point will have the smallest difference,
    # whereas the bottom-left will have the largest difference
    diff = np.diff(pts, axis=1)
    rect[1] = pts[np.argmin(diff)]
    rect[3] = pts[np.argmax(diff)]

    # return the ordered coordinates
    return rect


def four_point_transform(image, pts):
    """
     fix_curvature, order_points, and four_point_transform methods deal
     with having the four corners tranformed to be flat. These methods
     should probably not be used for any automatic stitching programs
    """
    # obtain a consistent order of the points and unpack them
    # individually
    rect = order_points(pts)
    (tl, tr, br, bl) = rect

    # compute the width of the new image, which will be the
    # maximum distance between bottom-right and bottom-left
    # x-coordiates or the top-right and top-left x-coordinates
    widthA = br[0] - bl[0]
    widthB = tr[0] - tl[0]
    maxWidth = max(abs(int(widthA)), abs(int(widthB)))

    # compute the height of the new image, which will be the
    # maximum distance between the top-right and bottom-right
    # y-coordinates or the top-left and bottom-left y-coordinates
    heightA = br[1] - tr[1]
    heightB = bl[1] - tl[1]
    maxHeight = max(abs(int(heightA)), abs(int(heightB)))

    # now that we have the dimensions of the new image, construct
    # the set of destination points to obtain a "birds eye view",
    # (i.e. top-down view) of the image, again specifying points
    # in the top-left, top-right, bottom-right, and bottom-left
    # order
    dst = np.array([[0, 0],
                    [maxWidth - 1, 0],
                    [maxWidth - 1, maxHeight - 1],
                    [0, maxHeight - 1]], dtype="float32")

    # compute the perspective transform matrix and then apply it
    M = cv2.getPerspectiveTransform(rect, dst)

    # return the warped image
    return cv2.warpPerspective(image, M, (image.shape[1], image.shape[0]))

def read_images(image_rawdata):

    return image_rawdata[0], np.array(io.imread(StringIO(image_rawdata[1]), as_grey=True))

def stitch_two(left_image, right_image, affine=False, ratio=0.75, reprojThresh=4.0, show_matches=False):

    # Use our method to find the key points and features of the two images
    # We can then use things in cv2 to match these special points together
    (left_key_points, left_features) = find_keypoints_and_features(left_image.image)
    (right_key_points, right_features) = find_keypoints_and_features(right_image.image)

    # Match features between the two images
    M = match_key_points(right_key_points, left_key_points, right_features, left_features, ratio, reprojThresh)

    # If the match is None, then there aren't enough matched keypoints to create a panorama
    if M is None:
        return right_image, left_image, np.array([[1, 0, 0],
                                                  [0, 1, 0],
                                                  [0, 0, 1]])

    # This is the part that angles the left image to work with the right
    # image, according to the homography matrix that match_key_points
    # returned
    (matches, H, status) = M

    # Makes result equal to the warped right photo, with empty space
    # infront (to store the unmatched part of the left photo), and a total
    # width equal to the two photos widths added together
    # This is all done in our own warpPerspective, to better suit our needs
    # for up and down layered images.
    # This function works by putting one image on top of the other
    result, left_image, Ht = warp_perspective(left_image, right_image, H, affine=affine)

    # Checks if the person wants to see the little green lines of where the
    # match points were
    if show_matches:
        vis = draw_matches(right_image.image, left_image.image, right_key_points, left_key_points, matches, status)
        # Return a tuple of the stitched image and the visualization
        return (result, left_image, Ht, vis)

    else:
        # Return the stitched image
        return (result, left_image, Ht)


def stitch_multiple(images, ratio=0.75, reprojThresh=4.0, affine=False):

    images_key = images[0]
    images_values = images[1]

    result = images_values[0]

    """
        for i in range(0, len(images_values)):

            img = fix_distortion(images_values[i], strength=0.41)
            try:
                stitched = stitch_two(img, result, affine=affine, ratio=ratio, reprojThresh=reprojThresh)
            except Exception as e:
                continue

            (result, img, H) = stitched

            result = _crop_black(result)

            (a, b) = (apply_homograph_to_points(images[:i], H), images[i:])
            images = a + b

            img = None
    """

    output_image_path = os.path.abspath(os.path.join(OUTPUT_FILE_PATH, "Stitched_IMG_" + images_key + OUTPUT_FILE_TYPE))
    io.imsave(output_image_path, result)

    #return result

def my_stitch_multiple(images, ratio=0.75, reprojThresh=4.0, affine=False):

    firstLoop = True
    first_image = Image(None, None)
    second_image = Image(None, None)
    temp_image = Image(None, None)

    for i in range(len(images)):

        if firstLoop:
            first_image = images[i]
            second_image = images[i + 1]
            firstLoop = False

        else:
            first_image = temp_image
            second_image = images[i]

        try:
            temp_image, left_image, homograph = stitch_two(first_image, second_image, affine=affine, ratio=ratio, reprojThresh=reprojThresh)
            temp_image.image = fix_distortion(temp_image.image, strength=0.41)
            temp_image.image = _crop_black(temp_image.image)
            print("Progress: " + str(i) + "/" + str(len(images) - 1))
        except:
            print("Error: " + str(i) + "/" + str(len(images) - 1) + ", with image " + str(images[i]))
    output_image_name = "stitched_img_.png"
    temp_image.save(path="/home/hduser/dev-materials/", filename=output_image_name)
    return temp_image


"""
if __name__ == "__main__":

    imagesFolderPath = "/home/hduser/dev-materials/"
    imagesCounter = len(glob.glob1(imagesFolderPath,"*.tif"))

    result = Image(None, None)
    images = []

    for i in range(0, imagesCounter):
        # images = cv2.imread("/home/travis/Desktop/000/IMG_%04d_2.tif" % i)
              # images.append(Image("/home/travis/Desktop/000/", "IMG_%04d_2.tif" % i))
        #images.append(Image("/home/hduser/dev-materials/", "IMG_%04d_1.png" % i))
        images.append(Image("/home/hduser/dev-materials/", "IMG_%04d_1.tif" % (i + 50)))

    #result = stitch_two(images[0], images[1])
    #fileName = "stitched_two.png"
    #result[0].save(path="/home/hduser/dev-materials/", filename=fileName)

    result, images = stitch_multiple(images)

    output_image_name = "stitched_img_.png"
    result.save(path="/home/hduser/dev-materials/", filename=output_image_name)
"""

reading_start_time = time()
images_rdd = sc.binaryFiles('hdfs://discus-p2irc-master:54310/user/hduser/registration_images_tif', 100)

images_bytes = images_rdd.map(read_images) \
    .map(lambda rawdata: (rawdata[0][78:79], [rawdata[1]])) \
    .reduceByKey(lambda first_image, second_image: first_image + second_image)

images_bytes.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
reading_end_time = time() - reading_start_time

processing_start_time = time()
images_bytes.foreach(stitch_multiple)
processing_end_time = time() - processing_start_time


"""
    .groupByKey() \
    .mapValues(list)

# This is just for debugging purposes
first_rdd_group_key = images_bytes.first()[0]
first_rdd_group_value = images_bytes.first()[1]

print images_bytes.count()
print np.array(first_rdd_group_value).shape
print len(first_rdd_group_value)
print first_rdd_group_key
"""

print '========================================================================='
print "SUCCESS: Images read from HDFS in {} seconds".format(round(reading_end_time, 3))
print "SUCCESS: Images processed and written to HDFS in {} seconds".format(round(processing_end_time, 3))
print '========================================================================='
