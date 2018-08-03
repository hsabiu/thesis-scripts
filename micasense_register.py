"""Credit to Adrian Rosebrock at
http://www.pyimagesearch.com/2016/01/11/opencv-panorama-stitching/
A lot of the code from that website has been used in functions in this program,
and some credit goes to the original author

Credit to arcticfox at

http://stackoverflow.com/questions/13063201/how-to-show-the-whole-image-when-using-opencv-warpperspective/20355545#20355545
For the initial steps of the Stitcher().warpPerpection method, which allows for
photos not to be cropped off when rotated off the size of the original image.

Written by Mark Eramian and Travis G.
"""

import cv2
import numpy as np
from skimage import img_as_ubyte
import math


def find_keypoints_and_features(image):
    """
     It takes in an image, then uses some cv2 functions to find and return
     the key points and features of the image

     @type  image: ndarray
     @param image: The image that will be searched for key points and
                   features

     @rtype:  tuple
     @return: a tuple with two elements. The first elements is a numpy
              array holding the key points, and the second element is a
              numpy array with the features

     Function written by Travis G.
    """
    # Check that image is not invalid
    if image is None:
        raise TypeError("Invalid image in find_keypoints_and_features")

    # Uses a function in cv2 that finds all important features in an
    # image. I have used something called the SIFT algorithm to do this,
    # instead of one known as SURF
    #    The differences are: Surf has been measured to be about three
    #    times fast than sift. Also, the tradeoff performance whise is that
    #    surf is better than sift at handling blurred images (which may be
    #    particularily for zooming drones and wind whipping the crop), and
    #    rotation, while it's not as good at handling viewpoint change and
    #    illumination change, which wouldn't be as important as blurryness
    #    when it comes to drones. Even with all this, we still need SIFT
    #    due to the ability to only keep the best features
    #
    # Another issue, with both sift and surf, is memory. any feature found
    # take a minimum of 256 bytes, and there are a lot of features
    #
    # The last issue is that both SURF and SIFT are patented, which means
    # that we would have to pay to use them if we used them for some
    # specific stuff
    # An alternative is ORB which is created by OpenCV for this reason.
    # However, it is different and I would need to look into it more.
    # However, feature matching with surf/sift and the brute-force matcher
    # is really versatile and strong, which is the main reason why I want
    # to keep using what we have
    #     My only thought as to why we need this is that there isn't a lot
    #     of variation when looking at a field of plants, and we need to be
    #     able to detect properly
    descriptor = cv2.xfeatures2d.SIFT_create(nfeatures=100000)

    # if fails means can't find similarities between two images
    (key_points, features) = descriptor.detectAndCompute(image, None)

    # =========================================================================
    #         IF YOU HAVE CV2 VERSION 2 USE THIS STUFF,
    #         INSTEAD OF THOSE TWO LINES
    #         #turn the image into greyscale to work with
    #         grey = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    #         detector = cv2.FeatureDetector_create("SURF")
    #         key_points = detector.detect(grey)
    #         extractor = cv2.DescriptorExtractor_create("SURF")
    #         (key_points, features) = extractor.compute(grey, key_points)
    # =========================================================================

    # Convert key_points from KeyPoint objects to numpy arrays
    key_points = np.float32([key_point.pt for key_point in key_points])
    return (key_points, features)


def match_key_points(right_key_points, left_key_points,
                     right_features, left_features, ratio, reproj_thresh):
    """
     Function written by Travis G.
    """
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
        (H, status) = cv2.findHomography(src_pts,
                                         dst_pts,
                                         cv2.RANSAC,
                                         reproj_thresh)
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

#            print((x_p-num1/dnm)**2)
            tmp = (x_p - (num1 / dnm))**2 + (y_p - (num2 / dnm))**2
            if status[i] == 1:
                back_proj_error += tmp
                inlier_count += 1

        return (matches, H, status, back_proj_error, inlier_count)
    else:
        return None


def register_channels(C, idx=0, ratio=.75, reproj_thresh=4):
    """
    @param             C: channels to register
    @type              C: list/tuple of ndarrays with depth 1

    @param           idx: the target channel to which the other channels should
                          be registered
    @type            idx: integer in range(len(c))
    @def             idx: 0

    @param         ratio: ratio for ratio test for determining which keypoint
                          matches are of sufficient quality. Typical values are
                          between 0.7 and 0.8.
    @type          ratio: float
    @def           ratio: 0.75

    @param reproj_thresh: Reprojection threshold for RANSAC-based rejection of
                          outliers when finding homography. This is the maximum
                          allowed reprojection error to treat a point as an
                          inlier. Typical values are between 1 and 4. For
                          further info check
                          http://docs.opencv.org/3.0-beta/modules/calib3d/doc/camera_calibration_and_3d_reconstruction.html?highlight=findhomography#cv2.findHomography

    @type  reproj_thresh:
    @def   reproj_thresh: 4

    @return       : Transformations to align the image channels.  This is a
                    list of 3x3 homography matrices, one for each image.
                    The target channel has the identity matrix.

    Function written by Mark Eramian with Travis G.
    """
    # Check that the images in C are good images and not empty
    if C is None:
        raise TypeError("Invalid image set in register_channels")
    for i in C:
        if len(i.shape) > 2:
            raise TypeError("Images have greater depth than 1!")

    # Compute SIFT features for each channel.
    # Channel images are converted to unsigned byte.  All proper scaling
    # is done by image_as_ubyte regardless of dtype of the input images.
    keypoints_and_features = [
        find_keypoints_and_features(
            img_as_ubyte(chan)) for chan in C]

    # Generate list of indices excluding the target channel index.
    channels_to_register = list(range(len(C)))
    del channels_to_register[idx]

    # Generate keypoint matches between each channel to be registered
    # and the target image.
    matched_key_points = [match_key_points(keypoints_and_features[i][0],
                                           keypoints_and_features[idx][0],
                                           keypoints_and_features[i][1],
                                           keypoints_and_features[idx][1],
                                           ratio=ratio,
                                           reproj_thresh=reproj_thresh)
                          for i in channels_to_register]

    # extract the homography matrices from 'matched_key_points'.
    H = [x[1] for x in matched_key_points]
    BPError = [x[3] for x in matched_key_points]
    Inliers = [x[4] for x in matched_key_points]
    # Add the identity matrix for the target channel.
    H.insert(idx, np.identity(3))
    return H, BPError, Inliers


def warp_image(I, H):
    """
    @param I:  image to transform
    @type  I:  ndarray
    @param H:  homography matrix to be applied to I
    @type  H:  3 x 3 ndarray
    @return :  transformed image resulting from applying H to I.

    Function written by Mark Eramian.
    """
    return cv2.warpPerspective(I, H, (I.shape[1], I.shape[0]))


def transform_channels(C, H):
    """
    @param C: image channels to transform
    @type  C: list of ndarray
    @param H: H[i] is the homography matrix for C[i]
    @type  I: list of ndarray

    @return : list of transformed images, where the i-th image
              is the result of applying H[i] to C[i].

    Function written by Mark Eramian.
    """
    return [warp_image(C[i], H[i]) for i in range(len(C))]


def decompose_homography(H):
    """
    @param H: homography
    @type  H: 3x3 array

    @return : tuple ((translationx, translationy), rotation,
                     (scalex, scaley), shear)

    Function writeen by Keenan Johnstone
    """
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

