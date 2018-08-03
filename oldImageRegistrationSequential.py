# -*- coding: utf-8 -*-
"""
    Created on Mon Jun 13 08:17:48 2016

    Takes a folder of images and registers them together.

    Arguements Taken:

    -i --impath     :The location of the images to register
    -o --outpath    :(OPTIONAL) The output location of the files
    -r --rgb        :(OPTIONAL) When set, output The R, G and B channels as a
    single RGB image
    -c --crop       :(OPTIONAL) When set, crop the RGB output image and save it
    -p --profile    :(OPTIONAL) When set, Output various profiling metrics

    @author: keenan
    """

import argparse
import os.path
import skimage.io as io
import numpy as np
import micasense_register as reg
import warnings
import math
import time
from matplotlib import pyplot as plt
#import p2irc
#import p2irc.data_server.db as db
#from p2irc.data_server.insert_new_images import insert_new_rows_with_image_file_path_column as img_insert

# Global vars
DEFAULT_OUTPATH = "Registered_Images/"
OUTPUT_FILE_TYPE = ".png"
DATABASE_TABLE = "image_registered"
DATABASE_REG_FOLDER = "intermediate_data/registered_images/"

# there is a user warning that pops up each time the image is saved.
warnings.filterwarnings("ignore", category=UserWarning)


def register_channels(impath, outpath, make_rgb=False,
                      crop=False, profile=False, database=False):
    if impath is None:
        raise TypeError("Invalid image path given in register_channels")

    # Parse the image path and output path
    if(outpath is DEFAULT_OUTPATH):
        outpath = impath + outpath
    else:
        outpath = outpath

    if crop is True:
        croppath = impath + "crop/"
    else:
        croppath = outpath + "crop/"

    if make_rgb is True:
        rgbpath = impath + "RGB/"
    else:
        rgbpath = outpath + "RGB/"

    # Count the number of files and create the string version of the total for
    # printing purposes
    num_files = len([f for f in os.listdir(impath) if os.path.isfile(os.path.join(impath, f))])
    print("Number of files: " + str(num_files))
    str_num_files = "%04d" % ((num_files / 5) - 1)

    # Create an array to store timing information
    indiv_time = []

    # Create a list_of_dicts for the use of Will's mega upload function
    # insert_new_images.insert_new_rows_with_image_file_path_column()
    list_of_dicts = []

    # Loop through the image numbers and send them off to registration method
    for i in range(0, int(num_files / 5)):
        str_image_num = "%04d" % i
        print("Image#\t" + str_image_num + "/" + str_num_files)
        image_names = ['IMG_' + str_image_num + '_' + str(n) + '.tif' for n in range(1, 6)]

        start_time = time.time()
        # Get the images and store them in an array, then calculate their
        # homographies and transform the images.
        # H, Back-proj-error and the inlier points are all calculated
        C = [np.array(io.imread((impath + name), as_grey=True) / 65535) for name in image_names]
        H, BPError, Inliers = reg.register_channels(C)

        # Add a 0 to the start of the list of back projection errors, since the
        # first image always has a BPError of 0 (This is for later where we
        # need to print the BPErrors)
        BPError.insert(0, 0)
        T = reg.transform_channels(C, H)

        # Decompose the homogrpahy and calculate the bounding box of the
        # good data, where all 5 channels are present
        max_x = []
        max_y = []
        max_theta = []
        for j in H:
            max_x.append(abs(reg.decompose_homography(j)[0][0]))
            max_y.append(abs(reg.decompose_homography(j)[0][1]))
            max_theta.append(abs(reg.decompose_homography(j)[1]))

        rot = math.ceil(math.sin(max(max_theta)) * C[0].shape[1])
        crop_x = math.ceil(max(max_x))
        crop_y = math.ceil(max(max_y))

        border_x = (crop_x + rot, C[0].shape[1] - crop_x - rot)
        border_y = (crop_y + rot, C[0].shape[0] - crop_y - rot)

        bounding_box = ((border_x[0], border_x[1]), (border_y[0], border_y[1]))

        # Check that the image paths we are going to write to exist
        if not os.path.exists(outpath):
            os.makedirs(outpath)
        if make_rgb is True and not os.path.exists(rgbpath):
            os.makedirs(rgbpath)
        if crop is True and not os.path.exists(croppath):
            os.makedirs(croppath)

        # Loop through each subset of images and re-save them now that they are
        # registered
        out_image_names = []
        for j in range(len(T)):
            io.imsave(outpath + "/IMG_" + str_image_num + "_" + str(j + 1) + OUTPUT_FILE_TYPE, T[j])
            out_image_names.append(outpath + "/IMG_" + str_image_num + "_" + str(j + 1) + OUTPUT_FILE_TYPE)

        #================================Added this block to calculate NDVI==================================
        # Create and save the RGB image if told too in CL
        if make_rgb is True:
            rgb = np.dstack([T[2], T[1], T[0]])
            io.imsave(rgbpath + "IMG_" + str_image_num + "_RGB" + OUTPUT_FILE_TYPE, rgb)

            #create and save the NDVI image
            num = np.subtract(T[3], T[2])
            dnm = np.add(T[3], T[2])
    
            ndvi_img = np.divide(num, dnm)
    
            original_ndvi = ndvi_img
                
            io.imsave(rgbpath + "IMG_" + str_image_num + "_NDVI" + OUTPUT_FILE_TYPE, original_ndvi)
        #===================================================================================================

        # If Cropped images are needed
        if crop is True:
            crop_img = np.dstack([T[2], T[1], T[0]])
            crop_img = crop_img[border_y[0]:border_y[1], border_x[0]:border_x[1]]
            io.imsave(croppath + "IMG_" + str_image_num + "_CROP" + OUTPUT_FILE_TYPE, crop_img)

        # If profiling is needed
        if profile is True:
            indiv_time.append(time.time() - start_time)
            print("Image proc. time: " + str(indiv_time[i]))

        # Append to the list_of_dictionaries that will be used to fill in
        # The database
        if database is True:
            for j in range(len(out_image_names)):
                list_of_dicts.append({"bounding_box_pixels": bounding_box, "file_type": OUTPUT_FILE_TYPE, "squared_sum_error": BPError[j],"git_commit_hash": db.get_current_git_commit_hash(), "file_path": out_image_names[j]})

    """
    # Print a histogram of the timing data
    if profile is True:
        (hist, _) = np.histogram(indiv_time, bins=np.arange(0, len(indiv_time)), range=(0, len(indiv_time)))
        indiv_time = np.array(indiv_time)
        plt.hist(indiv_time.astype(int))
        plt.title("Time Histogram")
        plt.xlabel("Value")
        plt.ylabel("Frequency")
        plt.show()
    """

    return list_of_dicts

if __name__ == "__main__":
    # Create arguements to parse
    ap = argparse.ArgumentParser(description="Take a folder of muli-spectral images and register them and create a single rgb image from the R, G and B channels.")
    ap.add_argument("-i", "--impath", required=True, help="Path to the image folder.")
    ap.add_argument("-o", "--outpath", required=False, help="Path to the desired output folder.", default=DEFAULT_OUTPATH)
    ap.add_argument("-r", "--rgb", required=False, help="Create and save RGB images from the R, G and B channels.", action="store_true", default="false")
    ap.add_argument("-c", "--crop", required=False, help="Create and save CROPPPED RGB images from the R, G and B channels.", action="store_true", default="false")
    ap.add_argument("-p", "--profile", required=False, help="Log important Information such as time to complete.", action="store_true", default="false")
    ap.add_argument("-d", "--database", required=False, help="Send the registered images to the crepe database", action="store_true", default="false")
    args = vars(ap.parse_args())

    # Parse the image path and output path
    if (args["outpath"] is DEFAULT_OUTPATH):
        outpath = args["impath"] + args["outpath"]
    else:
        outpath = args["outpath"]

    try:
        if args["database"] is True:
            abs_location = os.path.abspath(os.path.join(__file__, os.pardir))
            abs_location = (abs_location.split("discus-p2irc", 1)[0] + "discus-p2irc/")
            user = input("Database username: ")
            pswd = p2irc.data_server.server.get_password_from_prompt()
            p2irc.config.configure(abs_location, True)
            list_of_dicts = register_channels(args["impath"], outpath=outpath, make_rgb=args["rgb"], crop=args["crop"], profile=args["profile"], database=args["database"])
            # TODO: Use the list of dicts and send that to Will's mega upload
            #       Fucntion thingy
            #       (insert_new_images.insert_new_rows_with_image_file_path_column())
            # print(list_of_dicts)
            img_insert(list_of_dicts, "file_path", DATABASE_TABLE, outpath, DATABASE_REG_FOLDER, db_username=user, db_password=pswd)
        else:
            application_start_time = time.time()
            register_channels(args["impath"], outpath=args["outpath"], make_rgb=args["rgb"], crop=args["crop"], profile=args["profile"], database=args["database"])
            application_end_time = time.time() - application_start_time
            print ("=========================================================================")
            print ("SUCCESS: All images processed in {} seconds".format(round(application_end_time, 3)))
            print ("=========================================================================")
    finally:
        if (args["database"] is True):
            print("===========================")
            print("Database connection closed!")
            print("===========================")
