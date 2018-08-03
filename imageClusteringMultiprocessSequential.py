# Original Author: Amit Kumar Mondal
# Modified: Habib Sabiu
# Date: October 14, 2017
#
# Description: A multiprocess python application to cluster still camera images. The script read images data from a local directory and writes 
#              its output to a local directory. The output is a text file containing image name and the cluster to which it belongs.
#
# Example usage: python imageClusteringMultiprocessSequential.py -i /still_images_small/ -o /output-dir/


import re
import os
import io
import sys
import cv2
import types
import base64
import argparse
import warnings
import numpy as np
import multiprocessing as mp

from skimage import *
from time import time
from io import BytesIO
from skimage import color
from sklearn.cluster import KMeans
from skimage.feature import blob_doh
from sklearn.metrics import pairwise
from scipy.misc import imread, imsave



# Filter warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

class ImgCluster:

    def loadImages(self, img_lists):
        imgs_with_name = []
        for name in img_lists:
            img = imread(BytesIO(open(name, 'rb').read()))
            imgs_with_name.append((name, img))
        return imgs_with_name

    def estimate_feature(self, img):
        extractor = cv2.ORB_create()
        return extractor.detectAndCompute(img_as_ubyte(img), None)

    def convert(self, img):

        images = color.rgb2gray(img)
        return images

    def commonTransform(self, datapack):
        def common_transform(datapack):
            fname, images = datapack
            procsd_obj = ''
            try:
                procsd_obj = self.convert(images)
            except Exception as e:
                pass
            return (fname, images, procsd_obj)
        return common_transform(datapack)

    def commonEstimate(self, datapack):
        def common_estimate(datapack):
            fname, img, procsd_obj = datapack
            try:
                kp, descriptors = self.estimate_feature(procsd_obj)
            except Exception as e:
                descriptors = None
                pass
            return (fname, img, descriptors)
        return common_estimate(datapack)

    def commonModel(self, params):
        features, K, maxIterations = params
        vec = []
        for f_object in features:
            vec.append(f_object[0].tolist())
        converted_vec = np.array(vec)
        model = KMeans(init='k-means++', n_clusters=K, max_iter = maxIterations).fit(converted_vec)
        return model

    def commonAnalysisTransform(self, datapack, params):
        def common_transform(datapack):
            fname, img, procsdentity = datapack
            model,condition = params
            feature_matrix = np.array(procsdentity[0].tolist()).reshape(1, -1)
            clusterCenters = model.cluster_centers_

            measure = np.zeros(len(clusterCenters))

            k = model.predict(feature_matrix)

            return (fname, k[0] + 1)

        return common_transform(datapack)
            

    def img_clustering(self, input_imgs_list):
        K= 3
        itrns = 5
        imgs_with_name = self.loadImages(input_imgs_list)
        packs = []
        for bundle in imgs_with_name:
            packs.append(self.commonTransform(bundle))
        est_packs = []
        ii = 0
        features = []
        for pack in packs:
            if(len(pack[2].shape) !=0 ):
                fname, img, descriptor = self.commonEstimate(pack)
                features.append(descriptor)
                est_packs.append( (fname, img, descriptor))
            ii = ii+1

        model = self.commonModel((features,K,itrns))
        result = []
        for pack in est_packs:
            result.append(self.commonAnalysisTransform(pack, (model, 'sum')))

        return result



def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def mp_handler(image_filenames, n_per_proc):
    imgCluster = ImgCluster()
    pool = mp.Pool()
    result = pool.map(imgCluster.img_clustering, chunks(image_filenames, n_per_proc))
    return result
    

def get_images_paths(images_path):
    imageFiles = []
    for name in os.listdir(images_path):
        if not name.endswith(".jpg"):
            continue
        tmpFilePath = os.path.join(images_path, name)
        if os.path.isfile(tmpFilePath):
            imageFiles.append(tmpFilePath)
    return imageFiles


def saveClusterResult(result, output_dir):

    f = open(output_dir+"image_clustering_result.txt","w") 

    for lstr in result:

        img_name = lstr[0]
        group = lstr[1]

        f.write(img_name + "\t\t" + str(group) + "\n")

    f.close()


if __name__ == "__main__":

    application_start_time = time()


    # The location where the script is executed is the default output path
    DEFAULT_OUTPATH = os.getcwd() + "/"
    
    # Create command line arguements to parse
    ap = argparse.ArgumentParser(description="A python script to cluster still camera images")
    ap.add_argument("-i", "--input_file", required=True, help="Input directory containing the images to cluster")
    ap.add_argument("-o", "--output_path", required=False, help="Output directory where to write the output file", default=DEFAULT_OUTPATH)

    # Parse arguments 
    args = vars(ap.parse_args())

    # Get the input file path from command line arguments
    input_dir = args["input_file"]

    if not input_dir.endswith("/"):
        input_dir = input_dir + "/"

    output_dir = ""

    # Get the output path from command line arguments
    if(args["output_path"] is DEFAULT_OUTPATH):
        output_dir = DEFAULT_OUTPATH
    else:
        output_dir = args["output_path"]
        if not output_dir.endswith("/"):
            output_dir = output_dir + "/"

    input_files = get_images_paths(input_dir)

    clustering_result = mp_handler(input_files, 3)
    flat_clustering_result = [item for sublist in clustering_result for item in sublist]   
    flat_clustering_result = sorted(flat_clustering_result, key=lambda tup: tup[1], reverse=True)

    saveClusterResult(flat_clustering_result, output_dir)


    application_end_time = time() - application_start_time

    print("----------------------------------------------")
    print("SUCCESS: Images procesed in {} seconds".format(round(application_end_time, 3)))
    print("----------------------------------------------")


