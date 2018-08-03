# Original Author: Amit Kumar Mondal
# Modified: Habib Sabiu
# Date: October 14, 2017
#
# Description: A python application to cluster still camera images. The script read images data from a local directory and writes 
#              its output to a local directory. The output is a text file containing image name and the cluster to which it belongs.
#
# Example usage: python imageClusteringSequential.py -i /still_images_small/ -o /output-dir/



import os
import sys
import cv2
import argparse
import warnings
import numpy as np
import skimage.io as io

from skimage import *
from time import time
from io import BytesIO
from PIL import ImageFile
from scipy.spatial import distance
from sklearn.cluster import KMeans
from sklearn.cluster import MiniBatchKMeans
from scipy.misc import imread, imsave


# Filter warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning) 
warnings.filterwarnings("ignore", category=DeprecationWarning)

def get_images_paths(images_path):
    imageFiles = []
    for name in os.listdir(images_path):
        if not name.endswith(".jpg"):
            continue
        tmpFilePath = os.path.join(images_path, name)
        if os.path.isfile(tmpFilePath):
            imageFiles.append(tmpFilePath)
    return imageFiles

    
def images_to_descriptors(fname):
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    try:
        img_name = fname
        img_data = imread(BytesIO(open(img_name, 'rb').read()))

        extractor = cv2.xfeatures2d.SIFT_create()
        kp, descriptors = extractor.detectAndCompute(img_as_ubyte(img_data), None)
        return [img_name, descriptors]

    except (ValueError, IOError, SyntaxError) as e:
        pass


def assign_pooling(data):

    image_name, feature_matrix = data[0]
    model = data[1]

    clusterCenters = model.cluster_centers_

    feature_matrix = np.array(feature_matrix)

    #model = KMeansModel(clusterCenters)
    bow = np.zeros(len(clusterCenters))

    for x in feature_matrix:
        k = model.predict(x.reshape(1, -1))
        dist = distance.euclidean(clusterCenters[k], x)
        bow[k] = max(bow[k], dist)

    clusters = bow.tolist()
    group = clusters.index(min(clusters)) + 1
    return [image_name, group]


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

    images_with_file_names = list(map(images_to_descriptors, input_files))
    filtered_images_with_file_names = list(filter((lambda x: x[1].all() != None), images_with_file_names))
    filtered_images_with_file_names = list(map((lambda x: (x[0], x[1])), filtered_images_with_file_names))

    features = []

    for i in filtered_images_with_file_names:
        features.append(np.array(i[1]))

    features = np.concatenate(np.array(features), axis=0)

    model = MiniBatchKMeans(n_clusters=3, max_iter=5).fit(features)

    data_to_cluster = list(map((lambda x: [x, model]), filtered_images_with_file_names))

    features_bow = list(map(assign_pooling, data_to_cluster))

    saveClusterResult(features_bow, output_dir)


    application_end_time = time() - application_start_time

    print("----------------------------------------------")
    print("SUCCESS: Images procesed in {} seconds".format(round(application_end_time, 3)))
    print("----------------------------------------------")

