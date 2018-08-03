# Date: May 28, 2018
# Author: Habib Sabiu
# Purpose: Script to submit multiple jobs to Spark cluster, and log the start 
#          and end time of the experiment as well as jobs submission time.

import os
import sys
import time
import datetime
import subprocess
import numpy as np

from glob import glob


def split_list(a_list):
    """Split a python list into two paths"""
    half = len(a_list)//2
    return a_list[:half], a_list[half:]


if __name__ == "__main__":

    hdfs_url = "hdfs://discus-p2irc-master:54310/"
    spark_master_url = "spark://discus-p2irc-master:7077"
    #spark_master_url = "mesos://discus-p2irc-master:5050"
    #spark_master_url = "yarn"

    jobs_list = []
    delay = 1
    np.random.seed(100)

    # Normal HDFS directory Vs HAR directory experiment
    if sys.argv[1] == '1':

        experiment_type = "normal"
        #experiment_type = "har"

        if experiment_type == "normal":
            experiment_name = 'Normal HDFS Directory (with large number of small files per job)'
            input_prefix = "hdfs://discus-p2irc-master:54310/"
            drone_images_base_dir_path = "/data/mounted_hdfs_path/user/hduser/habib/inputs/normal_dirs/drone_camera/"
            still_camera_images_base_dir_path = "/data/mounted_hdfs_path/user/hduser/habib/inputs/normal_dirs/still_camera/"
        elif experiment_type == "har":
            experiment_name = 'Hadoop Archive Files (one large HDFS file per job)'
            input_prefix = "har:///"
            drone_images_base_dir_path = "/data/mounted_hdfs_path/user/hduser/habib/inputs/har_dirs/drone_camera/"
            still_camera_images_base_dir_path = "/data/mounted_hdfs_path/user/hduser/habib/inputs/har_dirs/still_camera/"

        image_registration_dirs = glob(os.path.join(drone_images_base_dir_path, "*", ""))
        still_camera_images = glob(os.path.join(still_camera_images_base_dir_path, "*", ""))

        flower_counter_dirs, image_clustering_dirs = split_list(still_camera_images)

        for single_dir in flower_counter_dirs:
            input_dir_path = input_prefix + "/".join(single_dir.split("/")[3:])
            job_name = "flower_counter_" + (input_dir_path.split("/")[-2].replace("-", "_")).replace(".", "_")
            output_dir_path = hdfs_url + "user/hduser/habib/outputs/18_jobs_output/" + job_name + "_output"

            job_string = "spark-submit --master " + spark_master_url + " --py-files canola_timelapse_image.py imageFlowerCounter.py " + input_dir_path + " " + output_dir_path + " " + job_name
            jobs_list.append(job_string)

        for single_dir in image_clustering_dirs:
            input_dir_path = input_prefix + "/".join(single_dir.split("/")[3:])
            job_name = "image_clustering_" + (input_dir_path.split("/")[-2].replace("-", "_")).replace(".", "_")
            output_dir_path = hdfs_url + "user/hduser/habib/outputs/18_jobs_output/" + job_name + "_output"

            job_string = "spark-submit --master " + spark_master_url + " imageClustering.py " + input_dir_path + " " + output_dir_path + " " + job_name
            jobs_list.append(job_string)

        for single_dir in image_registration_dirs:
            input_dir_path = input_prefix + "/".join(single_dir.split("/")[3:])
            job_name = "image_registration_"+input_dir_path.split("/")[-2].replace(".", "_")
            output_dir_path = "/data/mounted_hdfs_path/user/hduser/habib/outputs/18_jobs_output/" + job_name + "_output"

            if not os.path.exists(output_dir_path):
                os.makedirs(output_dir_path)
            if not os.path.exists(output_dir_path + "/processed"):
                os.makedirs(output_dir_path + "/processed")

            job_string = "spark-submit --master " + spark_master_url + " imageRegistration.py " + input_dir_path + " " + output_dir_path + " " + job_name
            jobs_list.append(job_string)

    
    # Multiple flowerCounter jobs experiment
    elif sys.argv[1] == '2':

        experiment_name = 'Multiple flowerCounter jobs (Standalone/Dynamic/1core/2GB/128MB RDD/NoSpec/2 min)'
        input_prefix = "hdfs://discus-p2irc-master:54310/"
        still_camera_images_base_dir_path = "/data/mounted_hdfs_path/user/hduser/cameradays/"
        flower_counter_dirs = glob(os.path.join(still_camera_images_base_dir_path, "1108-0*", ""))
        flower_counter_dirs = [x for x in flower_counter_dirs if x.split("-")[-1] not in ["0630/", "0701/", "0702/", "0703/", "0807/"]]

        for single_dir in flower_counter_dirs:
            input_dir_path = input_prefix + "/".join(single_dir.split("/")[3:])
            job_name = "flower_counter_" + (input_dir_path.split("/")[-2].replace("-", "_")).replace(".", "_")
            output_dir_path = hdfs_url + "user/hduser/habib/outputs/flower_counter/" + job_name + "_output"

            job_string = "spark-submit --master " + spark_master_url + " --py-files canola_timelapse_image.py imageFlowerCounter.py " + input_dir_path + " " + output_dir_path + " " + job_name

            jobs_list.append(job_string)


    print(len(jobs_list), jobs_list[0])

    random_choice = np.random.choice(jobs_list, len(jobs_list), replace=False)

    epoch_start_time = int(time.time())
    local_start_time = datetime.datetime.fromtimestamp(epoch_start_time).strftime('%c')

    logs_list = []
    processes = []

    logs_list.append("--------------------------------------------------------------------------------------------")
    logs_list.append("Experiment name: " + experiment_name)
    logs_list.append("Experiment start time: " + str(epoch_start_time) + " => " + local_start_time)
    logs_list.append("--------------------------------------------------------------------------------------------")

    for job in random_choice:
        job = job.split(" ")
        p = subprocess.Popen(job)
        processes.append((p, job[-1]))

        epoch_current_time = int(time.time())
        local_current_time = datetime.datetime.fromtimestamp(epoch_current_time).strftime('%c')

        line = '{} {:38s} {} {} {} {}'.format("Submitted", job[-1], "on", str(epoch_current_time), "=>", local_current_time)
        logs_list.append(line)

        time.sleep(delay)

    exit_codes = [(p[1], p[0].wait()) for p in processes]

    epoch_stop_time = int(time.time())
    local_stop_time = datetime.datetime.fromtimestamp(epoch_stop_time).strftime('%c')

    makespan = epoch_stop_time - epoch_start_time

    logs_list.append("--------------------------------------------------------------------------------------------")
    logs_list.append("Experiment end time: " + str(epoch_stop_time) + " => " + local_stop_time)
    logs_list.append("Experiment makespan: " + str(round(makespan, 3)) + " sec")
    logs_list.append("--------------------------------------------------------------------------------------------")

    output_logs_file = "experiment_logs"
    with open(output_logs_file, "w") as file_handle:
        for line in logs_list:
            file_handle.write("%s\n" % line)
        file_handle.close()

    print("------------------------------------------------")
    print("JOBS EXIT CODES ")
    print("------------------------------------------------")
    for i in exit_codes:
        print('{:38} {}'.format(i[0], i[1]))
    print("------------------------------------------------")


