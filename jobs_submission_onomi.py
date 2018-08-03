# October 14, 2017
# Habib Sabiu - Script to submit image processing applications to Onomi. The script also logs start and end of the experiment as well as jobs submission time.

import time
import datetime
import subprocess
import numpy as np


onomi_jobs = np.array([
    ["python", "imageFlowerCounterSequential.py", "-i", "/workspace/home/has956/inputs/still_camera_images/15072016_1108_images12/", "-o", "/workspace/home/has956/outputs/flower_counter/job_1/", "flowerCounter_job_1"],
    ["python", "imageClusteringSequential.py", "-i", "/workspace/home/has956/inputs/still_camera_images/15072016_1108_images1_7_10_13/", "-o", "/workspace/home/has956/outputs/image_clustering/job_1/", "imageClustering_job_1"], 
    ["python", "imageRegistrationSequential.py", "-i", "/workspace/home/has956/inputs/drone_images/23062016/png/", "-o", "/workspace/home/has956/outputs/image_registration/job_1/", "-r", "-c", "imageRegistration_job_1"],

    ["python", "imageFlowerCounterSequential.py", "-i", "/workspace/home/has956/inputs/still_camera_images/15072016_1108_images14/", "-o", "/workspace/home/has956/outputs/flower_counter/job_2/", "flowerCounter_job_2"],
    ["python", "imageClusteringSequential.py", "-i", "/workspace/home/has956/inputs/still_camera_images/15082016_1108_images1_7_10/", "-o", "/workspace/home/has956/outputs/image_clustering/job_2/", "imageClustering_job_2"], 
    ["python", "imageRegistrationSequential.py", "-i", "/workspace/home/has956/inputs/drone_images/14072016/png/", "-o", "/workspace/home/has956/outputs/image_registration/job_2/", "-r", "-c", "imageRegistration_job_2"],

    ["python", "imageFlowerCounterSequential.py", "-i", "/workspace/home/has956/inputs/still_camera_images/15082016_1108_images_0/", "-o", "/workspace/home/has956/outputs/flower_counter/job_3/", "flowerCounter_job_3"],
    ["python", "imageClusteringSequential.py", "-i", "/workspace/home/has956/inputs/still_camera_images/15072016_1108_images2_4_5/", "-o", "/workspace/home/has956/outputs/image_clustering/job_3/", "imageClustering_job_3"], 
    ["python", "imageRegistrationSequential.py", "-i", "/workspace/home/has956/inputs/drone_images/06082016/png/", "-o", "/workspace/home/has956/outputs/image_registration/job_3/", "-r", "-c", "imageRegistration_job_3"]
    ]) 

np.random.seed(100)
random_choice = np.random.choice(onomi_jobs, 9, replace=False)

epoch_start_time = int(time.time())
local_start_time = datetime.datetime.fromtimestamp(epoch_start_time).strftime('%c')

logs_list = []
processes = []

logs_list.append("----------------------------------------------------------------------------")
logs_list.append("Experiment start time: " + str(epoch_start_time) + " => " + local_start_time)
logs_list.append("----------------------------------------------------------------------------")

for job in random_choice:
    
    p = subprocess.Popen(job[0:-1])
    processes.append(p)
    
    epoch_current_time = int(time.time())
    local_current_time = datetime.datetime.fromtimestamp(epoch_current_time).strftime('%c')
    
    line = '{} {:23s} {} {} {} {}'.format("Submitted", job[-1], "on", str(epoch_current_time), "=>", local_current_time)
    logs_list.append(line)
    
    time.sleep(300)

exit_codes = [p.wait() for p in processes]

epoch_stop_time = int(time.time())
local_stop_time = datetime.datetime.fromtimestamp(epoch_stop_time).strftime('%c')

makespan = epoch_stop_time - epoch_start_time

logs_list.append("----------------------------------------------------------------------------")
logs_list.append("Experiment end time: " + str(epoch_stop_time) + " => " + local_stop_time)
logs_list.append("Experiment makespan: " + str(round(makespan, 3)) + " sec")
logs_list.append("----------------------------------------------------------------------------")


output_logs_file = "experiment_logs"
with open (output_logs_file, "w") as file_handle:
    for line in logs_list:
        file_handle.write("%s\n" % line)
    file_handle.close()

print("-------------------------------------------------------------------------")
print("SUCCESS: ALL JOBS ARE DONE...With exit codes: " + str(exit_codes))
print("-------------------------------------------------------------------------")

