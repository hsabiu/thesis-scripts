#!/bin/bash

# Author: Habib Sabiu
# Date: May 14, 2017
# Purpose: A script to automatically submit multiple Spark jobs to different cluster
#          managers including Standalone, Mesos, and YARN. Jobs can be imageConversions
#          or imageRegistration. The script can generate un-normalized random number,
#          poisson distribution, and constant inter-arrival time. It can also be used to
#          submit a mixture of different type of applications such as imageConversions
#          and imageRegistration
# Copyright: Any person can adopt this script to their specific need

#################################################################################################
# This is for imageConversions jobs
input_prefix_conversion="hdfs://discus-p2irc-master:54310/user/hduser/habib/still_camera_images/"
output_prefix_conversion="/data/mounted_hdfs_path/user/hduser/habib/converted_images_output/"
job_name_prefix_conversion='imageConversion_'
#################################################################################################

#################################################################################################
# This is for imageRegistration jobs
input_prefix_registration="hdfs://discus-p2irc-master:54310/user/hduser/habib/drone_images_tif/"
output_prefix_registration="/data/mounted_hdfs_path/user/hduser/habib/registered_images_output/"
job_name_prefix_registration='imageRegistration_'
#################################################################################################


# Minimum and maximum values for random number generation
#RANDOM_MIN=1
#RANDOM_MAX=20

conversion_job_counter=0
registration_job_counter=0

for i in `seq 1 5`; do
#for i in `seq 1 10`; do

    # Un-normalized random timer
    #random_timer=$(( $RANDOM % ($RANDOM_MAX + 1 - $RANDOM_MIN) + $RANDOM_MIN ))

    # Inter-arrival rate parameter(lambda) = 1/60 ~= 0.016. This means that on average
    # a new job will be submitted after every 60 seconds (or 1 minute)
    #poisson_inter_arrival=$(python -c "import random;print(int(random.expovariate(0.016)))")

    #if (($poisson_inter_arrival % 2 == 0))

    #then

        registration_job_counter=$((registration_job_counter+1))
        #echo "Submitting ---> imageRegistration_job_"$registration_job_counter "then sleep for" $poisson_inter_arrival "seconds";

        # Set command line arguments
        input_path_registration=$input_prefix_registration"job_"$registration_job_counter
        output_path_registration=$output_prefix_registration"job_"$registration_job_counter
        job_name_registration=$job_name_prefix_registration"job_"$registration_job_counter"_YARN_Dynamic_176Tasks_With_1cores_1GB_Per_Executors_Run_2"

        echo "Submitting ---> "$job_name_registration" then sleep for 60 seconds";

        #echo "################################"
        #echo $input_path_registration
        #echo $output_path_registration
        #echo $job_name_registration
        #echo "################################"

        # Remove all previous .png files in the output path
        #rm output_path_registration/*.png

        # Submit jobs to spark standalone cluster manager
        #spark-submit --master spark://discus-p2irc-master:7077 /data/scripts/imageRegistration.py $input_path_registration $output_path_registration $job_name_registration & sleep 60;

        # Submit jobs to Mesos cluster manager
        #spark-submit --master mesos://discus-p2irc-master:5050 /data/scripts/imageRegistration.py $input_path_registration $output_path_registration $job_name_registration & sleep 60;

        # Submit jobs to YARN cluster manager
        spark-submit --master yarn /home/habib/python/imageRegistration.py $input_path_registration $output_path_registration $job_name_registration  & sleep 60;

    #else

        #conversion_job_counter=$((conversion_job_counter+1))
        #echo "Submitting ---> imageConversion_job_" $conversion_job_counter "then sleep for 60 seconds";

        # Set command line arguments
        #input_path_conversion=$input_prefix_conversion"job_"$conversion_job_counter
        #output_path_conversion=$output_prefix_conversion"job_"$conversion_job_counter
        #job_name_conversion=$job_name_prefix_conversion"job_"$conversion_job_counter

        #echo "################################"
        #echo $input_path_conversion
        #echo $output_path_conversion
        #echo $job_name_conversion
        #echo "################################"

        # Remove all previous .png files in the output path
        #rm output_path_conversion/*.png

        # Submit jobs to spark standalone cluster manager
        #spark-submit --master spark://discus-p2irc-master:7077 /data/scripts/imageConversion.py $input_path_conversion $output_path_conversion $job_name_conversion & sleep 20;

        # Submit jobs to Mesos cluster manager
        #spark-submit --master mesos://discus-p2irc-master:5050 /data/scripts/imageConversion.py $input_path_conversion $output_path_conversion $job_name_conversion & sleep 60;

        # Submit jobs to YARN cluster manager
        #spark-submit --master yarn /data/scripts/imageConversion.py $input_path_conversion $output_path_conversion $job_name_conversion & sleep 20;

    #fi

done

echo "=============================="
echo "SUCCESS: ALL JOBS ARE SUBMITED"
echo "=============================="



# Submit har jobs to spark standalone cluster manager
#spark-submit --master spark://discus-p2irc-master:7077 /data/scripts/imageConversion.py har:///user/hduser/habib/still_camera_images/job_1.har /data/mounted_hdfs_path/user/hduser/habib/converted_images_output/job_1 imageCoversion_har & sleep 60;

# Submit jobs to spark standalone cluster manager using poisson inter-arrival
#spark-submit --master spark://discus-p2irc-master:7077 /data/scripts/imageConversion.py $input_path_conversion $output_path_conversion $job_name_conversion & sleep $poisson_inter_arrival;

#Submit MapReduce jobs to YARN
#yarn jar hibToPng.jar habib/still_camera_images/hipi-image-bundle.hib habib/converted_images_output/hipi-output
