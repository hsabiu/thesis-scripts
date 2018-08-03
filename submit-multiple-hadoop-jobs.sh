#!/bin/bash

#################################################################################################
# Job 5 prefix
input_prefix="habib/still_camera_images/hipi-input/"
output_prefix="habib/converted_images_output/hipi-output/"
job_name_prefix='MRImageConversion_'
#################################################################################################

# minimum and maximum values for random number generation 
#RANDOM_MIN=1
#RANDOM_MAX=20

job_counter=0

for i in `seq 1 5`; do

	# Un-normalized random timer
        #random_timer=$(( $RANDOM % ($RANDOM_MAX + 1 - $RANDOM_MIN) + $RANDOM_MIN ))

        #Inter-arrival rate parameter(lambda) = 1/60 ~= 0.016. This means that on average
        #a new job will be submitted after every 60 seconds (or 1 minute)
        #poisson_inter_arrival=$(python -c "import random;print(int(random.expovariate(0.016)))")

        job_counter=$((job_counter+1))
        echo "Submitting ---> MRImageConversion_job_" $job_counter "then sleep for 60 seconds";

        # Set command line arguments
        input_path=$input_prefix"job_"$job_counter".hib"
        output_path=$output_prefix"job_"$job_counter
        job_name=$job_name_prefix$"job_"$job_counter

        #echo "################################"
        #echo $input_path
        #echo $output_path
        #echo $job_name
        #echo "################################"

        # Remove all previous .png files in the output path
        #rm output_path/*.png      

        # Submit MapReduce jobs to YARN
        #yarn jar hibToPng.jar $input_path $output_path $job_name & sleep $poisson_inter_arrival;
        yarn jar hibToPng.jar $input_path $output_path $job_name & sleep 60;

done  

echo "=============================="
echo "SUCCESS: ALL JOBS ARE SUBMITED"
echo "=============================="
