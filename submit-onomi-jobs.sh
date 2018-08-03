#!/bin/bash

# Author: Habib Sabiu
# Date: May 14, 2017
# Purpose: A script to automatically run multiple 'imageConversionOnomi.py' on 'onomi'
#          server. The script can generate un-normalized random number, poisson distribution,
#          and constant inter-arrival time.
# Copyright: Any person can adopt this script to their specific need

#################################################################################################
# Job 5 prefix
input_prefix="/data/home/has956/input-dir/"
output_prefix="/data/home/has956/output-dir/"
#################################################################################################

# Minimum and maximum values for random number generation
#RANDOM_MIN=1
#RANDOM_MAX=20

job_counter=0

for i in `seq 1 5`; do

        # Un-normalized random timer
        #random_timer=$(( $RANDOM % ($RANDOM_MAX + 1 - $RANDOM_MIN) + $RANDOM_MIN ))

        # Inter-arrival rate parameter(lambda) = 1/60 ~= 0.016. This means that on average
        # a new job will be submitted after every 60 seconds (or 1 minute)
        #poisson_inter_arrival=$(python -c "import random;print(int(random.expovariate(0.016)))")

        job_counter=$((job_counter+1))
        echo "Submitting ---> ImageConversion_job_" $job_counter "then sleep for 20 seconds";

        # Set command line arguments
        input_path=$input_prefix"job_"$job_counter"/"
        output_path=$output_prefix"job_"$job_counter"/"

        #echo "################################"
        #echo $input_path
        #echo $output_path
        #echo "################################"

        # Submit jobs
        python imageConversionOnomi.py $input_path $output_path & sleep 20;

done

echo "=============================="
echo "SUCCESS: ALL JOBS ARE SUBMITED"
echo "=============================="
