#!/bin/bash

echo "--------------------------------------------------------------------------------"
echo "Experiment start time:  $(date)"
echo "--------------------------------------------------------------------------------"

for i in `seq 1 3`; do

    if (($i == 1))

    then 
        # Submit flowerCounter job to spark standalone cluster manager
        #spark-submit --master spark://discus-p2irc-master:7077 --py-files /data/habib/scripts/canola_timelapse_image.py /data/habib/scripts/imageFlowerCounterNew.py hdfs://discus-p2irc-master:54310/user/hduser/habib/still_camera_images/flat_02082016_2103 hdfs://discus-p2irc-master:54310/user/hduser/habib/outputs/flower_counter flowerCounter & sleep 300;

        # Submit flowerCounter job to Mesos cluster manager
        #spark-submit --master mesos://discus-p2irc-master:5050 --py-files /data/habib/scripts/canola_timelapse_image.py /data/habib/scripts/imageFlowerCounterNew.py hdfs://discus-p2irc-master:54310/user/hduser/habib/still_camera_images/flat_02082016_2103 hdfs://discus-p2irc-master:54310/user/hduser/habib/outputs/flower_counter flowerCounter & sleep 300;

        # Submit flowerCounter job to YARN cluster manager
        spark-submit --master yarn --py-files /data/habib/scripts/canola_timelapse_image.py /data/habib/scripts/imageFlowerCounterNew.py hdfs://discus-p2irc-master:54310/user/hduser/habib/still_camera_images/flat_02082016_2103 hdfs://discus-p2irc-master:54310/user/hduser/habib/outputs/flower_counter flowerCounter & sleep 300;

    elif (($i == 2))

    then
        # Submit imageClustering job to spark standalone cluster manager
        #spark-submit --master spark://discus-p2irc-master:7077 /data/habib/scripts/imageClustering.py hdfs://discus-p2irc-master:54310/user/hduser/habib/still_camera_images/flat_02082016_2103 hdfs://discus-p2irc-master:54310/user/hduser/habib/outputs/image_clustering imageClustering & sleep 300;

        #spark-submit --master mesos://discus-p2irc-master:5050 /data/habib/scripts/imageClustering.py hdfs://discus-p2irc-master:54310/user/hduser/habib/still_camera_images/flat_02082016_2103 hdfs://discus-p2irc-master:54310/user/hduser/habib/outputs/image_clustering imageClustering & sleep 300;

        # Submit imageClustering job to YARN cluster manager
        spark-submit --master yarn /data/habib/scripts/imageClustering.py hdfs://discus-p2irc-master:54310/user/hduser/habib/still_camera_images/flat_02082016_2103 hdfs://discus-p2irc-master:54310/user/hduser/habib/outputs/image_clustering imageClustering & sleep 300;

    elif (($i == 3))

    then
        # Submit imageRegistration job to spark standalone cluster manager
        #spark-submit --master spark://discus-p2irc-master:7077 /data/habib/scripts/imageRegistration.py hdfs://discus-p2irc-master:54310/user/hduser/habib/drone_images/06082016/png /data/mounted_hdfs_path/user/hduser/habib/outputs/image_registration/ imageRestration;

        # Submit imageRegistration job to Mesos cluster manager
        #spark-submit --master mesos://discus-p2irc-master:5050 /data/habib/scripts/imageRegistration.py hdfs://discus-p2irc-master:54310/user/hduser/habib/drone_images/06082016/png /data/mounted_hdfs_path/user/hduser/habib/outputs/image_registration/ imageRestration;

        # Submit imageRegistration job to YARN cluster manager
        spark-submit --master yarn /data/habib/scripts/imageRegistration.py hdfs://discus-p2irc-master:54310/user/hduser/habib/drone_images/06082016/png /data/mounted_hdfs_path/user/hduser/habib/outputs/image_registration/ imageRestration;

    fi

done

echo "--------------------------------------------------------------------------------"
echo "SUCCESS: ALL JOBS WERE SUBMITED"
echo "Experiment end time:  $(date)"
echo "--------------------------------------------------------------------------------"


