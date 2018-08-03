#!/bin/bash  

USERNAME=hduser
HOSTS="discus-p2irc-mario discus-p2irc-luigi discus-p2irc-worker1 discus-p2irc-worker2 discus-p2irc-worker3 discus-p2irc-worker4 discus-p2irc-worker5 discus-p2irc-worker6 discus-p2irc-worker7 discus-p2irc-worker8 discus-p2irc-worker9"

SCRIPT="cd; cd /usr/local/spark-2.1.0/; sbin/start-mesos-shuffle-service.sh; cd"

echo "**********Starting Mesos Shuffle Service**********"

for HOSTNAME in ${HOSTS} ; do
    ssh -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "${SCRIPT}"
    echo "**********Mesos Shuffle Service Started on" ${HOSTNAME}"**********" 
done

echo ""
echo "==================================================="
echo "SUCCESS: Mesos shuffle service running on all nodes"
echo "==================================================="

