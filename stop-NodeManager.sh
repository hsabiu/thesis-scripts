#!/bin/bash  

echo "**********Stopping NodeManager**********"

cd /usr/local/hadoop/
sbin/stop-yarn.sh
cd

echo "============================"
echo "SUCCESS: NodeManager Stopped"
echo "============================"


