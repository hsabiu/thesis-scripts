#!/bin/bash  

echo "**********Starting NodeManager**********"

cd /usr/local/hadoop/
sbin/start-yarn.sh
cd

echo "============================"
echo "SUCCESS: NodeManager Started"
echo "============================"
