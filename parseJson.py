# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to format Spark generated log file into a formatted json file
# Copyright: Any person can adopt this script to their specific need

import json
import glob

# Get all file names in the current working directory
all_files = glob.glob('./*')

# Filter the files that have .txt, .py, .png, .jpg extension.
# This leaves us with only the log files which don't have any extension
log_files = [file for file in all_files if not file.endswith(('.txt', '.py', '.png', '.jpg'))]

for fileName in log_files:
    # A list to hold the parsed json
    parsedJson = []
    with open(fileName) as rawData:
        for line in rawData:
            # Parse each json line by line
            parsedJson.append(json.loads(line))
    # Print the parsed json with indentation of 4
    print "********** File Name = " + fileName + " **********"
    print json.dumps(parsedJson, indent=4, sort_keys=True)
