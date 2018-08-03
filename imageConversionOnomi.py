from time import time
from PIL import Image, ImageFile
import os
import sys

input_path = sys.argv[1]
output_path = sys.argv[2]

fileName = input_path[28:33] + "_RunTime.txt"

start_time = time()

for subdir, dirs, files in os.walk(input_path):

    dirs.sort()

    for file in sorted(files):

        try:
            ImageFile.LOAD_TRUNCATED_IMAGES = True
            fileInputPath = os.path.join(subdir, file)
            fileOutPath = output_path + fileInputPath[34:53] + ".png"

            #print ("Input: " + fileInputPath + "    Output: " + fileOutPath)

            im = Image.open(fileInputPath)
            im.save(fileOutPath)

        except (ValueError, IOError, SyntaxError, TypeError) as e:
            pass

end_time = time() - start_time

with open(fileName, 'w') as eventsFile:
    eventsFile.write(str(round(end_time, 3)) + "\n")

print ('===========================================================')
print ("SUCCESS: Job " + input_path[28:33] +" finished processing in " + str(round(end_time, 3)) + " seconds")
print ('===========================================================')

