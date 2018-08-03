from time import time
from PIL import Image, ImageFile
import os

rootdir = '/data/home/has956/input-dir'
outputdir = '/data/home/has956/output-dir/'
runtimes = outputdir + "runtimes.txt"

start_time = time()

runningTime = []

for subdir, dirs, files in os.walk(rootdir):

    dirs.sort()

    for file in sorted(files):

        try:
            ImageFile.LOAD_TRUNCATED_IMAGES = True
            fileInputPath = os.path.join(subdir, file)
            fileOutPath = outputdir + fileInputPath[28:53] + ".png"

            print ("Input: " + fileInputPath + "    Output: " + fileOutPath)

            #im = Image.open(fileInputPath)
            #im.save(fileOutPath)

        except (ValueError, IOError, SyntaxError, TypeError) as e:
            pass

    intermediate_time = time() - start_time
    runningTime.append(subdir[28:] + " = " + str(intermediate_time))

end_time = time() - start_time

with open(runtimes, 'w') as eventsFile:
    for index, value in enumerate(runningTime):
        if index != 0:
            eventsFile.write(value + "\n")
            print (value)

print ('=================================================')
print ("SUCCESS: All images processed in {} seconds".format(round(end_time, 3)))
print ('=================================================')

