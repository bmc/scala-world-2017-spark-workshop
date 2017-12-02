#import csv
import codecs
import sys
import os
from grizzled.file import eglob

if len(sys.argv) < 3:
    print("Usage: {0} year dir [dir] ...".format(sys.argv[0]))
    sys.exit(1)

output = sys.argv[1]
    
wrote_header = False
with open(output, mode='w') as output:
    for dir in sys.argv[2:]:
        if not os.path.isdir(dir):
            print("Skipping {0}: Not a directory.".format(dir))
            continue

        for csv in eglob("{0}/**/*.csv".format(dir)):
            if not os.path.isfile(csv):
                print("Skipping {0}: Not a file.".format(csv))
                continue

            print("Processing {0}...".format(csv))
            with open(csv, mode='r') as input:
                lines = input.readlines()

            if not wrote_header:
                output.write(lines[0])
                wrote_header = True

            for line in lines[1:]:
                output.write(line)
            
        
        
