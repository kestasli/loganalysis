import pandas as pd
from sys import argv
import os

def getLogFiles(path):

    filelist = os.listdir(path)
    logfiles = []

    for file in filelist:
        if file.startswith('account-stats'):
            logfiles.append(path + "/" + file)
    return logfiles

program, datafile = argv

print getLogFiles('csv')

df = pd.read_csv(datafile, sep = ', ', engine = 'python', index_col = False, skip_blank_lines = True)
#print df

print df.groupby(['User']).mean().nlargest(50, ['Gigabytes'])
print '\n'
print df.groupby(['Date']).sum()