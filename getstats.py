import pandas as pd
from sys import argv

program, datafile = argv

df = pd.read_csv(datafile, sep = ', ', engine = 'python', index_col = False, skip_blank_lines = True)
#print df

print df.groupby(['User']).mean().nlargest(1000, ['Gigabytes'])
print '\n'
print df.groupby(['Date']).sum()