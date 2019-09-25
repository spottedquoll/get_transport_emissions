import os
from pandas import DataFrame, read_excel, concat
from math import isnan

transport_dir = os.environ['TRANSPORT_DIR']

print('Processing IEA data')

results = []

for year in range(2000, 2013):

    df = read_excel(transport_dir + '/IEA/IEA' + str(year) + '.xlsx', header=0)

print('All finished')