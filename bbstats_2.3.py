import os
import time
from datetime import datetime
from datetime import timedelta
from sys import argv
import ConfigParser

import pandas as pd
from netaddr import IPAddress, IPNetwork


class iptoclient:
    clientsIPdb = []

    def __init__(self, ripedb):

        clientIPRange = []
        with open(ripedb) as f:
            for line in f:
                # neskaitome, jeigu komentaras t.y. prasideda nuo #
                if not line.startswith("#"):
                    lineList = line.split(",")
                    # print lineList
                    for x in lineList[1:]:
                        clientIPRange.append(lineList[0])
                        a = x.replace(" ", "")
                        a = a.replace("\r", "")
                        a = a.replace("\n", "")
                        a = a.split("-")
                        # yra naudojama address range atskirtas - zenklu
                        if len(a) == 2:
                            clientIPRange.append(IPAddress(a[0]))
                            clientIPRange.append(IPAddress(a[1]))
                        # nera naudojamas address range atskirtas - zenklu
                        if len(a) == 1:
                            # yra naudojamas subnetas, jeigu su /
                            if "/" in a[0]:
                                clientIPRange.append(IPNetwork(a[0]).network)
                                clientIPRange.append(IPNetwork(a[0]).broadcast)
                            else:
                                clientIPRange.append(IPAddress(a[0]))
                                clientIPRange.append(IPAddress(a[0]))

                        # reikia panaudoti list() komanda, nes turi buti padaryta list'o kopija
                        iptoclient.clientsIPdb.append(list(clientIPRange))
                        del clientIPRange[:]

    def get_name(self, IP):
        for ipranges in self.clientsIPdb:
            if IPAddress(IP) >= ipranges[1] and IPAddress(IP) <= ipranges[2]:
                return ipranges[0]

    def isclient(self, IP):
        for ipranges in self.clientsIPdb:
            if IPAddress(IP) >= ipranges[1] and IPAddress(IP) <= ipranges[2]:
                return True
        return False

def getLogFiles(path):

    filelist = os.listdir(path)
    logfiles = []

    for file in filelist:
        if "access.log" in file:
            logfiles.append(path + "/" + file)
    return logfiles

start_time = time.time()

statsconfig = ConfigParser.ConfigParser()
statsconfig.read(['config.cfg'])

program, start, end = argv

startdate = datetime.strptime(start, "%Y-%m-%d")
enddate = datetime.strptime(end, "%Y-%m-%d") + timedelta(hours = 24)
#enddate = datetime.strptime(end, "%Y-%m-%d")

pd.options.mode.chained_assignment = None
pd.options.display.max_rows = 1000

regex_long = '([(\d\.)]+) - .*? \[(\d\d/.../\d{4}:\d\d:\d\d:\d\d) .*?\] +".*? (.*?) .*?" \d+ \d+ "(.*?)" "(.*?)" "(.*?)"'

filelist = getLogFiles('logfiles')

ds_raw = pd.DataFrame()

for logfile in filelist:
    print 'Reading :', logfile
    x1 = pd.read_csv(logfile , delimiter = regex_long, engine = 'python',
                     index_col = False , skip_blank_lines = True, header = None, keep_default_na = False,
                     names=['X', 'IP', 'DATETIME', 'URL', 'REF', 'BROWSER', 'UID', 'Y'])
    ds_raw = pd.concat([ds_raw, x1], ignore_index=True)


#po regex'o atsiranda kazkokie parazitiniai columnt'ai, cia juos isnaikiname
del ds_raw['X']
del ds_raw['Y']

#parodome, kiek parsinimo klaidu buvo
print '\nParse errors:', ds_raw['IP'].isnull().sum(), '\n'

#pasaliname visas eilutes, kur parseris 'atidavo not match'
ds = ds_raw.dropna()

#performatuojame laiko ir UID formatus
ds['DATETIME'] = pd.to_datetime(ds['DATETIME'], format = '%d/%b/%Y:%H:%M:%S')
ds['UID'] = ds['UID'].replace('^uid=', '', regex = True)

#isfiltruojame duoemenis tam tikram periodui
datemask = (ds['DATETIME'] > startdate) & (ds['DATETIME'] <= enddate)
ds = ds.loc[datemask]
###########################FORMATING DONE###########################

#################Find and show clients related info#################
converter = iptoclient('ripedb.txt')

#isrenkame tik unikalius IP adresus
uniqueip = ds.drop_duplicates(subset = 'IP', keep = 'first')

#surandame, kurie adresai priklauso klientams
clientipsubset = [ip for ip in uniqueip['IP'] if converter.isclient(ip)]
#print clientipsubset

#paliekame tik tas eilutes, kurios turi kliento IP adresa ir pasibaigia /
filtered = ds[ds['IP'].isin(clientipsubset)]

#konvertuojame ip adresus i klientu pavadinimus
mappedip = filtered['IP'].map(converter.get_name)

#pridedame column'a su klientu pavadinimais
filtered['CLIENT'] = mappedip

urlfilter = statsconfig.get('filters', 'url')
clientfilter = statsconfig.get('filters', 'client')
filtered = filtered[(filtered['URL'].str.contains(urlfilter)) & filtered['CLIENT'.str(clientfilter)]]

#pateikiame ataskaitas
print filtered.groupby(['CLIENT']).size().nlargest(50)
print '\n'
print filtered.groupby(['URL']).size().nlargest(50)
print '\n'
print filtered.groupby(['CLIENT', 'URL']).size()
print '\n'
print filtered.groupby(['CLIENT', 'UID']).size()
print '\n'
print filtered.groupby(['CLIENT', 'IP']).size()

print '\n'
print 'Loglines: ', len(ds)
print 'Unique IP: ', len(uniqueip['IP'])
print("Exec time: %s seconds" % (time.time() - start_time))

if statsconfig.get('export', 'toexcel') == '1':
    filtered.to_excel('BBreport.xlsx', 'fltered')

#todo: sukurti config faila, kuriame galima nurodyti url'us, imoniu pavadinimus ir UID