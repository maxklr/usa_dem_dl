import os

os.environ['USE_PYGEOS'] = '0'
# os.environ["PROJ_LIB"] = r"C:\Users\maxkl\.conda\envs\gis\Library\share\proj"
from osgeo import gdal
import rasterio
import rasterio.warp
from rasterio.warp import reproject, Resampling
import requests
import json
import os
from os import walk
from glob import glob
import shutil
from zipfile import ZipFile
import time
import numpy as np
import pickle

https://data.tnris.org/collection?c=90ac2f86-6bb7-49d3-89db-f4557376ebb6

# For resource types
resource_type = "Digital%20Elevation%20Model"
api_url = f"https://api.tnris.org/api/v1/resources?resource_type_name={resource_type}&limit=30000"
dl_path = r"F:\Houston\dem\2023_tnris_complete"
rastertuple = ('.img', '.tif', '.TIF')
exList = [f for f in next(walk(dl_path), (None, None, []))[2]  if not f.endswith(rastertuple)]

# Used to write binary file from list can be retrieved to a list by using 
# with open ('/content/list_1.ob', 'rb') as fp:
#     list_1 = pickle.load(fp)

zipListLog = f"E:\\Houston\dem\\USGS_dem_dowload\\data\\2023\\log{time.strftime('%Y%m%d-%H%M%S')}.ob"

# For collections
collection_id = "6ddcc1e6-2059-4fa2-a2cf-4ab163e2c97e"
api_url = f"https://api.tnris.org/api/v1/resources?collection_id={collection_id}&limit=3000"


api_json = json.loads(requests.get(api_url).text)
print(api_json.keys())

# Get list of urls from api. Used to discover files not downloaded
resultList = []
for i in api_json['results']:
    resultList.append(i['resource'])

zipList = []
for i in api_json['results']:
    url = i['resource']
    if url.endswith('dem.zip') and url not in zipList:
        r = requests.get(url)
        name = os.path.join(dl_path, url.split("/")[-1])
        zipList.append(name)
        with open(name, 'wb') as outfile:
            outfile.write(r.content)
        time.sleep(1)   

# List of failed downloads:
exList = []
for i in zipList:
    exList.append(i.split("\\")[-1])

demList = []
for i in resultList:
    if i.split("/")[-1] not in exList:
        demList.append(i)
    else:
        pass

# download remaining links:
zipList = []
for i in demList:
    if i.endswith('dem.zip'):
        r = requests.get(i)
        name = os.path.join(dl_path, i.split("/")[-1])
        zipList.append(name)
        with open(name, 'wb') as outfile:
            outfile.write(r.content)
        time.sleep(1)  

with open

x=0
for i in api_json['results']:
    x =+ i['filesize']
    time.sleep(0.01)