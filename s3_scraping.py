import os
os.environ['USE_PYGEOS'] = '0'

from osgeo import gdal
import rasterio
import rasterio.warp
from rasterio.warp import reproject, Resampling
from rasterio.enums import ColorInterp
import glob
import requests
import pandas as pd
import geopandas as gpd
import time
import xml.etree.ElementTree as ET

# Link: https://tnris-data-warehouse.s3.us-east-1.amazonaws.com/index.html?prefix=LCD/collection/
fileEndings = ['.img', '.tif']

url = "https://tnris-data-warehouse.s3.us-east-1.amazonaws.com/LCD/collection/"
outroot = r"E:\Houston\dem\USGS_dem_dowload\2023\nodata_test"

# CSV file for 
csvFile = r"E:\Houston\dem\USGS_dem_dowload\2023\nodata_test\test_tiles_nodata.csv"
df = pd.read_csv(csvFile, sep=',')
# df['dirname'] = df['dirname'].replace('cityofgeorgetown-2015-50cm', 'city-of-georgetown-2015-50cm')
# df['dirname'] = df['dirname'].replace('stratmap-2014-50cm-lampasas', 'stratmap-2014-50cm-bandera-lampasas')

dirDem = list(zip(df.dirname, df.demname))

# Compare directories to list of dirname
# Get the list of subdirs from S3
def list_subdirectories(bucket, prefix):
    url = f'https://{bucket}.s3.amazonaws.com/?prefix={prefix}&delimiter=/'
    response = requests.get(url)
    
    if response.status_code != 200:
        print("Failed to fetch the URL")
        return []

    root = ET.fromstring(response.content)
    subdirectories = set()

    for prefix in root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}CommonPrefixes'):
        subdir = prefix.find('{http://s3.amazonaws.com/doc/2006-03-01/}Prefix').text
        subdirectories.add(subdir)

    return subdirectories

bucket_name = 'tnris-data-warehouse'
prefix = 'LCD/collection/'

subdirectories = list_subdirectories(bucket_name, prefix)

# for subdir in subdirectories:
#     print(subdir.split('/'))


# 
used_list = []
def buildUrl(url, fileEndings, dirname, demname, outroot):
    for i in fileEndings:
        dlPath = f"{url}{dirname}/dem/{demname}{i}"
        print(dlPath)
        outpath = os.path.join(outroot, dlPath.split('/')[-1])
        data = requests.get(dlPath)
        print(f"{i} response length: {len(data.content)}")
        if len(data.content) > 500:
            with open(outpath, 'wb') as raster:
                raster.write(data.content)
            used_list.append(dlPath)
        else:
            continue
        time.sleep(1)

#Run
for i, j in dirDem:
    buildUrl(url, fileEndings, i, j, outroot)


# Alternative with timeout error handling:
sed_list = []
skipList = []

def buildUrl(url, fileEndings, dirname, demname, outroot, max_retries=5):
    for i in fileEndings:
        dlPath = f"{url}{dirname}/dem/{demname}{i}"
        print(dlPath)
        outpath = os.path.join(outroot, dlPath.split('/')[-1])
        
        success = False  # Flag to check if any fileEndings download is successful
        retries = 0
        
        while retries < max_retries:
            try:
                data = requests.get(dlPath)
                print(f"{i} response length: {len(data.content)}")
                
                if len(data.content) > 500:
                    with open(outpath, 'wb') as raster:
                        raster.write(data.content)
                    used_list.append(dlPath)
                    success = True
                    break  # Successful download, so break out of the retry loop for this fileEnding
                else:
                    retries += 1  # Increment retries if file is not larger than 500 bytes
            except requests.ConnectionError as e:
                print(f"Error occurred: {e}")
                retries += 1
                
                if retries < max_retries:
                    print("Retrying after 10 seconds...")
                    time.sleep(10)
        
        if success:
            break  # If any fileEnding is successfully downloaded, break out of the fileEndings loop
    
    # If no successful download for any fileEnding, add base name to skipList
    if not success:
        skipBase = f"{url}{dirname}/dem/{demname}"
        skipList.append(skipBase)

        time.sleep(1)

# Example Run
# url, fileEndings, newDirDem, and outroot would be defined elsewhere in your code.
# for i, j in newDirDem:
#     buildUrl(url, fileEndings, i, j, outroot)

# Check for projection in all rasters:
rasPath = r"E:\Houston\dem\USGS_dem_dowload\2023\danevang_el_campo\data"
rasList = []
outname = []

for filename in os.listdir(rasPath):
    if filename.endswith('.img') or filename.endswith('.tif'):
        rasList.append(os.path.join(rasPath, filename))
        outname.append(filename)



testUsed = used_list
testSplit = [i.split('/')[-1].split('.')[0] for i in testUsed]
testDlList = dirDem


newList = []
for i in testDlList:
    if i[1] not in testSplit:
        newList.append(i)
    else:
        pass


# Check for existing files in dirDem based on rasList (lsit of dowloaded rasters):
# Extract all filenames from rasList
file_names = [os.path.basename(path)[:-4] for path in rasList]

# Check items in dirDem against filenames, keep the ones that don't match
newDirDem = [item for item in dirDem if not any(fname.startswith(item[1]) for fname in file_names)]


# Reproject all rasters to 26915

# Builds the output url from path of original raster. Separates the filename from rest of path with rpartition.
crs_out = 'EPSG:26915'
tifRoot = r"E:\Houston\dem\USGS_dem_dowload\2023\danevang_el_campo"


def outpath(crs, filepath, tifRoot):
    return os.path.join(tifRoot, filepath.rpartition('\\')[-1][:-3] + 'tif')

# Reprojection
# SHould include a defined cell size also.
def reproj(crs, filepath, cellsize=None):
    outtif = os.path.join(tifRoot, filepath.rpartition('\\')[-1][:-3] + 'tif')
    with rasterio.open(filepath, 'r+') as src:
        #src.colorinterp = [ColorInterp.gray]
        if src.crs != crs:
            dst_crs = crs
            #src.nodata = 127 # Sett no data til rett verdi (i datasettet)
            transform, width, height = rasterio.warp.calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': dst_crs,
                'transform': transform,
                'width': width,
                'height': height
            })
        
            with rasterio.open(outtif, 'w', **kwargs) as dst:
                src.colorinterp = [ColorInterp.gray]
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest)
        else:
            pass
            print("CRS is already correct")

for i in rasList:
    reproj(crs_out, i)


# Check for projection in all rasters:
imgPath = r"E:\Houston\dem\USGS_dem_dowload\2023\austin\26915_2"
imgList = []

for filename in os.listdir(rasPath):
    if filename.endswith('.img') or filename.endswith('.tif'):
        imgList.append(os.path.join(rasPath, filename))

with open('austin_vrt_list.txt', mode='wt', encoding='utf-8') as myfile:
    myfile.write('\n'.join(imgList))




