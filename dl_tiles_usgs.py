import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
from osgeo import gdal
import requests
import pandas as pd
import os
from os import walk
from zipfile import ZipFile
from zipfile import BadZipFile
import shutil
import rasterio
import rasterio.features
import rasterio.warp
from shapely.geometry import shape


# Data and paths - make sure to change before running the downloads
# CSV generated from https://apps.nationalmap.gov/downloader/
# How to get access to s3??? 
data = r"U:\USA\philly\data.csv"
# Working directory - create folder beforehand
path = os.path.join(r"U:\USA\philly\data")

# List of raster formats, used in the sortraster function later
rastertuple = ('.img', '.tif', '.TIF')

# Functions for unzipping and organizing data
# Sorts raster files from non-raster files. 
def sortrasters(rastertuple, path):
    nonraster = [f for f in next(walk(path), (None, None, []))[2]  if not f.endswith(rastertuple)] #List all files that are not rasters
    # Check if list of non rasters is empty
    if not nonraster:
        print("Only raster files")
    else:
        os.makedirs(os.path.join(path, "ancillaryData"))
        print("Moving non-rasters to //ancillary data")
        for f in nonraster:
            shutil.move(os.path.join(path, f), os.path.join(path, "ancillaryData"))


# Funksjon som Unzipper filer til path og flytter zip filer i mappe zipArchive (bør være tom)
def unzip(path):
    badZip = []
    #list files ending with .zip
    ziplist = [f for f in next(walk(path), (None, None, []))[2]  if f.endswith('zip')]
    # Check for zip files in list
    if not ziplist:
        print("No zip files")
    else:
        print("Running unzip")
        os.makedirs(os.path.join(path, "zipArchive"))
        for f in ziplist:
            try:
                with ZipFile(os.path.join(path, f),"r") as zip_ref:
                    zip_ref.extractall(path)
                shutil.move(os.path.join(path, f), os.path.join(path, "zipArchive", f))
            except BadZipFile:
                badZip.append(f)
                pass

# Read dataframe from csv files, usecols is column where url is stored.
df = pd.read_csv(data, sep=",", header = None, usecols=[14])

#List urls
urlList = df.iloc[:, 0].tolist()

# Henter alle filer fra liste, filbane hentes fra path og filnavn er alt etter siste "/" i url
for i in urlList:
    filepath = os.path.join(path, i.rpartition('/')[-1])
    myfile = requests.get(i, allow_redirects=True)
    with open(filepath, 'wb') as tiff:
        tiff.write(myfile.content)

# Run the sorting function, separating the non-rasters to a separate folder.
unzip(path)
sortrasters(rastertuple, path)

#Reprojecting:
# Get rasters: 


def outpath(crs, filepath, tifRoot):
    return os.path.join(tifRoot, filepath.rpartition('\\')[-1][:-3] + 'tif')

# Reprojection
# Builds the output url from path of original raster. Separates the filename from rest of path with rpartition.
crs_out = 'EPSG:26915'
tifRoot = r"E:\Houston\dem\USGS_dem_dowload\2023\danevang_el_campo"

rasPath = r"E:\Houston\dem\USGS_dem_dowload\2023\danevang_el_campo\data"
rasList = []
outname = []

for filename in os.listdir(rasPath):
    if filename.endswith('.img') or filename.endswith('.tif'):
        rasList.append(os.path.join(rasPath, filename))
        outname.append(filename)
        
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

