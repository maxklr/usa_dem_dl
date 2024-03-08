import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import pandas as pd
import requests
from requests_futures.sessions import FuturesSession
import time
from zipfile import ZipFile
from zipfile import BadZipFile
from os import walk
import shutil



base_url = 'https://tnmaccess.nationalmap.gov/api/v1/products'

# Define the parameters as a dictionary
#bbox = xMin, yMin, xMax, yMax
params = {
    'bbox': '-117.87,33.62,-117.54,33.87',
    'datasets': 'Digital Elevation Model (DEM) 1 meter,National Elevation Dataset (NED) 1/9 arc-second,National Elevation Dataset (NED) 1/3 arc-second',
    'prodFormats': 'TIFF,GeoTIFF,IMG'
}

# MOdify paramas as needed
# params['bbox'] = ''
# params['max'] = ''
# params['prodFormats'] = None

# Make the request
r = requests.get(base_url, params=params)
print(r.status_code)
# Print the URL to check if it's constructed properly
print(r.url)

# Check if request was successful (status code 200)
if r.status_code == 200:
    data = r.json()  # Convert the response content to a Python dictionary
    
    if "items" in data:  # Check if 'items' key exists in the dictionary
        df = pd.DataFrame(data["items"])
        print(df.head())  # Display the first few rows of the dataframe
else:
    print("Failed to retrieve data.")

# Convert items list to dataframe 
# This can be used for logging puropses later
df = pd.DataFrame(data["items"])

# Assert that all rows have url
# Use for error handling later
assert df['downloadURL'].isna().sum() == 0

# Download all tiles from urlList
# Henter alle filer fra liste, filbane hentes fra path og filnavn er alt etter siste "/" i url
# Set DL path
dlPath = r"E:\Houston\dem\USGS_dem_dowload\scripts\test_data3"

# Faster test
# Create a FuturesSession to manage async requests
session = FuturesSession()

# Start download processes for each URL
futures = []
for i, j in zip(df['downloadURL'], df['publicationDate']):
    base_name, _, file_ext = i.rpartition('/')[-1].rpartition('.')
    filepath = os.path.join(dlPath, f"{base_name}_{j}.{file_ext}")
    print(filepath)
    
    future = session.get(i, allow_redirects=True)  # This won't block
    futures.append((future, filepath))

# Now handle the results as they complete
for future, filepath in futures:
    response = future.result()  # This will block until the request is done
    with open(filepath, 'wb') as data:
        data.write(response.content)

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
    dir_path = os.path.join(path, "zipArchive")

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    for f in ziplist:
        try:
            with ZipFile(os.path.join(path, f),"r") as zip_ref:
                zip_ref.extractall(path)
            shutil.move(os.path.join(path, f), os.path.join(path, "zipArchive", f))
        except BadZipFile:
            badZip.append(f)
            pass

# Sorts raster files from non-raster files. 
def sortrasters(rastertuple, path):
    nonraster = [f for f in next(walk(path), (None, None, []))[2]  if not f.endswith(rastertuple)] #List all files that are not rasters
    # Check if list of non rasters is empty
    if not nonraster:
        print("Only raster files")
    else:
        dir_path = os.path.join(path, "ancillayData")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        print("Moving non-rasters to //ancillary data")
        for f in nonraster:
            shutil.move(os.path.join(path, f), os.path.join(path, "ancillaryData"))

unzip(dlPath)
sortrasters(('.img', '.tif'), dlPath)



