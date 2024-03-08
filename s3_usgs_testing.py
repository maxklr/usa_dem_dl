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
import boto3
import xml.etree.ElementTree as ET



# PARSE DIRECTORIES
# Base URL for your bucket
bucket_url = 'https://prd-tnm.s3.amazonaws.com/'
#prefix = 'StagedProducts/Elevation/1m/Projects/'
prefix = 'StagedProducts/Elevation/1m/FullExtentSpatialMetadata'

response = requests.get(bucket_url, params={"prefix": prefix, "delimiter": "/"})
response.raise_for_status()  # Raise an exception if there's an error

# Parse the XML
root = ET.fromstring(response.text)

# Print the "subfolders"
directories = set()
for content in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}CommonPrefixes/{http://s3.amazonaws.com/doc/2006-03-01/}Prefix'):
    subfolder = content.text[len(prefix):].split('/')[0]
    if subfolder not in directories:
        print(subfolder)
        directories.add(subfolder)

# URL to fetch XML content from
url = "https://prd-tnm.s3.amazonaws.com/"

# PARSE FILES IN DIRECTORY
# Base URL for your bucket
bucket_url = 'https://prd-tnm.s3.amazonaws.com/'
prefix = 'StagedProducts/Elevation/1m/FullExtentSpatialMetadata'

response = requests.get(bucket_url, params={"prefix": prefix})
response.raise_for_status()  # Raise an exception if there's an error

# Parse the XML
root = ET.fromstring(response.text)

# Print the contents
for content in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}Key'):
    print(content.text)

# DOWNLOAD A FILE
# Base URL for your bucket and the file key/path
file_url = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/FullExtentSpatialMetadata/FESM_1m.gpkg'

response = requests.get(file_url, stream=True)
response.raise_for_status()  # Raise an exception if there's an error

# Specify where to save the file locally
local_filename = "FESM_1m.gpkg"

# Write the file in chunks to handle large files
with open(local_filename, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192): 
        f.write(chunk)

print(f"File saved as {local_filename}")

# Fetch content
response = requests.get(url)
response.raise_for_status()  # This will raise an exception if there was an error fetching the content

# Parse XML content
root = ET.fromstring(response.content)

for content in root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
    key = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text
    last_modified = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}LastModified').text
    size = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Size').text
    
    # Print out the details (can be adjusted as per requirements)
    print(f"Key: {key}, Last Modified: {last_modified}, Size: {size}")




    first_level_keys = set()  # using a set to avoid duplicates

for content in root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
    key = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text
    
    # Split the key by '/' and get the first level directory or file
    key_parts = key.split('/')
    if len(key_parts) == 1:
        first_level_key = key_parts[0]
    else:
        first_level_key = key_parts[0] + '/'

    if first_level_key not in first_level_keys:
        first_level_keys.add(first_level_key)
        last_modified = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}LastModified').text
        size = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Size').text
        print(f"Key: {first_level_key}, Last Modified: {last_modified}, Size: {size}")



url = "https://prd-tnm.s3.amazonaws.com/JoinSite.py"  # The direct URL to your file

# Fetch content
response = requests.get(url)
response.raise_for_status()  # This will raise an exception if there was an error fetching the content

# Save to a local file
with open('JoinSite.py', 'wb') as f:
    f.write(response.content)

print("File downloaded successfully!")