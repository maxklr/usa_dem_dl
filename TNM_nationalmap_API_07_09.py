import geopandas as gpd
import pandas as pd
import requests, rasterio, os, time
from datetime import datetime
from requests_futures.sessions import FuturesSession
from zipfile import ZipFile
from zipfile import BadZipFile
from rasterio.warp import reproject, Resampling, calculate_default_transform, transform_bounds
from rasterio.enums import ColorInterp
from osgeo import gdal
import numpy as np

gdal.UseExceptions() 

#Reproject files

def prj_raster(rPath,date):
    try:
        base_name, _, file_ext = rPath.rpartition('/')[-1].rpartition('.')
        outD = os.path.join(os.path.dirname(rPath),f'{base_name}_3857.tif')
  
        warp = gdal.Warp(outD,rPath,dstSRS='EPSG:3857')
        warp = None # Closes the files
                
        os.utime(outD, (date, date)) 
        os.remove(rPath)
    except Exception as e:
        print(e)
        pass
                
# Funksjon som Unzipper filer til path og flytter zip filer i mappe zipArchive (bør være tom)
def unzip(path):
    badZip = []
    #list files ending with .zip
    ziplist = [(f,d) for v,f,d in path if f.endswith('zip')]
    # Check for zip files in list
    if not ziplist:
        print("No zip files")
    else:
        print("Running unzip")
    prj = []
    for zfile,date in ziplist:
        try:
            with ZipFile(zfile,"r") as z:
                for f in z.infolist():
                    if f.filename.endswith(tuple(('.img', '.tif'))):
                        outFname = os.path.join(os.path.dirname(ziplist[0][0]), f.filename)
                        with open(outFname, 'wb') as outFile:
                            outFile.write(z.open(f).read())
                        prj.append((outFname,date))
            os.remove(zfile)
        except BadZipFile:
            badZip.append(f)
            pass
        
    for raster,date in prj:
       prj_raster(raster,date)
    
def main(locations):
    try:
        base_url = 'https://tnmaccess.nationalmap.gov/api/v1/products'
        
        dfl = gpd.read_file(locations)
        dfb = gpd.read_file(r'R:/USA/Basins/HucWatersheds.gpkg')
        
        dfb = dfb.sjoin(dfl,how='inner')

        for idx,row in dfb.iterrows():
            huc8 = row['huc8']
            
            xmin,ymin,xmax,ymax = row['geometry'].bounds
            
            # Define the parameters as a dictionary
            params = {
                'polyCode': f'{huc8}',
                'polyType': 'huc8',
                #'bbox': f'{round(xmin,2)},{round(ymin,2)},{round(xmax,2)},{round(ymax,2)}',
                'datasets': 'Digital Elevation Model (DEM) 1 meter,National Elevation Dataset (NED) 1/9 arc-second,National Elevation Dataset (NED) 1/3 arc-second',
                'prodFormats': 'TIFF,GeoTIFF,IMG'
            }

            # Download all tiles from urlList
            # Set DL path
            dlPath = r"R:\USA\DEM"
            aoiPath = rf"R:\USA\aois\{huc8}"

            if not os.path.exists(aoiPath):
                os.mkdir(aoiPath)

            # Calculate the bounding box in EPSG:3857
            with rasterio.Env():
                transform, width, height = calculate_default_transform('EPSG:4326', 'EPSG:3857', width=1, height=1, left=xmin, bottom=ymin, right=xmax, top=ymax)
                bbox3857 = transform_bounds('EPSG:4326', 'EPSG:3857', xmin, ymin, xmax, ymax)
                
            # Make the request
            r = requests.get(base_url, params=params)

            # Check if request was successful (status code 200)
            if r.status_code == 200:
                data = r.json()  # Convert the response content to a Python dictionary
                
                if "items" in data:  # Check if 'items' key exists in the dictionary
                    df = pd.DataFrame(data["items"])
                    #print(df.head())  # Display the first few rows of the dataframe
            else:
                print("Failed to retrieve data.")
                break

            # Convert items list to dataframe 
            # This can be used for logging puropses later
            df = pd.DataFrame(data["items"])

            # Assert that all rows have url
            # Use for error handling later
            assert df['downloadURL'].isna().sum() == 0

            # Faster test
            # Create a FuturesSession to manage async requests
            session = FuturesSession()

            # Start download processes for each URL
            futures = []
            files = []
            for index,row in df.iterrows():
                url = row['downloadURL']
                base_name, _, file_ext = url.rpartition('/')[-1].rpartition('.')
                if 'one_meter' in url or '1M' in url:
                    curDir = dlPath
                elif 'ned19' in url:
                    curDir = os.path.join(dlPath, '1_9_arc-second') 
                elif 'USGS_13' in url:
                    curDir = os.path.join(dlPath, '1_3_arc-second')
                
                bname = f"{base_name}_3857.tif"
                filepath = os.path.join(curDir, bname)
                checkpath = os.path.join(curDir, bname)
                files.append(bname)
                
                nDate = datetime.strptime(df['lastUpdated'][0][:10],"%Y-%m-%d")
                
                if os.path.exists(checkpath):
                    cDate = datetime.fromtimestamp(os.path.getmtime(checkpath))
                    if nDate <= cDate:
                        continue
                
                if not os.path.exists(curDir):
                    os.mkdir(curDir)
                
                future = session.get(url, allow_redirects=True)  # This won't block
                futures.append((future,filepath,time.mktime(nDate.timetuple())))

            if futures:
                # Now handle the results as they complete
                for future, filepath, date in futures:
                    response = future.result()  # This will block until the request is done
                    with open(filepath, 'wb') as data:
                        data.write(response.content)
                    if not filepath.endswith('.zip'):
                        prj_raster(filepath,date)

                unzip(futures)
                
                #Function to create three vrts for the different resolutions for the aoi
                dirpaths = ['','1_9_arc-second', '1_3_arc-second']
                tiflist = [] 
                vrt_path = os.path.join(aoiPath,f'dem.vrt')
                for dirpath in dirpaths[::-1]: #Make sure the lowest resolution is first
                    dirpath = os.path.join(dlPath,dirpath)
                    for name in os.listdir(dirpath):
                        if name in files:
                            tiflist.append(os.path.join(dirpath,name))
                if tiflist:
                    gdal.BuildVRT(vrt_path, tiflist, options=gdal.BuildVRTOptions(resolution='highest',  resampleAlg='nearest',outputBounds=bbox3857))
    except Exception as e:
        print(e)
if __name__ == "__main__":
    locations = r'R:\USA\locations.gpkg'
    main(locations)