from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
import os

import json
from netCDF4 import Dataset
import merge


from time import perf_counter

def pathExists(path):
    total = "."
    for folder in path.split('/')[:-1]:
        total = total + "/" + folder
        if os.path.isdir(total) is False:
            os.mkdir(total)

def downloader(provider, path, container_name, var_list, min_lat, max_lat, min_long, max_long):


    # Start the stopwatch / counter 
    t1_start = perf_counter()  

    #os.mkdir("Prova2/Prova3")

    with open("auth.json") as json_file:
        auth = json.load(json_file)

    if provider == "S3":
        cls = get_driver(Provider.S3)
        driver = cls(auth[provider]["API_LOGIN"], auth[provider]["API_KEY"], region="us-east-1",
                     token=auth[provider]["API_TOKEN"])
    elif provider == "Azure":
        cls = get_driver(Provider.AZURE_BLOBS)
        driver = cls(auth[provider]["API_LOGIN"], auth[provider]["API_KEY"])
    elif provider == "Google":
        cls = get_driver(Provider.GOOGLE_STORAGE)
        driver = cls(auth[provider]["API_LOGIN"], auth[provider]["API_KEY"])


    download_dataset_path = "download/" + path
    #print (download_dataset_path)

    # Se non esiste il path ricrealo
    if not os.path.exists(download_dataset_path):
        os.makedirs(download_dataset_path)

    #print(path+"/"+path.split('/')[1]+".nc")
    #print(path)
    metafile_path = "download/" + path + "/__meta__.nc4"
    #print(metafile_path)

    container = driver.get_container(container_name)
    pathExists('download/' + path)
    meta = driver.get_object(container_name=container_name, object_name=path + "/__meta__.nc4")
    meta.download("download/" + path + "/__meta__.nc4", overwrite_existing=True)

    
    rootgrp = Dataset(metafile_path)


    for variable in rootgrp.variables.values():

        if variable.name in var_list:
            for k in variable.ncattrs():
                if (k == "_cos_split_latitude"):
                    _cos_split_latitude = variable.getncattr(k)
                if (k == "_cos_split_longitude"):
                    _cos_split_longitude = variable.getncattr(k)


    print ("_cos_split_latitude: " + str(_cos_split_latitude))
    print ("_cos_split_longitude: " + str(_cos_split_longitude))

    round_start_lat = int(min_lat / _cos_split_latitude) * _cos_split_latitude
    round_end_lat = int(max_lat / _cos_split_latitude) * _cos_split_latitude + (_cos_split_latitude - 1)

    round_start_lon = int(min_long / _cos_split_longitude) * _cos_split_longitude
    round_end_lon = int(max_long / _cos_split_longitude) * _cos_split_longitude + (_cos_split_longitude - 1)

    folder_lat_start = int(round_start_lat / _cos_split_latitude)
    folder_lat_end = int(round_end_lat / _cos_split_latitude)
    file_lon_start = int(round_start_lon / _cos_split_longitude)
    file_lon_end = int(round_end_lon / _cos_split_longitude)


    for elem in driver.list_container_objects(container):

        # 2 -> Variabile
        # 3 -> Tempo
        # 4 -> Lat
        # 5 -> Long
        if len(elem.name.split('/')) > 3 and elem.name.split('/')[2] in var_list:
                if int(elem.name.split('/')[4]) in [*range(folder_lat_start,folder_lat_end+1)]:
                    #print(elem.name.split('/'))
                    if int(elem.name.split('/')[5].split('.')[0]) in [*range(file_lon_start,file_lon_end+1)]:

                        pathExists('download/' + elem.name)
                        driver.download_object(elem, 'download/' + elem.name, overwrite_existing=True)

   
    file_merged_path = download_dataset_path + "/" + var_list[0] \
                       + "_lat[" + str(round_start_lat) + "-" + str(round_end_lat) \
                       + "]_lon[" + str(round_start_lon) + "-" + str(round_end_lon) + "].nc4"



    # Stop the stopwatch / counter 
    t1_stop = perf_counter() 
  
    


    # Start the stopwatch / counter 
    t2_start = perf_counter()  
    
    merge.merge(path, metafile_path,file_merged_path, var_list, round_start_lat, round_end_lat, round_start_lon, round_end_lon, _cos_split_latitude, _cos_split_longitude)


    print("\n\nDurata downloader in secondi: ", t1_stop-t1_start)

    # Stop the stopwatch / counter 
    t2_stop = perf_counter() 
    print("Durata merge in secondi: ", t2_stop-t2_start)

    


data = "data/rms3_d03_20200506Z1200"
service = "Google"
container = "bucket-nc"
variable = ['temp']
downloader(service, data, container, variable, 540, 972, 0, 888)