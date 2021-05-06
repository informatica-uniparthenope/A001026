from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
import os
import json

from time import perf_counter 

def uploader(provider, path, container_name):

    # Start the stopwatch / counter 
    t1_start = perf_counter()  

    with open("auth.json") as json_file:
        auth = json.load(json_file)


    if provider == "S3":
        cls = get_driver(Provider.S3)
        driver = cls(auth[provider]["API_LOGIN"], auth[provider]["API_KEY"], region="us-east-1", token=auth[provider]["API_TOKEN"])
    elif provider == "Azure":
        cls = get_driver(Provider.AZURE_BLOBS)
        driver = cls(auth[provider]["API_LOGIN"], auth[provider]["API_KEY"])
    elif provider == "Google":
        cls = get_driver(Provider.GOOGLE_STORAGE)
        driver = cls(auth[provider]["API_LOGIN"], auth[provider]["API_KEY"])


    container = driver.get_container(container_name=container_name)
    extra = {'meta_data': {'owner': 'myuser', 'created': '2020-02-2'}}

    for r, d, f in os.walk(path):
        for file in f:
            with open(os.path.join(r, file), 'rb') as iterator:
                obj = driver.upload_object_via_stream(iterator=iterator,
                                                      container=container,
                                                      object_name=os.path.join(r, file).replace("\\", "/"),
                                                      extra=extra)


    # Stop the stopwatch / counter 
    t1_stop = perf_counter() 
  
    print("Durata uploader in secondi: ", t1_stop-t1_start)


data = "data/rms3_d03_20200506Z1200"
service = "Google"
container = "bucket-nc"
uploader(service, data , container)
