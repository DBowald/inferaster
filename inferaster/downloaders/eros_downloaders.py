import json
from typing import List
import requests
import sys
import time
import argparse
import threading
import datetime
import re
import glob
import shutil
import zipfile, os
import yaml
import rasterio
import copy
from data_trawler.utils.geotiff import Geotiff


from data_trawler.downloaders.data_downloader import DataDownloader, Entry

class ErosDownloader(DataDownloader):
    """
    Abstract base class for a downloader. Only the abstract methods + login() and process_data() should be overriden when subclassed.
    You must implement the two abstract classes (get_image_data_list() and download_one()) to sub class. 
    Parameters
    ----------
    metaclass : _type_, optional
        _description_, by default abc.ABCMeta
    """
    def __init__(self, parsed_config:dict) -> None:
        """

        Parameters
        ----------
        parsed_config : dict
            parsed config, from utils/config_files. Obtained using utils/parse_config().
        """
        super().__init__(parsed_config)

        self.bbox = self.bounding_box
        self.datapath = parsed_config["datapath"]
        self.tiff_dir = parsed_config["full_tiff_dir"]
        self.username=parsed_config['username']
        self.password=parsed_config['password']
        
        self.start_date=parsed_config['time_range']['start_date']
        self.end_date=parsed_config['time_range']['end_date']
        self.max_downloads=parsed_config['max_downloads']
        self.datasetName=parsed_config['datasets']

        self.serviceUrl = "https://m2m.cr.usgs.gov/api/api/json/stable/"
        self.apiKey = self.login()

        self.spatialFilter =  {'filterType' : "mbr",
                        'lowerLeft' : {'latitude' : self.bbox.sw.lat, 'longitude' : self.bbox.sw.lon},
                        'upperRight' : { 'latitude' : self.bbox.ne.lat, 'longitude' : self.bbox.ne.lon}}
        self.temporalFilter = {'start' : self.start_date , 'end' : self.end_date}

    
    def convert_search_results_to_json(self):
        raise NotImplementedError
    
    def get_image_data_list(self, max_items: int) -> List[Entry]:
        """ 
        Forms a list of metadata entries for download. Check out the Entry docstring for info on all the parameters an Entry should contain.
        In addition, be sure to include as much metadata you can in JSON form in the full_metadata field, especially everything you will need
        to download a geotiff in the download_one function. These functions are decoupled so you can do error checking on the metadata, which
        is relatively inexpensive, before you download a bunch of wrong images, or images with incorrectly formatted metadata.

        Parameters
        ----------
        max_items : int
            Maximum number of entries for download.

        Returns
        -------
        List[dict]
            List of entries (each a dictionary). Each entry should have all the metadata you need to download
            one geotiff from your dataset, as well as the shared required_metadata values. Check out the class
            Entry docstring for more details.

        """
        all_entries = []
        api_bbox_search_results = []
        datasets = self.datasetFilter()
        for dataset in datasets:
            scenes = self.sceneFilter(dataset)
            if scenes[0] is not None:
                api_bbox_search_results = api_bbox_search_results + (self.createMeta(scenes, dataset["collectionName"]))
        # TODO very likely supposed to be here but not sure
            if len(api_bbox_search_results) > max_items:
                break
        for each_result in api_bbox_search_results:

            entry = {}
            name = each_result["name"]
            uid = each_result["uid"]
            # the relpath is currently unfinished until it is downloaded
            relpath = each_result["relpath"]
            required_metadata = {}
            required_metadata["channels"] = each_result["required_metadata"]["channels"]
            required_metadata["dataset"] = each_result["required_metadata"]["dataset"]
            required_metadata["gsd_m"] = each_result["required_metadata"]["gsd_m"]
            required_metadata["date_collected"] = each_result["required_metadata"]["date_collected"]
            full_metadata = each_result["full_metadata"]
            # full_metadata = self.convert_search_results_to_json(each_result)

            entry = Entry(name, uid, relpath, required_metadata, full_metadata)
            all_entries.append(entry)
        print(len(all_entries))
        return all_entries
    
    def download_from_my_cool_api(self, relevant_params):
        raise NotImplementedError
    
    def cleanup(self, outputpath):
        raise NotImplementedError
    
    def download_one(self, entry: dict) -> None:
        """
        Download one tiff, using the assembled entries from get_image_data_list. This is also where all unzipping,
        sorting, cleanup, converting to geotiff, etc should happen, such that your end result is a 
        georeferenced geotiff image in the ~/data/geotiffs/tiffs_to_chip folder.

        Parameters
        ----------
        entry : dict
            See Entry docstring for details.
        """
        pathzip = self.uniqueFile(self.datapath)
        pathtiff = self.uniqueFile(self.datapath)
        url = entry.full_metadata["url"]
        try:        
            response = requests.get(url, stream=True)
            disposition = response.headers['content-disposition']
            filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
            print(f"Downloading {filename} ...\n")
            if pathzip != "" and pathzip[-1] != "/":
                filename = "/" + filename
            open(pathzip+filename, 'wb').write(response.content)
            print(f"Downloaded {filename}\n")
        except Exception as e:
            print(f"Failed to download from {url}. Will try to re-download.")
            print(e)
        self.unzipping(pathzip, pathtiff)
        self.insertingMeta(entry, pathtiff)
        self.deleteFile(pathzip)
        self.deleteFile(pathtiff)
        os.rmdir(pathzip)
        os.rmdir(pathtiff)
        return


        # outputpath = os.path.join(self.datapath, entry.relpath)
        # relevant_dl_params = entry.full_metadata["my_api_relevant_param"]
        # self.download_from_my_cool_api(outputpath, relevant_dl_params)
        # self.cleanup(outputpath)
        # return

    def data_process(self):
        """
        Overrideable function to further process data after download process is done, if needed. It also logs out of the API after downloading all the tiffs.
        """
        # Logout so the API Key cannot be used anymore
        endpoint = "logout"  
        if self.sendRequest(self.serviceUrl + endpoint, None, self.apiKey) == None:        
            print("Logged Out\n\n")
        else:
            print("Logout Failed\n\n")
        # print("data_process function not overwritten in child class; no preprocessing done.")

    def login(self):
        """
        Overrideable function to do a login, if needed by the target API.
        """        
        payload = {'username' : self.username , 'password' : self.password }
        apiKey = self.sendRequest(self.serviceUrl + "login", payload)
        print("\nSigned in...\n")
        return apiKey

    def sendRequest(self, url: str, data: dict, apiKey: str = None):  
        """ 
        Sends a request to the API to interact with the API or retrieve related information from the site. If an error occurs, it will print the error and exit the script.

        Parameters
        ----------
        url : str
            The url to the API call we want
        data : dict
            A dictionary of data that is either required for the API call or to filter out searches
        apiKey : str
            The token that allows us to access the API

        Returns
        -------
        Any
            Its return value depends on what is being requested and what information is given during the function call.
        """
        json_data = json.dumps(data)
        
        if apiKey == None:
            response = requests.post(url, json_data)
        else:
            headers = {'X-Auth-Token': apiKey}              
            response = requests.post(url, json_data, headers = headers)    
        
        try:
            httpStatusCode = response.status_code 
            if response == None:
                print("No output from service")
                sys.exit()
            output = json.loads(response.text)	
            if output['errorCode'] != None:
                print(output['errorCode'], "- ", output['errorMessage'])
                sys.exit()
            if  httpStatusCode == 404:
                print("404 Not Found")
                sys.exit()
            elif httpStatusCode == 401: 
                print("401 Unauthorized")
                sys.exit()
            elif httpStatusCode == 400:
                print("Error Code", httpStatusCode)
                sys.exit()
        except Exception as e: 
            response.close()
            print(e)
            sys.exit()
        response.close()
        return output['data']
    
    def datasetFilter(self):
        """ 
        Filters out all unnecessary datasets and only keep the datasets that potentially have IR data.

        Returns
        -------
        List[dict]
            Returns a list of dictionary where each dictionary contains the information for each dataset.
        """
        if self.datasetName == "None":       # when you don't know which datasets you want
            datasetPayload = {'spatialFilter' : self.spatialFilter,
                            'temporalFilter' : self.temporalFilter}
        else:                           # when you know the datasets you want
            datasetPayload = { 'datasetName': self.datasetName,
                            'spatialFilter' : self.spatialFilter,
                            'temporalFilter' : self.temporalFilter}
        print("Searching datasets...\n")
        allDatasets = self.sendRequest(self.serviceUrl + "dataset-search", datasetPayload, self.apiKey)
        allDatasets.reverse()     # reversing the list so we can get the most recent data when filtering
        datasets = []
        for dataset in allDatasets:
            try:    # these filters either remove datasets we don't need or don't have access to
                if dataset['catalogs'].count('HDDS') == 0:   # remove all HDDS datasets
                    if self.filterAbstract(datasets, dataset['abstractText']):   # remove repeated datasets
                        if self.filterString(dataset, 'MODIS'):                  # remove MODIS datasets
                            if self.filterString(dataset, 'ECOSTRESS'):          # remove ECOSTRESS datasets
                                if self.filterString(dataset, 'NASA'):           # remove NASA datasets
                                    datasets.append(dataset)
            except TypeError:           # datasets with None type
                continue
        datasets.reverse()         # reversing the list back in order for easier debugging
        return datasets
    
    def sceneFilter(self, dataset: dict):
        """ 
        Filters out all unnecessary scenes and only keep the scenes that potentially have IR data.

        Parameters
        ----------
        dataset : dict
            The dataset where we potentially pull the scenes from

        Returns
        -------
        List[list[dict], list[dict]]
            Returns a list that contains two list, where each list is a dictionary. The list at index 0 are dictionary where each dictionary contain information to download the tiffs. The list at index 1 are dictionary where each dictionary contains metadata for some tiff that satisfies the constraints. (the lists do not directly correspond with each other ex. the seventh element of each list may not point to the same tiff)
        """
        scenesPayload = {'datasetName' : dataset['datasetAlias'], 
                                'maxResults' : self.max_downloads,
                                'startingNumber' : 1, 
                                'sceneFilter' : {
                                                'spatialFilter' : self.spatialFilter,
                                                'acquisitionFilter' : self.temporalFilter
                                                }}
        print("Searching scenes...\n\n")
        scenes = self.sendRequest(self.serviceUrl + "scene-search", scenesPayload, self.apiKey)
        # If we found nothing, end the iteration
        if scenes['recordsReturned'] > 0:
            # Aggregate a list of scene ids, these are what we will pass to api to find the urls for downloading
            sceneIds = []
            for result in scenes['results']:
                # Add this scene to the list I would like to download
                sceneIds.append(result['entityId'])
            # we need to further filter each individual scenes out by their metadata
            sceneMeta = []
            for scene in sceneIds:
                sceneMetaPayload = {
                    'datasetName' : dataset['datasetAlias'],
                    'entityId' : scene
                }
                meta = self.sendRequest(self.serviceUrl + "scene-metadata", sceneMetaPayload, self.apiKey)
                sceneMeta.append(meta)
            downloadIds = self.filterIR(sceneMeta, sceneIds)
            # Find the download options for these scenes, 
            optionPayload = {'datasetName' : dataset['datasetAlias'], 'entityIds' : downloadIds}                
            downloadOptions = self.sendRequest(self.serviceUrl + "download-options", optionPayload, self.apiKey)
            # Narrow that list of scenes down to avaible ones and only the ids of the download options we want 
            wantedDownloads = []
            try:
                for product in downloadOptions:
                    # Make sure the product is available for this scene
                    if product['available'] == True and not ("Compressed" in product["productName"]):
                        wantedDownloads.append({'entityId' : product['entityId'],
                                        'productId' : product['id']})
            except TypeError:           # ignore datasets with None type
                pass
                # continue
            except NameError:           # ignore when no scenes were found with this name
                pass
                # continue
            return [wantedDownloads, sceneMeta]
        return [None, None]
    
    def createMeta(self, scenes: list, dataset: str):
        """ 
        Given the information of all the scenes that we want to download and the metadata for all the scenes, this function matches the downloadable scenes with it's correct metadata.

        Parameters
        ----------
        scenes : list[list[dict], list[dict]]
            A list of list of dictionary where the list on index 0 contains all the scenes and the list on index 1 contains all the metadata.
        dataset : str
            A string of the name of the dataset the scenes will come from

        Returns
        -------
        List[dict]
            Returns a list of dictionary where each element contains all the metadata needed for each scene.
        """
        wantedDownloads = scenes[0]
        sceneMeta = scenes[1]
        downloadMeta = []
        for id in wantedDownloads:
            for entity in sceneMeta:
                if entity["entityId"] == id["entityId"]:
                    downloadMeta.append(entity)
        # you have the metadata, now change it to the correct form
        downloadMetadata = []
        for meta in downloadMeta:
            metaDic = {}
            name = ""
            date_collected = ""
            for i in meta["metadata"]:
                if i["fieldName"] == "Aquisition Date":
                    date_collected = i["value"]
                if i["fieldName"] == "Ending Date":
                    date_collected = i["value"]
                if i["fieldName"] == "Image Name":
                    name = i["value"]
            # add name: str
            metaDic["name"] = name
            # add uid: str
            metaDic["uid"] = meta["entityId"]
            # add relpath: str
            metaDic["relpath"] = self.tiff_dir+"/"+name+".tiff"
            # add required_metadata: dict
                # since you're only working with high_res right now, you can take shortcut but generalize the
                # code to all datasets when you have time
            # hard coded for high_res_ortho only
            channels = {"0": "Red", "1": "Green", "2": "Blue", "3": "Color Near-Infrared"}
            gsd_m = .3048       # hard coded for high_res_ortho only
            # found the dataset and date_collected in the for loop above
            required_metadata = {"channels": channels, "dataset": dataset, "gsd_m": gsd_m, "date_collected": date_collected}
            metaDic["required_metadata"] = required_metadata
            # add full_metadata: dict
            metaDic["full_metadata"] = meta
            downloadMetadata.append(metaDic)
        if wantedDownloads:
            for eachDownload in wantedDownloads:
                # set a label for the download request
                label = "download-sample"
                # Create the payload with the ids found previously
                urlPayload = {'downloads' : [eachDownload],
                                'label' : label,} 

                # Call the api to get the direct download urls
                requestResults = self.sendRequest(self.serviceUrl + "download-request", urlPayload, self.apiKey)
                # requestResults["preparingDownloads"][0]["entityId"] = eachDownload["entityId"]
                for meta in downloadMetadata:
                    if meta["uid"] == eachDownload["entityId"]:
                        meta["full_metadata"]["url"] = requestResults["preparingDownloads"][0]["url"]
        else:
            print("No Download options fit your criteria. Double check the product name you are looking for.")
        return downloadMetadata
    
    def unzipping(self, pathzip: str, pathtiff: str):
        """ 
        Unzips all the files from pathzip and move them to "~/data/geotiffs/leftovers". Move all the tiffs in pathzip to pathtiff.

        Parameters
        ----------
        pathzip : str
            The path to the directory where all the zip files are located
        pathtiff : str
            The path to the directory where we want to store all the tiffs
        """
        print("start unzipping")
        direc = pathzip
        direc = os.path.expanduser(direc)
        files = os.listdir(direc)
        for file in files:
            sourceZip = direc+"/"+file            # path to the zip
            unzipDir = "~/data/geotiffs/leftovers"          # directory to open the zip in
            unzipDir = os.path.expanduser(unzipDir)
            destination = pathtiff
            destination = os.path.expanduser(destination)
            # filter and find all the .tiff files
            with zipfile.ZipFile(sourceZip,"r") as zip_ref:
                zip_ref.extractall(unzipDir)
                for zip in zip_ref.filelist:
                    unzipPath = destination+"/"+zip.filename
                    ext = os.path.splitext(unzipPath)[-1].lower()
                    if ext == ".tif":
                        zip_ref.extract(zip, path=destination)
            # move all the files in subdirectories to the main directory
            for root, dirs, files in os.walk(destination, topdown=False):
                for file in files:
                    try:
                        shutil.move(os.path.join(root, file), destination)
                    except OSError:
                        pass
            # delete all the unnecessary subdirectories
            destinationDir = [ f.path for f in os.scandir(destination) if f.is_dir() ]
            for i in destinationDir:
                shutil.rmtree(i)

    def insertingMeta(self, entry: dict, pathtiff: str):
        """ 
        Inserting the metadata from entry into the file in the path pathtiff.

        Parameters
        ----------
        entry : dict
            see Entry docstring for detail
        pathtiff : str
            The path to the file where we want to store the metadata.
        """
        print("separating the IR and RGB bands")
        destination = pathtiff       # directory where the .tiff are located
        destination = os.path.expanduser(destination)
        files = os.listdir(destination)
        folder = self.datapath+"/"+self.tiff_dir    # need to change this file into something else(hide the processes of unzipping and extracting)!!!!!!!!!!!!!!!!
        folder = os.path.expanduser(folder)
        for file in files:
            src = rasterio.open(destination+'/'+file, 'r+')
            srcread = src.read()
            rgb_band = srcread[0:3]
            ir_band = srcread[3]
            # get the output name
            fileName = os.path.splitext(file)[0]
            rgb_name = fileName + "_" + "rgb" + ".tiff"
            ir_name = fileName + "_" + "ir" + ".tiff"
            rgb_out_img = os.path.join(folder, rgb_name)
            ir_out_img = os.path.join(folder, ir_name)
            # get the metadata
            rgbMeta = None
            irMeta = None
            # metadataName = os.path.splitext(os.path.basename(entry.relpath))[0]
            # if fileName == metadataName:
            rgbMeta = copy.deepcopy(entry)
            # rgbMeta.name = "rgb_"+rgbMeta.name
            rgbMeta.name = fileName + "_" + "rgb"
            rgbMeta.relpath = os.path.dirname(rgbMeta.relpath)+"/"+rgb_name
            rgbMeta = self.entryToDict(rgbMeta)
            # rgbMeta = json.dumps(rgbMeta)
            irMeta = copy.deepcopy(entry)
            # irMeta.name = "ir_"+irMeta.name
            irMeta.name = fileName + "_" + "ir"
            irMeta.relpath = os.path.dirname(irMeta.relpath)+"/"+ir_name
            irMeta = self.entryToDict(irMeta)
            # irMeta = json.dumps(irMeta)
            # editting file creating data
            rgb_out_meta = src.meta.copy()
            rgb_out_meta.update({"count": 3})
            ir_out_meta = src.meta.copy()
            ir_out_meta.update({"count": 1})
            # save the clipped raster to disk
            with rasterio.open(rgb_out_img, "w", **rgb_out_meta) as dest:
                dest.write(rgb_band)
                # dest.update_tags(**rgbMeta)
            with rasterio.open(ir_out_img, "w", **ir_out_meta) as dest:
                dest.write(ir_band, 1)
                # dest.update_tags(**irMeta)
            src.close()

    def filterAbstract(self, dataset: dict, filter: str):
        """ 
        Determine if any of element in dataset contain the filter. If no dataset's "abstractText" equals the filter,  keep it, else remove it.

        Parameters
        ----------
        dataset : list
            A list of dictionary where each element represents a dataset
        filter : str
            A string used to filter the dataset

        Returns
        -------
        Bool
            Returns True if the dataset does not contain the filter, False if the dataset does contain the filter.
        """
        for elem in dataset:
            if elem['abstractText'] == filter:
                return False
        return True

    def filterString(self, dataset: list, filter: str):
        """ 
        Determine if any of element in dataset contain the filter. If no dataset's "abstractText" equals the filter,  keep it, else remove it.

        Parameters
        ----------
        dataset : list
            A list of dictionary where each element represents a dataset
        filter : str
            A string used to filter the dataset

        Returns
        -------
        Bool
            Returns True if no elements of the dataset does not contain the filter, False if some element of the dataset does contain the filter.
        """
        try:
            if dataset['keywords'].find(filter) == -1:
                return True
            return False
        except AttributeError:        # to catch None types
            return True
        
    def filterIR(self, sceneMeta: list, sceneIds: list):
        """ 
        Returns a list of any scenes that contain IR images through certain keywords.

        Parameters
        ----------
        sceneMeta : list
            A list of dictionary where each element contains all the metadata for a scene
        sceneIds : list
            A list of all the unique IDs that correspond with the specific scenes

        Returns
        -------
        List
            Returns a list of all the entityIds corresponding with the scenes that we want to download
        """
        downloadIds = []
        # these are the current known ir filter, will update this list accordingly
        for scene in range(len(sceneMeta)):
            for metadata in sceneMeta[scene]['metadata']:
                if (metadata['fieldName'] == 'Sensor Type' and
                    metadata['value'] == 'Color Near-Infrared'):
                    downloadIds.append(sceneIds[scene])
                elif (metadata['fieldName'] == 'Sensor Type' and
                    metadata['value'] == 'CNIR'):
                    downloadIds.append(sceneIds[scene])
                elif (metadata['fieldName'] == 'Film Type' and
                    metadata['value'] == 'Color Infrared'):
                    downloadIds.append(sceneIds[scene])
                elif (metadata['fieldName'] == 'Image Type' and
                    metadata['value'] == 12):
                    downloadIds.append(sceneIds[scene])
                elif (metadata['fieldName'] == 'Image Type' and
                    metadata['value'] == 13):
                    downloadIds.append(sceneIds[scene])
                elif (metadata['fieldName'] == 'Sensor Type' and
                    metadata['value'] == 'Multispectral'):
                    if (metadata['fieldName'] == 'Resolution' and
                    metadata['value'] == 10):
                        downloadIds.append(sceneIds[scene])
                elif (metadata['fieldName'] == 'Sensor Type' and
                    metadata['value'] == 'Multispectral'):
                    if (metadata['fieldName'] == 'Resolution' and
                    metadata['value'] == 20):
                        downloadIds.append(sceneIds[scene])
        return downloadIds

    def uniqueFile(self, currentDir: str):
        """ 
        Creates a directory with a unique name in the currentDir

        Parameters
        ----------
        currentDir : str
            A string of the directory we want the new directory to be created at

        Returns
        -------
        Str
            Returns the name of the new directory that was just created
        """
        path=currentDir+"/"+"tmp_ir"
        counter = 1
        while True:
            try:
                newPath = os.path.expanduser(path)+str(counter)
                os.mkdir(newPath)
                return newPath
            except OSError as e:
                print(e)
                counter += 1
                continue

    def deleteFile(self, folder: str):
        """ 
        Deletes all the files within the directory folder.

        Parameters
        ----------
        folder : str
            A string of the directory where all the files we want to delete are located
        """
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

    def entryToDict(self, entry: Entry):
        """ 
        Take an Entry object and convert it into a dictionary

        Parameters
        ----------
        entry : Entry
            See the Entry class for more details

        Returns
        -------
        Dict
            Returns a dictionary created from an Entry object.
        """
        dict = {}
        dict["name"] = entry.name
        dict["uid"] = entry.uid
        dict["relpath"] = entry.relpath
        dict["required_metadata"] = entry.required_metadata
        dict["full_metadata"] = entry.full_metadata
        return dict

    def write_to_metadata_json(self, entry: Entry):#name, uid, relpath, required_metadata, full_metadata):
        """ Function to update a collection in the metadata json. A collection represents a group of downloaded tiffs from
        the same survey, that therefore all have the same metadata. Every tiff should have an associated collection for
        standarization purposes, even if there is only one tiff in that collection.

        Args:
            col_name (str): Human readable name to represent the collection
            col_uid (str): Unique ID for the collection, used as key in metadata json. Ideally uses the same ID as the
                source dataset, if it contains a unique ID field. 
            dataset_name (str): Name of source dataset; e.g. maxar, eros, aviris
            full_metadata (dict): A python dictionary in JSON format 
        """        
        col_update_rgb = {}
        col_update_ir = {}
        col_update_rgb["name"] = entry.name + "_rgb"
        col_update_ir["name"] = entry.name + "_ir"
        col_update_rgb["uid"] = entry.uid
        col_update_ir["uid"] = entry.uid
        col_update_rgb["relpath"] = os.path.dirname(entry.relpath)+"/"+col_update_rgb["name"]+".tiff"
        col_update_ir["relpath"] = os.path.dirname(entry.relpath)+"/"+col_update_ir["name"]+".tiff"
        Entry.check_required_metadata(entry.required_metadata)
        col_update_rgb["required_metadata"] = entry.required_metadata
        col_update_ir["required_metadata"] = entry.required_metadata
        col_update_rgb["full_metadata"] = entry.full_metadata
        col_update_ir["full_metadata"] = entry.full_metadata
        self.metadata_json["collections"][entry.uid] = col_update_rgb
        self.metadata_json["collections"][entry.uid] = col_update_ir

        full_path_rgb = self.get_tiff_path(entry.uid, col_update_rgb["name"])
        full_path_ir = self.get_tiff_path(entry.uid, col_update_ir["name"])

        gtiff_rgb = Geotiff(full_path_rgb)
        gtiff_ir = Geotiff(full_path_ir)
        gtiff_rgb.write_tags(col_update_rgb)
        gtiff_ir.write_tags(col_update_ir)
        gtiff_rgb.close()
        gtiff_ir.close()


class ErosHyperionDownloader(DataDownloader):
    """
    Abstract base class for a downloader. Only the abstract methods + login() and process_data() should be overriden when subclassed.
    You must implement the two abstract classes (get_image_data_list() and download_one()) to sub class. 
    Parameters
    ----------
    metaclass : _type_, optional
        _description_, by default abc.ABCMeta
    """
    def __init__(self, parsed_config:dict) -> None:
        """

        Parameters
        ----------
        parsed_config : dict
            parsed config, from utils/config_files. Obtained using utils/parse_config().
        """
        super().__init__(parsed_config)
