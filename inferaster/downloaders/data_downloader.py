import abc
import json
import os
import requests
from typing import List

from inferaster.utils.geo_shapes import WgsBBox, WgsPoint, GeoPoint, GeoBBox
from inferaster.utils.geotiff import Geotiff


class Entry():
    def __init__(self, name:str, uid:str, relpath:str, required_metadata:dict, full_metadata:dict) -> None:
        """
        Class representing one "entry" containing all metadata needed to download one tiff.

        Parameters
        ----------
        name : str
            Human understandable string for the filename.
        uid : str
            Guaranteed unique ID string, in case name is not unique. 
            Should match dataset image ID if provided.
        relpath : str
            Relative path to the geotiff, relative from within the datapath folder.
            For example, if the path to my tiff is ~/data/geotiff/tiffs_to_chip/panoramic_SWIR_0.tiff, and my datpath
            is ~/data/geotiff, then my relpath is tiffs_to_chip/panoramic_SWIR_0.tiff

        required_metadata : dict
            All information in common and needed to be known across datasets, Currently, this includes:
                channels - dictionary mapping channel number to band identifier; 
                    e.g. {\"0\":\"R\", \"1\":\"G\", \"2\":\"B\"}, {\"0\":\"mag\"}
                dataset - string representing the source dataset; e.g. maxar, aviris, eros.
                gsd_m - float representing the ground sample density in meters (ground level meters per pixel)
                    e.g. 200.0, 157.888967
                date_collected - date string representing the date and time (if available) the image was taken.
        full_metadata : dict
            Entire metadata provided, wrangled into dictionary form. Should include everything you're given,
            unless the data is prohibitively large.
        """
        self.name = name
        self.uid = uid
        self.relpath = relpath
        self.required_metadata = required_metadata
        self.full_metadata = full_metadata
    
    @staticmethod
    def check_required_metadata(required_metadata:dict):
        """
        Validates that the correct data in the correct format have been included; check out Entry for all
        the values that should be included.

        Parameters
        ----------
        required_metadata : dict
            Dictionary of all required metadata for an entry. Check out the docstring in __init__ for more
            details.

        Raises
        ------
        KeyError
            If you get this, either an entry doesn't exist that should, OR you have the correct
            entry, but it's the wrong type.
        """
        if "channels" not in required_metadata:
            raise KeyError("\"channels\" key required in required_metadata dictionary. Valid value should be a dict (dictionary), \n" +\
                           "mapping channel number to band identifier; e.g. {\"0\":\"R\", \"1\":\"G\", \"2\":\"B\"}")
        elif type(required_metadata["channels"]) != type({}):
            raise KeyError("\"channels\" key must be a dictionary. Valid value should be a dict (dictionary), \n" +\
                           "mapping channel number to band identifier. \n" + \
                            "Examples: {\"0\":\"R\", \"1\":\"G\", \"2\":\"B\", \"3\":\"SWIR\"} (for a VisSWIR)")

        if "dataset" not in required_metadata:
            raise KeyError("\"dataset\" key required in required_metadata dictionary. Valid value should  be a string, and \n" + \
                           "represent the source dataset the tiff came from. \n" + \
                           "Examples: maxar, aviris, eros")
        
        if "gsd_m" not in required_metadata:
            raise KeyError("\"gsd_m\" key required in required_metadata dictionary. Valid value should be a float, representing\n" +\
                           "the ground sample density in meters (ground level meters per pixel).\n" + \
                            "Examples: 200.0, 157.888967")

        if "date_collected" not in required_metadata:
            raise KeyError()

class DataDownloader(metaclass=abc.ABCMeta):
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
        self.config = parsed_config
        self.bounding_box = parsed_config["bounding_box"]
        self.datapath = parsed_config["datapath"]
        self.full_tiff_dir = parsed_config["full_tiff_dir"]
        self.metadata_json = self.read_metadata_json()

    @abc.abstractmethod
    def get_image_data_list(self, max_items:int) -> List[Entry]:
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
        pass
        

    @abc.abstractmethod
    def download_one(self, entry:dict) -> None:
        """
        Download one tiff, using the assembled entries from get_image_data_list. This is also where all unzipping,
        sorting, cleanup, converting to geotiff, etc should happen, such that your end result is a 
        georeferenced geotiff image in the ~/data/geotiffs/tiffs_to_chip folder.

        Parameters
        ----------
        entry : dict
            See Entry docstring for details.
        """
        pass

    def download(self, max_items=3, skip_existing=True):
        """
        This is the parent download loop. Subclasses should not override this method, but instead implement
        abstract methods get_image_data_list() and download_one() to properly conform and use this method.
        Creates a list of images to download, then downloads one at a time.

        Parameters
        ----------
        max_items : int, optional
            Max number of tiffs to download, by default 3
        skip_existing : bool, optional
            If true, do not download existing images; if false, overwrite existing images. By default True
        """
        #TODO maybe this should be a generator
        entries_to_dl = self.get_image_data_list(max_items)
        for each_entry in entries_to_dl:

            # Skip downloading tiff if already downloaded and skip_existing=True
            if skip_existing:
                path = os.path.join(self.datapath, each_entry.relpath)
                if os.path.exists(path):
                    print("{} already exists, skip_existing is True; skipping download".format(path))
                    continue
            try:
                self.download_one(each_entry)
                Entry.check_required_metadata(each_entry.required_metadata)
                self.write_to_metadata_json(each_entry)
            except requests.HTTPError as http_err:
                print("{} has failed to download: {}".format(http_err))
            except Exception as e:
                print("ERROR: ", e)
                continue
        self.data_process()
        self.save_updated_metadata_json()

    def data_process(self):
        """
        Overrideable function to further process data after download process is done, if needed.
        """
        print("data_process function not overwritten in child class; no preprocessing done.")

    def login(self):
        """
        Overrideable function to do a login, if needed by the target API.
        """
        print("No login function specefied in child class; no login occured.")
    
    #@final
    def read_metadata_json(self) -> dict:
        """
        Loads in metadata json in python disctionary form.

        Returns
        -------
        dict
            Python dictionary of metadata json.
        """
        metadata_path = os.path.join(self.datapath, "metadata.json")
        if not os.path.exists(metadata_path):
            with open(metadata_path, 'w') as new_metafp:
                json.dump({"collections": {}, "paths": {}}, new_metafp)

        with open(metadata_path, 'r') as metafp:
            metadata_json = json.load(metafp)
        return metadata_json
    
        
    #@final
    def save_updated_metadata_json(self) -> None:
        """
        Saves the metadata json dictionary to file.
        """
        metadata_path = os.path.join(self.datapath, "metadata.json")
        with open(metadata_path, 'w') as metafp_write:
            json.dump(self.metadata_json, metafp_write)
    
        
    def get_tiff_path(self, uid, name):
        full_tiffs_path = os.path.join(self.datapath, self.full_tiff_dir, name + ".tiff")
        return full_tiffs_path
 
    #@final
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
        col_update = {}
        col_update["name"] = entry.name
        col_update["uid"] = entry.uid
        col_update["relpath"] = entry.relpath
        Entry.check_required_metadata(entry.required_metadata)
        col_update["required_metadata"] = entry.required_metadata
        col_update["full_metadata"] = entry.full_metadata
        self.metadata_json["collections"][entry.uid] = col_update

        full_path = self.get_tiff_path(entry.uid, entry.name)

        gtiff = Geotiff(full_path)
        gtiff.write_tags(col_update)
        gtiff.close()
