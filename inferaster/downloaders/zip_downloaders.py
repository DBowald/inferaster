#from types import List
import os
import glob
import shutil
import json

from inferaster.downloaders.data_downloader import DataDownloader, Entry
from zipfile import ZipFile
from inferaster.utils.geotiff import Geotiff

from geopy.distance import geodesic


def json_parser(in_str: str) -> dict:
    return json.loads(in_str)

def recursive_parser(in_str: str) -> dict:
    data = json.load(in_str)
    return_dict = {}
    for key in data:
        try:
            sub_dict = json_parser(data[key])
            return_dict[key] = sub_dict
        except:
            return_dict[key] = data[key]
            #print("End of branch")

    return return_dict


def metaDataParser(path: str="/Users/low/ATRC/GADDS/test/") -> dict:
    file = os.path.join(path, "request.json")
    try:
        f = open(file, 'r')
    except:
        f = open(file, 'w')
    return recursive_parser(f)


def logParser(path: str="/Users/low/ATRC/GADDS/test/") -> list:
    file = os.path.join(path, "gadds_download.log")
    try:
        f = open(file, 'r')
    except:
        print("File does not exist")
        return []
    lines = [line.rstrip() for line in f]
    return lines


def mergeLogMeta(path: str="/Users/low/ATRC/GADDS/test/") -> dict:
    meta = metaDataParser(path)
    log = logParser(path)


class GaddsZipDownloader(DataDownloader):
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
    
    def convert_search_results_to_json(self):
        raise NotImplementedError
    
    def unzip(self, zip_relpath="gadds/gadds_zips", unzip_relpath="gadds/gadds_unzipped", replace=False):
        full_zip_path = os.path.join(self.datapath, zip_relpath)
        full_unzip_path = os.path.join(self.datapath, unzip_relpath)
        zips = os.listdir(full_zip_path)
        unzipped_folders = os.listdir(full_unzip_path)
        for each_zip in zips:
            if ".zip" not in each_zip:
                print("Non zip file in gadds_zip: {}".format(full_zip_path))
                continue
            each_zip_path = os.path.join(full_zip_path, each_zip)
            each_unzip_path = os.path.join(full_unzip_path, each_zip).replace(".zip", "")
            if each_unzip_path.split(os.sep)[-1] in unzipped_folders and replace==False:
                continue
            with ZipFile(each_zip_path, 'r') as zObject:
                zObject.extractall(path=each_unzip_path)
    
    def search_tiffs_by_bbox(self, unzip_relpath="gadds/gadds_unzipped"):
        tiffs_in_bbox_dir_paths = []
        full_unzip_path = os.path.join(self.datapath, unzip_relpath)
        tiff_paths = glob.glob(full_unzip_path + "/**/**.tif")
        for each_tiff_path in tiff_paths:
            gtiff = Geotiff(each_tiff_path)
            if self.bbox.intersects(gtiff.wgs_bounds):
                gtiff_folder = each_tiff_path.replace(os.sep + each_tiff_path.split(os.sep)[-1], "")
                tiffs_in_bbox_dir_paths.append(gtiff_folder)
        return tiffs_in_bbox_dir_paths
    
    def get_gsd_m_from_folder(self, folder):
        tiff_path = glob.glob(folder + os.sep + "/**.tif")[0]
        gtiff = Geotiff(tiff_path)
        lon_d = geodesic(gtiff.wgs_bounds.nw, gtiff.wgs_bounds.ne).m
        lat_d = geodesic(gtiff.wgs_bounds.nw, gtiff.wgs_bounds.sw).m
        pix_width = gtiff.pixel_bounds[1][0]
        pix_height = gtiff.pixel_bounds[1][1]
        return lon_d/pix_width


    def get_image_data_list(self, max_items: int) -> 'list[Entry]':
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
        self.unzip()
        all_entries = []
        # Don't really have an API, but I'll keep the name for consistency
        api_bbox_search_results = self.search_tiffs_by_bbox()

        for each_result in api_bbox_search_results:
            if len(api_bbox_search_results) > max_items:
                break

            
            entry = {}
            dir_meta_dict = metaDataParser(each_result)
            tif_name = glob.glob(each_result + os.sep + "/**.tif")[0].split(os.sep)[-1]
            uid = uid = each_result.split(os.sep)[-1]
            name = uid
            relpath = self.tiff_dir + os.sep + name + ".tiff"
            required_metadata = {}
            required_metadata["channels"] = {0: "mag"}
            required_metadata["dataset"] = "gadds"
            required_metadata["gsd_m"] = self.get_gsd_m_from_folder(each_result)
            required_metadata["date_collected"] = dir_meta_dict["startDateTime"]
            full_metadata = dir_meta_dict
            full_metadata["dir_path"] = each_result

            entry = Entry(name, uid, relpath, required_metadata, full_metadata)
            all_entries.append(entry)
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
        tiff_dir_path = entry.full_metadata["dir_path"]
        tiff_path = glob.glob(tiff_dir_path + os.sep + "*.tif")[0]
        shutil.copy2(tiff_path, os.path.join(self.datapath,entry.relpath))


        #outputpath = os.path.join(self.datapath, entry.relpath)
        #relevant_dl_params = entry.full_metadata["my_api_relevant_param"]
        #self.download_from_my_cool_api(outputpath, relevant_dl_params)
        #self.cleanup(outputpath)
        return

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

