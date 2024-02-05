#from types import List
import os
import glob
import shutil
import json
import datetime
from inferaster.downloaders.data_downloader import DataDownloader, Entry
from zipfile import ZipFile
from inferaster.utils.geotiff import Geotiff
import rasterio

from geopy.distance import geodesic

from rasterio.warp import reproject, Resampling
from affine import Affine
import numpy as np

import math
import json




class UmbraZipDownloader(DataDownloader):
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
    
    def unzip(self, zip_relpath="umbra/umbra_zips", unzip_relpath="umbra/umbra_unzipped", replace=False):
        full_zip_path = os.path.join(self.datapath, zip_relpath)
        full_unzip_path = os.path.join(self.datapath, unzip_relpath)
        zips = os.listdir(full_zip_path)
        unzipped_folders = os.listdir(full_unzip_path)
        for each_zip in zips:
            if ".zip" not in each_zip:
                print("Non zip file in umbra_zips: {}".format(full_zip_path))
                continue
            each_zip_path = os.path.join(full_zip_path, each_zip)
            each_unzip_path = os.path.join(full_unzip_path, each_zip).replace(".zip", "")
            if each_unzip_path.split(os.sep)[-1] in unzipped_folders and replace==False:
                continue
            with ZipFile(each_zip_path, 'r') as zObject:
                zObject.extractall(path=each_unzip_path)
    
    def search_tiffs_by_bbox(self, unzip_relpath="umbra/umbra_unzipped"):
        tiffs_in_bbox_dir_paths = []
        for root, dirs, files in os.walk(os.path.join(self.datapath, unzip_relpath)):
            for name in files:
                if ".tif" not in name:
                    continue
                gtiff = Geotiff(os.path.join(root, name))
                if self.bbox.intersects(gtiff.wgs_bounds):
                    gtiff_folder = root
                    tiffs_in_bbox_dir_paths.append(gtiff_folder)
                gtiff.close()
        return tiffs_in_bbox_dir_paths



        # tiffs_in_bbox_dir_paths = []
        # full_unzip_path = os.path.join(self.datapath, unzip_relpath)
        # tiff_paths = glob.glob(full_unzip_path + "/**/**.tif")
        # for each_tiff_path in tiff_paths:
        #     gtiff = Geotiff(each_tiff_path)
        #     if self.bbox.intersects(gtiff.wgs_bounds):
        #         gtiff_folder = each_tiff_path.replace(os.sep + each_tiff_path.split(os.sep)[-1], "")
        #         tiffs_in_bbox_dir_paths.append(gtiff_folder)
        #     gtiff.close()
        # return tiffs_in_bbox_dir_paths

    def get_gsd_m_from_folder(self, folder):
        tiff_path = glob.glob(folder + os.sep + "/**.tif")[0]
        gtiff = Geotiff(tiff_path)
        lon_d = geodesic(gtiff.wgs_bounds.nw, gtiff.wgs_bounds.ne).m
        lat_d = geodesic(gtiff.wgs_bounds.nw, gtiff.wgs_bounds.sw).m
        pix_width = gtiff.pixel_bounds[1][0]
        pix_height = gtiff.pixel_bounds[1][1]
        return lon_d/pix_width

    def get_datetime_from_path(self, path):
        datestr = path.split("_UMBRA")[0].split(os.sep)[-1]
        y,mo,d,h,m,s = datestr.split("-")
        date = datetime.datetime(int(y),int(mo),int(d),int(h),int(m),int(s))
        return str(date)


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
        # self.unzip()
        all_entries = []
        # Don't really have an API, but I'll keep the name for consistency
        api_bbox_search_results = self.search_tiffs_by_bbox(unzip_relpath='sar-data/tasks')

        for each_result in api_bbox_search_results:
            if len(api_bbox_search_results) > max_items:
                break

            
            entry = {}
            #dir_meta_dict = metaDataParser(each_result)
            tif_name = glob.glob(each_result + os.sep + "/**.tif")[0].split(os.sep)[-1]
            uid = uid = each_result.split(os.sep)[-1]
            name = uid
            relpath = self.tiff_dir + os.sep + name + ".tiff"
            required_metadata = {}
            required_metadata["channels"] = {0: "sar"}
            required_metadata["dataset"] = "umbra"
            required_metadata["gsd_m"] = self.get_gsd_m_from_folder(each_result)
            required_metadata["date_collected"] = self.get_datetime_from_path(each_result)
            full_metadata = {}
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
        #rasterio.open(tiff_path)
        geotiff = Geotiff(tiff_path)
        f = open(glob.glob(tiff_dir_path + os.sep + "*.json")[0])
        meta = json.load(f)
        rotation_t = self.get_rotation_north(geotiff)
        rotation_t = math.degrees(rotation_t)
        rotation = meta["collects"][0]['angleAzimuthDegrees']
        rotation = -rotation
        if(rotation < 0):
            rotation = 360 + rotation
        width = geotiff.geo_reader.width
        height = geotiff.geo_reader.height
        max_length = math.sqrt((width**2) + (height**2))
        adj_w = max_length - width
        adj_h = max_length - height
        shift_x, shift_y = self.get_shift_for_rotation(rotation, width, height)
        #shift_y = height/2
        fname = tiff_path.split('/')[-1]
        out_path = os.path.join(self.datapath, "tiffs_to_chip", fname + "f")
        self.rotate_raster(tiff_path, out_path, rotation,
                           adj_height=adj_h, adj_width=adj_w, shift_x=-shift_x, shift_y=shift_y)
        
        #shutil.copy2(tiff_path, os.path.join(self.datapath,entry.relpath))


    def my_rotate(self, x, y, angle):
        # Assumes input in degrees
        theta = math.radians(angle)
        return x*math.cos(theta) - y*math.sin(theta), x*math.sin(theta) + y*math.cos(theta)

    def get_shift_for_rotation(self, rotation, width, height):
        x = width
        y = -height

        (x1, y1) = self.my_rotate(x, 0, rotation)
        (x2, y2) = self.my_rotate(0, y, rotation)
        (x3, y3) = self.my_rotate(x, y, rotation)

        shift_x = min(x1, x2, x3, 0)
        shift_y = max(y1, y2, y3, 0)
        return -shift_x, shift_y


    def get_rotation_north_new(self,geotiff):
        width = geotiff.geo_reader.width
        height = geotiff.geo_reader.height
        points = {
            "tl": geotiff.pix_to_wgs84((0,0)),
            "tr": geotiff.pix_to_wgs84((width,0)),
            "bl": geotiff.pix_to_wgs84((0,height)),
            "br": geotiff.pix_to_wgs84((width,height))

        }

        #dict(sorted(people.items(), key=lambda item: item[1]))



    def get_rotation_north(self, geotiff):
        sx = np.linalg.norm(np.array(geotiff.src_affine.column_vectors[0]), ord=2)
        sy = np.linalg.norm(np.array(geotiff.src_affine.column_vectors[1]), ord=2)
        sz = np.linalg.norm(np.array(geotiff.src_affine.column_vectors[2]), ord=2)

        theta = math.acos(geotiff.src_affine.a / sx)
        theta1 = -1 * math.asin(geotiff.src_affine.b / sx)
        theta2 = math.asin(geotiff.src_affine.d / sy)
        theta3 = math.acos(geotiff.src_affine.e / sy)

        return theta


        width = geotiff.geo_reader.width
        height = geotiff.geo_reader.height
        big_height = geotiff.wgs84_to_pix(geotiff.wgs_bounds.se)[0][1]
        delta_h = big_height - height
        big_width = geotiff.wgs84_to_pix(geotiff.wgs_bounds.ne)[0][0]
        delta_w = big_width - width
        return math.atan(delta_h/width)
        

    def rotate_raster(self, in_file,out_file, angle, shift_x=0, shift_y=0,adj_width=0, adj_height=0):


        with rasterio.open(in_file) as src:

            # Get the old transform and crs
            src_transform = src.transform 
            crs = src.crs

            # Affine transformations for rotation and translation
            rotate = Affine.rotation(angle)
            trans_x = Affine.translation(shift_x,0)
            trans_y = Affine.translation(0, -shift_y)

            # Combine affine transformations
            dst_transform = src_transform * rotate * trans_x * trans_y

            # Get band data
            band = np.array(src.read(1))

            # Get the new shape
            y,x = band.shape
            dst_height = y + adj_height
            dst_width = x + adj_width

            # set properties for output
            dst_kwargs = src.meta.copy()
            dst_kwargs.update(
                {
                    "transform": dst_transform,
                    "height": dst_height,
                    "width": dst_width,
                    "nodata": 255,
                }
            )

            # write to disk
            with rasterio.open(out_file, "w", **dst_kwargs) as dst:
                # reproject to new CRS

                reproject(source=band,
                            destination=rasterio.band(dst, 1),
                            src_transform=src_transform,
                            src_crs=crs,
                            dst_transform=dst_transform,
                            dst_crs=crs,
                            resampling=Resampling.nearest)



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

