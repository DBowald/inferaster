#from curses import window
#from math import dist
from re import T
import rasterio
import rasterio.features
from rasterio.windows import Window
import numpy as np
import pyproj
from matplotlib import pyplot as plt
import random
import os
from PIL import Image
from rasterio._err import CPLE_NotSupportedError
import yaml
import argparse
from inferaster.utils.geo_shapes import WgsBBox
import warnings
import json
warnings.filterwarnings("ignore")


# TODO- Replace Pyproj.proj with Pyproj.Transform
# TODO- docker pipeline
# TODO - shaply bounding boxes insted of ULBR 
# TODO - doc strings
# TODO - make another class for chipping function 
# TODO merge with geo_shapes?

class Geotiff:
    def __init__(self, geotiff_path):
        self.geo_reader = rasterio.open(geotiff_path, 'r+')
        self.src_affine = self.geo_reader.transform
        self.src_crs = self.geo_reader.crs

        bounds = self.geo_reader.bounds
        self.geo_bounds = np.array([[bounds.left, bounds.top],
                        [bounds.right, bounds.bottom]])
        pixel_bounds_arr = self.geo_to_pix(self.geo_bounds)
        self.pixel_bounds = pixel_bounds_arr
        # TODO This is a VERY hacky way to ensure back compatibility;
        # ultimately, wgs84_bounds code needs to be re written, and
        # geotiff also rewritten to work with geo_shapes
        try:
            wgs_bounds_arr = self.geo_to_wgs84(self.geo_bounds)
        except:
            wgs_bounds_arr = np.array([[bounds.left, bounds.top],
                        [bounds.right, bounds.bottom]])
        self.wgs84_bounds = wgs_bounds_arr
        self.wgs_bounds = WgsBBox(wgs_bounds_arr[0], wgs_bounds_arr[1])
    def close(self):
        self.geo_reader.close()

    def geo_to_pix(self, geo_xy):
        """
        Converts geo x,y vectors to pixel coordinates given a rasterio.Affine
        object representing the affine transformation between raster and
        2D geographic space
        """
        geo_xy = np.atleast_2d(geo_xy)
        A = np.array(self.src_affine).reshape(3, 3)
        xy1 = np.vstack((geo_xy.T, np.ones(geo_xy.shape[0],)))
        return np.dot(np.linalg.inv(A), xy1).T[:, 0:2]

    def geo_to_wgs84(self, geo_xy):
        """
        Takes in geo X and geo Y coordinates represented in the rasterio crs
        format and returns an Nx2 numpy.ndarray of Longitude, Latitude in Degrees
        """
        geo_xy = np.atleast_2d(geo_xy)
        lng, lat = rasterio.warp.transform(self.src_crs, {'init': 'epsg:4326'},
                                        geo_xy[:, 0], geo_xy[:, 1])
        return np.array([lng, lat]).T

    def pix_to_geo(self, pix_xy):
        """
        Converts pixel x,y vectors to geo coordinates given a rasterio.Affine
        object representing the affine transformation between raster and
        2D geographic space
        """
        pix_xy = np.atleast_2d(pix_xy)
        A = np.array(self.src_affine).reshape(3, 3)
        xy1 = np.vstack((pix_xy.T, np.ones(pix_xy.shape[0],)))
        return np.dot(A, xy1).T[:, 0:2]

    def pix_to_wgs84(self, pix):
        """
        Takes in pixels, returns an Nx2 numpy.ndarray of
        coordinates represented in the WGS-84 Lng, Lat (deg)
        """
        if self.src_crs != "epsg:4326":
            geo = self.pix_to_geo(pix)
            lon_lat = self.geo_to_wgs84(geo)
        else:
            lon_lat = self.pix_to_geo(pix)
        return lon_lat 
    
    def wgs84_to_geo(self, lon_lat):
        """
        Takes in WGS-84 Lng, Lat (deg) an returns an Nx2 numpy.ndarray of \
        coordinates represented in the rasterio crs \
        """
        lon_lat = np.atleast_2d(lon_lat)
        p1 = pyproj.Proj(init='epsg:4326')
        p2 = pyproj.Proj(self.src_crs, preserve_units=True)
        geo = np.array(pyproj.transform(p1, p2, lon_lat[:, 0], lon_lat[:, 1]))
        return geo.T

    def wgs84_to_pix(self, lon_lat):
        """
        Takes in WGS-84 Lng, Lat (deg) an returns an Nx2 numpy.ndarray of \
        coordinates represented in the raster pixel space \
        """
        # geo = self.wgs84_to_geo(lon_lat)
        # pix = self.geo_to_pix(geo)
        if (str(self.src_crs) != "EPSG:4326"):
            geo = self.wgs84_to_geo(lon_lat)
            pix = self.geo_to_pix(geo)
        else:
            pix = self.geo_to_pix(lon_lat)
        return pix


    def pix_bbox_to_wgs84(self, bbox):
        """
        Takes in a bbox representing a chip in geotiff image,
        returns a grid representing the WGS84 location of each
        pixel in the chip.
        """
        l, t = bbox[0]
        r, b = bbox[1]
        pix_grid = np.array(np.meshgrid(np.arange(l,r+1), np.arange(t,b+1))).T.reshape(-1,2)
        wgs_grid = self.pix_to_wgs84(pix_grid).reshape(r-l+1, b-t+1, 2)
        return  wgs_grid
    
    def wgs84_bbox_to_chip(self, bbox):
        """
        Takes in a bbox in wgs84 coordinates,
        an image chip of that region from the geotiff.
        """
        f_pixel_chip_bounds = self.wgs84_to_pix(bbox)
        pixel_chip_bounds = np.rint(f_pixel_chip_bounds).astype("int")
        start_col = pixel_chip_bounds[0][0]
        start_row = pixel_chip_bounds[0][1]
        end_col = pixel_chip_bounds[1][0]
        end_row = pixel_chip_bounds[1][1]
        width = end_col - start_col
        height = end_row - start_row
        chip = self.geo_reader.read(window=Window(start_col, start_row, width, height))
        return chip
    
    def wgs84_bbox_to_rio_chip(self,bbox):
        f_pixel_chip_bounds = self.wgs84_to_pix(bbox)
        pixel_chip_bounds = np.rint(f_pixel_chip_bounds).astype("int")
        start_col = pixel_chip_bounds[0][0]
        start_row = pixel_chip_bounds[0][1]
        end_col = pixel_chip_bounds[1][0]
        end_row = pixel_chip_bounds[1][1]
        width = end_col - start_col
        height = end_row - start_row
        window=Window(start_col, start_row, width, height)
        chip = self.geo_reader.read(window=window)
        win_transform = self.geo_reader.window_transform(window=window)
        profile = self.geo_reader.profile.copy()
        profile.update({
            'height': height,
            'width': width,
            'transform': win_transform
        })
        return chip, profile    


    def display_chip(self, chip):
        plt.imshow(np.moveaxis(chip,0,-1))
        plt.show()
    
    def display_tiff(self):
        # DANGER: Do at your own risk
        img = self.geo_reader.read()
        plt.imshow(np.moveaxis(img,0,-1))
        plt.show()
    
    def display_downsampled_tiff(self, factor):
        #TO DO
        pass
    
    def check_wgs84_in_bounds(self, point):
        # Do this for real later
        return True
    
    def randomly_sample_chips(self, chip_size, n_chips, with_replacement=True):
        # This is a bad function, don't put it in anything important
        if not with_replacement:
            raise Exception("Not implemented yrasterio transformrasterio transformrasterio transformet")
        for i in range(n_chips):
            randx = random.randint(0, int(geotiff.pixel_bounds[1][0]) - chip_size)
            randy = random.randint(0, int(geotiff.pixel_bounds[1][1]) - chip_size)
            chip = self.geo_reader.read(window=Window(randx, randy, chip_size, chip_size))
            center  = (randx + chip_size//2, randy + chip_size//2)
            wgs_center = self.pix_to_wgs84(center)
            filename = "{}W{}N".format(wgs_center[0][0], wgs_center[0][1])
            if not os.path.exists("./data/hroi/{}".format(filename)):
                os.mkdir("./data/hroi/{}".format(filename))
                im = Image.fromarray(chip.swapaxes(0,2))
                im.save("./data/hroi/{}/{}.png".format(filename, filename))

    def wgs84_to_ecef(self,lon_lat):
        zero=np.zeros(len(lon_lat))
        x,y,z = rasterio.warp.transform('epsg:4326', 'epsg:4978',
                                    lon_lat[:, 0], lon_lat[:, 1], zero)
        
        return np.array([x,y,z]).T
    
    def write_tags(self, metadata_dict:dict):
        jstring_dict = {}
        for k,v in metadata_dict.items():
            jstring_dict[k] = json.dumps(v)
        self.geo_reader.update_tags(**jstring_dict)
        

    def read_tags(self):
        tags = {}
        for k,v in self.geo_reader.tags().items():
            if type(v) == type("") and v[0] == '{' and v[-1] == '}':
                tags[k] = json.loads(v)
            else:
                tags[k] = v
        return tags


    # def ecef_distance(self,points):
    #     dismatrix=points[0,:]-points[1,:]
    #     dis=np.sqrt(sum(dismatrix**2))
    #     return dis
 

#hi



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml",default= "inferaster/utils/config_files/maxar.yaml",
                        help="the yaml file for inputs")
    args = parser.parse_args()

    stream = open(args.yaml,'r')
    inputs = yaml.full_load(stream)
    datapath = inputs['datapath']
    #RGB_dir = inputs['Image_location']['RGB']
    #current_dir=os.path.join(data_prefix, RGB_dir)
    #outdir = inputs['Output_Location']
    #size = 200
    #count = 0


    #geotiff=Geotiff(geotiff_path=current_dir+ '/RGB-cvg_r0034c0049.tif')
    #geotiff.split_images(outdir,size)

    for img in os.listdir(datapath):
        if '.tif' in img:
            try: 
                geotiff = Geotiff(geotiff_path = datapath+ '/'+ img)
            except CPLE_NotSupportedError: 
                print(img+' has CPLE_NotSupportedError')
                continue     