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
import math
from rasterio.warp import reproject, Resampling
from affine import Affine

warnings.filterwarnings("ignore")


# TODO- Replace Pyproj.proj with Pyproj.Transform
# TODO- docker pipeline
# TODO - shaply bounding boxes insted of ULBR 
# TODO - doc strings
# TODO - make another class for chipping function 
# TODO merge with geo_shapes?

class Geotiff:
    def __init__(self, geotiff_path):
        self.geo_path = geotiff_path
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
        self.show_bounds(cords_set = [self.find_exact()], bbox_set = [self.geo_bounds])
        


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
    
    # def wgs84_bbox_to_rio_chip_old(self,bbox):
    #     f_pixel_chip_bounds = self.wgs84_to_pix(bbox)
    #     pixel_chip_bounds = np.rint(f_pixel_chip_bounds).astype("int")
    #     start_col = pixel_chip_bounds[0][0]
    #     start_row = pixel_chip_bounds[0][1]
    #     end_col = pixel_chip_bounds[1][0]
    #     end_row = pixel_chip_bounds[1][1]
    #     width = end_col - start_col
    #     height = end_row - start_row
    #     window=Window(start_col, start_row, width, height)
    #     chip = self.geo_reader.read(window=window)
    #     win_transform = self.geo_reader.window_transform(window=window)
    #     profile = self.geo_reader.profile.copy()
    #     profile.update({
    #         'height': height,
    #         'width': width,
    #         'transform': win_transform
    #     })
    #     return chip, profile    


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
    def show_bounds(self,cords_set = [],bbox_set= []):
        """runs and show cords and bbox relitive ot one another
        exampe function is 
        self.show_bounds(cords_set = [self.go_cords], bbox_set = [self.geo_bounds])

        Args:
            cords_set (list, optional): _description_. Defaults to [].
            bbox_set (list, optional): _description_. Defaults to [].
        """
        plt.figure()
        for cord in cords_set:
            cords = np.append(cord,[cord[0]],axis = 0)
            x, y = zip(*cords) 
            plt.plot(x,y)

        for bbox in bbox_set:
            cord = self.bbox_to_cords(bbox)
            cords = np.append(cord,[cord[0]],axis = 0)
            x, y = zip(*cords) 
            plt.plot(x,y)

        plt.show()



    def bbox_to_cords(self,cord):
        """convers the bbox format into the cords format. The cords format is used for non-northallined functions
        """
        first = cord[0]
        second = [cord[0][0],cord[1][1]]
        third = cord[1]
        forth = [cord[1][0],cord[0][1]]
        cords = [first,second,third,forth]
        return cords

    
    def find_exact(self):
        """
        Finds exact coordinates for a rasterio image This is necessary for images that are not north aligned.
        Returns:
            _type_: a 2 by 4 array 
        """
        affine = self.src_affine
        shape = self.geo_reader.shape
        box = self.create_box(shape)
        box = np.array(box)
        affine = np.array(affine)
        affine = np.reshape(affine,[3,3])
        return self.apply_affine(box,affine)
    
    def apply_affine(self,box,affine):
        """
        applies affine transform to a list of pairs of points 
        Returns:
            _type_: 
        """
        affine = np.reshape(np.array(affine),[3,3])
        main = affine[:2,:2]
        end = affine[:2,2]
        box_updated = []
        for loc in box:
            mid = np.cross(loc,main)
            found = mid + end
            box_updated.append(found)
        box_update = np.array(box_updated)
        return box_update[:,:]
    
    def create_box(self,shape):
        first = [0,0]
        second = [0,-shape[1]]
        third = [shape[0],-shape[1]]
        forth = [shape[0],0]
        box = [first,second,third,forth]
        return box
    
    def wgs84_to_pix_rotated(self, lon_lat):
        """
        Takes in WGS-84 Lng, Lat (deg) an returns an Nx2 numpy.ndarray of \
        coordinates represented in the raster pixel space \
        """
        # geo = self.wgs84_to_geo(lon_lat)
        # pix = self.geo_to_pix(geo)
        if (str(self.src_crs) != "EPSG:4326"):
            geo = self.wgs84_to_geo(lon_lat)
            pix = self.geo_to_pix_rotated(geo)
        else:
            pix = self.geo_to_pix_rotated(lon_lat)
        return pix
    
    def geo_to_pix_rotated(self, geo_xy):
        """
        Converts geo x,y vectors to pixel coordinates given a rasterio.Affine
        object representing the affine transformation between raster and
        2D geographic space
        """
        geo_xy = np.atleast_2d(geo_xy)
        A = np.array(self.rotated_affine).reshape(3, 3)
        xy1 = np.vstack((geo_xy.T, np.ones(geo_xy.shape[0],)))
        return np.dot(np.linalg.inv(A), xy1).T[:, 0:2]
    
    def wgs84_bbox_to_rio_chip(self,bbox):
        self.save_rotate()
        f_pixel_chip_bounds = self.wgs84_to_pix_rotated(bbox)
        pixel_chip_bounds = np.rint(f_pixel_chip_bounds).astype("int")
        start_col = pixel_chip_bounds[0][0]
        start_row = pixel_chip_bounds[0][1]
        end_col = pixel_chip_bounds[1][0]
        end_row = pixel_chip_bounds[1][1]
        width = end_col - start_col
        height = end_row - start_row
        window=Window(start_col, start_row, width, height)
        chip = self.rotated_geo_reader.read(window=window)
        win_transform = self.rotated_geo_reader.window_transform(window=window)
        profile = self.rotated_geo_reader.profile.copy()
        profile.update({
            'height': height,
            'width': width,
            'transform': win_transform
        })
        return chip, profile 


    def save_rotate(self):
        tiff_path = self.geo_path
        fname = tiff_path.split('/')[-1]
        out_path = os.path.join('/tmp', fname)
        self.rotated_path = out_path
        if not os.path.exists(out_path):
            rotation_t = self.get_rotation_north()
            rotation = math.degrees(rotation_t)
            rotation = -rotation
            width = self.geo_reader.width
            height = self.geo_reader.height
            if(rotation < 0):
                rotation = 360 + rotation
            max_length = math.sqrt((width**2) + (height**2))
            adj_w = max_length - width
            adj_h = max_length - height
            max_length = math.sqrt((width**2) + (height**2))
            adj_w = max_length - width
            adj_h = max_length - height
            shift_x, shift_y = self.get_shift_for_rotation(rotation, width, height)
            self.rotate_raster(tiff_path, out_path, rotation,
                                adj_height=adj_h, adj_width=adj_w, shift_x=-shift_x, shift_y=shift_y)
        rotated_data = rasterio.open(out_path)
        self.rotated_geo_reader = rotated_data
        self.rotated_affine = rotated_data.transform
        self.rotated_width = rotated_data.width
        self.rotated_height = rotated_data.height
        return         
    

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

    def get_rotation_north(self):
        sx = np.linalg.norm(np.array(self.src_affine.column_vectors[0]), ord=2)
        # sy = np.linalg.norm(np.array(self.src_affine.column_vectors[1]), ord=2)
        # sz = np.linalg.norm(np.array(self.src_affine.column_vectors[2]), ord=2)

        theta = math.acos(self.src_affine.a / sx)
        # theta1 = -1 * math.asin(self.src_affine.b / sx)
        # theta2 = math.asin(self.src_affine.d / sy)
        # theta3 = math.acos(self.src_affine.e / sy)

        return theta

    def get_rotation_north_new(self):
        width = self.geo_reader.width
        height = self.geo_reader.height
        points = {
            "tl": self.pix_to_wgs84((0,0)),
            "tr": self.pix_to_wgs84((width,0)),
            "bl": self.pix_to_wgs84((0,height)),
            "br": self.pix_to_wgs84((width,height))

        }
        
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
    # def ecef_distance(self,points):
    #     dismatrix=points[0,:]-points[1,:]
    #     dis=np.sqrt(sum(dismatrix**2))
    #     return dis
 

#hi



if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--yaml",default= "inferaster/utils/config_files/maxar.yaml",
    #                     help="the yaml file for inputs")
    # args = parser.parse_args()

    # stream = open(args.yaml,'r')
    # inputs = yaml.full_load(stream)
    # datapath = inputs['datapath']
    #RGB_dir = inputs['Image_location']['RGB']
    #current_dir=os.path.join(data_prefix, RGB_dir)
    #outdir = inputs['Output_Location']
    path = '/home/isaac/data/hypernet_full/tiffs_to_chip/Coal Oil Point 1, CA-8-6-07.tiff'

    geotiff=Geotiff(geotiff_path=path)
    # array([[-119.92554625,   34.428002  ],
    #    [-119.75620525,   34.394692  ]])
    
    bbox = ([[-119.9,   34.420  ],
       [-199.8,  34.4  ]])

    chip, profile = geotiff.wgs84_bbox_to_rio_chip_updated(bbox)


    # geotiff.split_images(outdir,size)

    # for img in os.listdir(datapath):
    #     if '.tif' in img:
    #         try: 
    #             geotiff = Geotiff(geotiff_path = datapath+ '/'+ img)
    #         except CPLE_NotSupportedError: 
    #             print(img+' has CPLE_NotSupportedError')
    #             continue     