#from cmath import atan
#from math import dist
#from re import T
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
import inferaster.utils.geotiff as geotiff
import inferaster.tiling.tilesets as tilesets
from inferaster.utils.geo_shapes import WgsBBox, WgsPoint
import glob
from inferaster.utils.geotiff import Geotiff
import geopandas
import pandas as pd
import json
import rasterio


class BaseChipper():
    """
    
    """
    def __init__(self, parsed_config:dict) ->None:
        """

        Parameters
        ----------
        parsed_config : dict
            _description_
        """
        self.tileset = parsed_config["tiling_method"]
        self.datapath = parsed_config["datapath"]
        self.full_tiff_dir = parsed_config["full_tiff_dir"]
        self.chip_dir = parsed_config["chip_dir"]
        self.metadata_json = self.read_metadata_json()
        self.full_tiffs_path = os.path.join(self.datapath, self.full_tiff_dir)
        self.chips_path = self.get_chips_path()

    def chip(self, stitch_mode="no_stitch"):
        """
        Parameters
        ----------
        stitch_mode : str, optional
            how to fill in chips at image edges, by default "no_stitch".
            Options:
                no_stitch - do nothing, tiffs will not be used unless they fully contain the chip
                mosaic - unimplemented, stitch in information from same domain tiffs to fill the gap
        """
        # TODO speed this up somehow
        # TODO full list to df vs building df 1 row at a time speed comparison
        tile_list = self.tileset.get_tiles_from_wgs_bbox()
        aoi_tiff_gdf = self.get_aoi_tiffs_gdf(self.full_tiffs_path, use_cache=False)
        # TODO Should probably make tile dataframe and loop over tiffs instead...
        
        for each_tile in tile_list:
            if stitch_mode == "no_stitch": 
                self.save_stack_no_stitch(each_tile, aoi_tiff_gdf)
            elif stitch_mode == "mosaic": 
                self.save_stack_mosaic(each_tile, aoi_tiff_gdf)
            else: 
                raise NotImplementedError("Valid options for stitch modes are no_stitch, and mosaic")
        
        # TODO: See if there's a better way to do this
        # TODO: Make this a generator
        #for each_tile in tile_list:
        #    pass
    
    def save_stack_no_stitch(self, tile:tilesets.Tile, tiff_gdf:geopandas.GeoDataFrame):
        """


        Parameters
        ----------
        tile : tilesets.Tile
            Tile bounding where to pull geotiff data from
        tiff_gdf : geopandas.GeoDataFrame
            dataframe of all relevant geotiffs
        """
        coverage_tiff_gdf = tiff_gdf[tiff_gdf.covers(tile) == True]
        for i, row in coverage_tiff_gdf.iterrows():
            if len(coverage_tiff_gdf) <= 0:
                break
            geo = Geotiff(row["full_path"])
            self.save_rio_chip(geo, tile)
            geo.close()
        print(coverage_tiff_gdf)
    
    def save_stack_mosaic(self, tile, tiff_gdf):
        raise NotImplementedError

    def get_chips_path(self) -> str:
        """
        Gets path to a subdirectory, based on the tiling method.

        Returns
        -------
        str
            path to subdirectories for given tileset.
        """

        prefix = os.path.join(self.datapath, self.chip_dir)
        if type(self.tileset) == tilesets.EquiviTilesTileset:
            postfix = os.path.join("equivitiles", str(self.tileset.chip_size_m))
        elif type(self.tileset) == tilesets.OsmTileset:
            postfix = os.path.join("osm", str(self.tileset.zoom_level))
        else:
            raise NotImplementedError
        return os.path.join(prefix, postfix)        

    
    def cache_tiff_gdf(self):
        raise NotImplementedError

    def load_cached_gdf(self):
        raise NotImplementedError
    
    def read_metadata_json(self):
        metadata_path = os.path.join(self.datapath, "metadata.json")
        with open(metadata_path, 'r') as metafp:
            metadata_json = json.load(metafp)
        return metadata_json

    def get_aoi_tiffs_gdf(self, tiff_path, use_cache=False):
        if use_cache == True:
            all_tiff_gdf = self.load_cached_gdf()
        else:
            all_tiff_gdf = self.get_all_tiffs_gdf(tiff_path)
        aoi_tiff_gdf = all_tiff_gdf[all_tiff_gdf.intersects(self.tileset.bounds) == True]
        return aoi_tiff_gdf
        
    def get_all_tiffs_gdf(self, tiff_path):
        full_tiff_list = glob.glob(tiff_path + "/*.tiff")
        tiff_bboxes = []
        img_names = []
        full_paths = []
        for each_tiff in full_tiff_list:
            full_paths.append(each_tiff)
            img_name = each_tiff.split(os.path.sep)[-1]
            img_names.append(img_name)
            new_tiff = Geotiff(each_tiff)
            tiff_bboxes.append(new_tiff.wgs_bounds)
            new_tiff.close()
            print(each_tiff)
        df = pd.DataFrame({"img_name": img_names,"full_path": full_paths})
        all_tiff_gdf = geopandas.GeoDataFrame(df, geometry=tiff_bboxes)
        return all_tiff_gdf
    
    def save_rio_chip(self, geotiff:Geotiff, tile:tilesets.Tile):
        bbox = [[tile.nw[0], tile.nw[1]],
                    [tile.se[0], tile.se[1]]]
        chip, profile = geotiff.wgs84_bbox_to_rio_chip(bbox)
        tile_dir = "{:3.6f}_{:3.6f}".format(tile.nw.lon, tile.nw.lat)
        name = geotiff.geo_reader.name.split(os.path.sep)[-1] #geotiff.read_tags()["name"].strip("\"") + ".tiff"
        tile_path = os.path.join(self.chips_path, tile_dir)
        chip_path = os.path.join(tile_path, name)
        if chip.any():
            # if (chip == 0).any():
            #     print("array is all zeros")
            #     return
            if not os.path.exists(tile_path):
                os.makedirs(tile_path)
            with rasterio.open(chip_path, 'w', **profile) as dst:
                dst.write(chip)
        else:
            print("array is empty")
 
        
    
    

    