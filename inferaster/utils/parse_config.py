import argparse
import yaml
import os
from inferaster.utils.geo_shapes import WgsBBox, WgsPoint
from inferaster.tiling.tilesets import OsmTileset, EquiviTilesTileset

# Need a more elegant way to do this




def parse_config(yaml_path:str) -> dict:
    """Parses the config yaml into a new parsed dictionary with correct initialization.

    Parameters
    ----------
    yaml_path : str
        full file path to config yaml. Check out inferaster/utils/config_files for examples.

    Returns
    -------
    dict
        Dictionary w/ same keys as og yaml, but with intialized values. 

    Raises
    ------
    NotImplementedError
        _description_
    """
    stream = open(yaml_path,'r')
    inputs = yaml.full_load(stream)

    new_dict = {}

    for k,v in inputs.items():
        if k == "bounding_box":
            nw_pt = WgsPoint(inputs["bounding_box"]["nw_point"]["longitude"], 
                                inputs["bounding_box"]["nw_point"]["latitude"])
            se_pt = WgsPoint(inputs["bounding_box"]["se_point"]["longitude"], 
                            inputs["bounding_box"]["se_point"]["latitude"])
            bounds = WgsBBox(nw_pt, se_pt)
            new_dict[k] = bounds
        elif k == "bounding_box_set":
            keys = inputs['bounding_box_set'].keys()
            first = list(keys)[0]
            nw_pt = WgsPoint(inputs["bounding_box_set"][first]["nw_point"]["longitude"], 
                                inputs["bounding_box_set"][first]["nw_point"]["latitude"])
            se_pt = WgsPoint(inputs["bounding_box_set"][first]["se_point"]["longitude"], 
                            inputs["bounding_box_set"][first]["se_point"]["latitude"])
            bounds = WgsBBox(nw_pt, se_pt)
            new_dict["bounding_box"] = bounds
            all_bounds = []
            for key in keys:
                nw_pt = WgsPoint(inputs["bounding_box_set"][key]["nw_point"]["longitude"], 
                                    inputs["bounding_box_set"][key]["nw_point"]["latitude"])
                se_pt = WgsPoint(inputs["bounding_box_set"][key]["se_point"]["longitude"], 
                                inputs["bounding_box_set"][key]["se_point"]["latitude"])
                bounds = WgsBBox(nw_pt, se_pt)
                all_bounds.append(bounds)
            new_dict['bounding_box_set']  = all_bounds


        elif k == "tiling_method":
            if inputs["tiling_method"].lower() == "EquiviTiles".lower():
                try:
                    chip_size_m = inputs["chip_size_m"]
                except KeyError:
                    Warning("No chip_size_m specified in \"{}\". Using default of 200 meters per chip.".format(yaml_path))
                    chip_size_m = 200
                tiling_method = EquiviTilesTileset(new_dict["bounding_box"], chip_size_m)
                new_dict[k] = tiling_method
            elif inputs["tiling_method"].lower() == "OSM".lower():
                try:
                    zoom_level = inputs["zoom_level"]
                except KeyError:
                    Warning("No zoom_level specified in \"{}\". Using default of 17.".format(yaml_path))
                    zoom_level = 17
                tiling_method = OsmTileset(new_dict["bounding_box"], zoom_level)
                new_dict[k] = tiling_method
            else:
                raise NotImplementedError("Error: Only implemented tiling methods are EquiviTiles and OSM.")
        elif k == "datapath":
            try:
                datapath = inputs["datapath"]
            except KeyError:
                Warning("No datapath specified in \"{}\"; defaulting to ~/data/geotiffs/".format(yaml_path))
                datapath = "~/data/geotiffs/"
            datapath = os.path.expanduser(datapath)
            new_dict[k] = datapath
        elif k == "full_tiff_dir":
            try:
                full_tiff_dir = inputs["full_tiff_dir"]
            except KeyError:
                Warning("No datapath for tiffs specified in \"{}\"; defaulting to tiffs_to_chip".format(yaml_path))
                full_tiff_dir = "tiffs_to_chip"
            new_dict[k] = full_tiff_dir 
        elif k == "chip_dir":
            try:
                full_tiff_dir = inputs["chip_dir"]
            except KeyError:
                Warning("No datapath for tiffs specified in \"{}\"; defaulting to chipped".format(yaml_path))
                full_tiff_dir = "chipped"
            new_dict[k] = full_tiff_dir
        elif k == "chip_size_m":
            new_dict[k] = int(v)
        elif k == "zoom_level":
            new_dict[k] = int(v)
        else:
            new_dict[k] = v


    return new_dict