import argparse
from data_trawler.utils.parse_config import parse_config
import os
import shutil
import pathlib


# The script takes in the path to the directory with both ir and rgb images. It will split them up into two separate
# folders with subdirectories with the same name, except only ir images in one and rgb in the other

def find_suffix(file: str):
    """ 
    Looks for "_" in a file name and whatever that comes after that as a string

    Parameters
    ----------
    file : str
        Name of the file we want to parse

    Returns
    -------
    Str
        Its return a string of whatever that comes after "_" in the file name
    """
    name = pathlib.Path(file).stem
    underscore = name.find("_") + 1
    if underscore == -1:
        return "neither"
    return name[underscore:]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml",default= "utils/config_files/my_default_config.yaml",
                        help="Your yaml file for inputs; do not use base_* files, these are example files, and changes will be pushed.")
    parser.add_argument("--download", action='store_true')
    parser.add_argument("--chip", action='store_true')
    args = parser.parse_args()
    print(args)

    parsed_config = parse_config(args.yaml)

    datapath = parsed_config["datapath"]
    chip_dir = parsed_config["chip_dir"]

    rel_path = input("What is the relative path to the directory you want to split: ")
    # rel_path = "equivitiles/200"
    abs_path = os.path.join(datapath, chip_dir, rel_path)

    rgb_path = abs_path + "_rgb"
    ir_path = abs_path + "_ir"
    os.makedirs(rgb_path)
    os.makedirs(ir_path)

    subdir = os.listdir(abs_path)
    for dir in subdir:
        files = os.listdir(os.path.join(abs_path, dir))
        os.makedirs(os.path.join(rgb_path, dir))
        os.makedirs(os.path.join(ir_path, dir))
        for file in files:
            suffix = find_suffix(file)
            if suffix == "rgb":
                src_path = os.path.join(abs_path, dir, file)
                dst_path = os.path.join(rgb_path, dir, file)
                os.rename(src_path, dst_path)
            if suffix == "ir":
                src_path = os.path.join(abs_path, dir, file)
                dst_path = os.path.join(ir_path, dir, file)
                os.rename(src_path, dst_path)
    shutil.rmtree(abs_path)
