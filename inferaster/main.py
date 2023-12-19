import argparse
import os

from data_trawler.utils.parse_config import parse_config
from data_trawler.chipping.chipper import BaseChipper

try:
    from data_trawler.downloaders.maxar_downloaders import MaxarApiTilesDownloader, MaxarZipDataDownloader
except ImportError as err:
    print(Warning("Current environment not compatible with maxar downloader; {}".format(err)))

try:
    from data_trawler.downloaders.aviris_downloaders import AvirisDownloader
except ImportError as err:
    print(Warning("Current environment not compatible with aviris downloader; {}".format(err)))

try:
    from data_trawler.downloaders.eros_downloaders import ErosDownloader
except ImportError as err:
    print(Warning("Current environment not compatible with eros downloader; {}".format(err)))

try:
    from data_trawler.downloaders.hyperion_downloader import ErosHyperionDownloader
except ImportError as err:
    print(Warning("Current environment not compatible with eros downloader; {}".format(err)))
try:
    from data_trawler.downloaders.mag_downloaders import GaddsZipDownloader
except ImportError as err:
    print(Warning("Current environment not compatible with GADDS downloader; {}".format(err)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml",default= "utils/config_files/hyperion_input.yaml",
                        help="Your yaml file for inputs; do not use base_* files, these are example files, and changes will be pushed.")
    parser.add_argument("--download", action='store_true')
    parser.add_argument("--chip", action='store_true')
    args = parser.parse_args()
    print(args)

    parsed_config = parse_config(args.yaml)

    datapath = parsed_config["datapath"]

    if not os.path.exists(datapath):
        os.makedirs(datapath)
    
    if "full_tiff_dir" in parsed_config:
        tiff_path = os.path.join(datapath, parsed_config["full_tiff_dir"])
        if not os.path.exists(tiff_path):
            os.mkdir(tiff_path)

    if "chip_dir" in parsed_config:
        chip_path = os.path.join(datapath, parsed_config["chip_dir"])
        if not os.path.exists(chip_path):
            os.mkdir(chip_path)

    
    if(args.download):
        dset = parsed_config["dataset"]
        if dset == "maxar_api":
            downloader = MaxarApiTilesDownloader(parsed_config)
        elif dset == "maxar_zip":
            donwloader = MaxarZipDataDownloader(parsed_config)
        elif dset == "aviris":
            downloader = AvirisDownloader(parsed_config)
        elif dset == "eros":
            downloader = ErosDownloader(parsed_config)
        elif dset == "eros_hyperion":
            downloader = ErosHyperionDownloader(parsed_config)
        else:
            raise NotImplementedError("Downloader {} not implemented. Valid options are maxar_api, maxar_zip, aviris, and eros.".format(dset))
    
        downloader.download(max_items=parsed_config["max_downloads"])
    
    if(args.chip):
        #tset = parsed_config["tiling_method"]
        #datapath = parsed_config["datapath"]
        chipper = BaseChipper(parsed_config)
        chipper.chip()
