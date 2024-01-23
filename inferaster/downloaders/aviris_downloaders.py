
from inferaster.utils.geo_shapes import WgsPoint, GeoBBox
from inferaster.downloaders.data_downloader import DataDownloader,Entry
from typing import List
import requests
import urllib
import csv
import os
from shapely.geometry import Polygon, Point, box
from datetime import *
import tarfile
import glob 
import spectral
from scipy import ndimage
from rasterio.control import GroundControlPoint
from rasterio.transform import from_gcps
import rasterio
from skimage.transform import resize
import numpy as np
import sys

class AvirisDownloader(DataDownloader):
    def __init__(self, parsed_config) -> None:
        super().__init__(parsed_config)

        self.max_dl_size = parsed_config['download-size']['max']
        self.min_dl_size = parsed_config['download-size']['min']
        self.max_step_size = parsed_config['step-size']['max']
        self.min_step_size = parsed_config['step-size']['min']

    
    # og https://docs.google.com/spreadsheets/d/1Mu3lJDsQK2p5UljOMbwrsGUmHL5uQHofwhERIC_esAw/edit#gid=1788431533
    # csv https://docs.google.com/spreadsheets/d/e/2PACX-1vQimCBBALJHXrUz9Z7xXEjwWuWidBEUir3GNCW6aj0-efXLsNh2-9IIBGHmPX7Mlj6tCm3HfQ-hFh01/pub?gid=1788431533&single=true&output=csv
    def get_image_data_list(self, max_items) -> List[dict]:
        """gets the list of image data from the AVIRIS data list as of 2023 from https://aviris.jpl.nasa.gov/dataportal/.

        Args:
            max_items : This lists the max number of geotiffs (or up to as many available) per set of bounds in the aviris yaml

        Returns:
            List[dict]: returns the desired dict as requested by data_downloader as well it contains all needed data from the data_trawler
        """

        og_link = 'https://docs.google.com/spreadsheets/d/1Mu3lJDsQK2p5UljOMbwrsGUmHL5uQHofwhERIC_esAw/edit#gid=1788431533'
        csv_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQimCBBALJHXrUz9Z7xXEjwWuWidBEUir3GNCW6aj0-efXLsNh2-9IIBGHmPX7Mlj6tCm3HfQ-hFh01/pub?gid=1788431533&single=true&output=csv'
        #TODO currently uses a non updating file need to use og link
        temp_csv_loc = '/tmp/Arvis_data.csv'
        response = requests.get(csv_link)
        data = urllib.request.urlretrieve(csv_link, temp_csv_loc)
        
        # wget_res = wget.download(og_link) didnt work
        assert response.status_code == 200, 'Wrong status code did not connect to ARVIS Data'

        labels, data=self.import_data(temp_csv_loc)

        self.start_date = self.config['time_range']['start_date']
        self.end_date = self.config['time_range']['end_date']
        want_coast = self.config['costal_only']
        band_sets = self.config['bands']
        bands = []
        for set in band_sets:
            band = range(set[0],set[1])
            bands.append(band)

        self.desired_output = bands
        aviris_dict_list =[]
        bound_sets = self.config['bounding_box_set']
        for location in bound_sets.items():
            location = location[1]
            datal = self.extract_locations(data, labels,location)
            datad = self.extract_dates(datal, labels,self.start_date, self.end_date)
            datadl = self.extract_size_dl(datad, labels, self.max_dl_size, self.min_dl_size)
            datasi = self.extract_size(datadl,labels, self.max_step_size, self.min_step_size)
            # datat = self.is_downloadable(datasi,labels)
            datac = self.on_coast(datasi,labels,want_coast,max_items)
            

            datas = datac
            set_total =0
            
            for i,info in enumerate(datas):
                if set_total< max_items:
                    saved_info = {}
                    err_sum = 0
                    for label in labels:
                        saved_info[label] = info[labels.index(label)]
                    url = saved_info['link_ftp']
                    r = requests.get(url,stream=True)
                    if r.status_code == 200:                    
                        name = "{}-{}".format(saved_info["Site Name"].replace('/','-'), saved_info["Date"].replace('/','-'))
                        uid = saved_info["Name"].replace('/','-')
                        relpath = os.path.join(self.datapath, self.full_tiff_dir, name + ".tiff")
                        required_metadata = {}
                        required_metadata['channels'] = {0:'hyperspectral'}
                        required_metadata['dataset'] = 'Aviris'
                        required_metadata['gsd_m'] = saved_info['Pixel Size']
                        required_metadata['date_collected'] = saved_info['Date']
                        full_metadata = saved_info
                        entry= Entry(name,uid,relpath,required_metadata,full_metadata)

                        if not entry in aviris_dict_list:
                            aviris_dict_list.append(entry)
                            set_total=+1
                    elif r.status_code == 403:
                        err_sum+=1


                    else:
                        print('image has access code ' + str(r.status_code))
                else:
                    print(str(err_sum) + " with access code 403")
                    break



        return aviris_dict_list
    
    def on_coast(self,datas,label,run,max_runs):
        """this runs though the data and determines if any of the geotiffs is on a coast. This is done though the https://osmdata.openstreetmap.de/data/coastlines.html 
        database of coastlines. 

        Args:
            datas : the data must contain all the data from the AVIRIS dataset
            label : the labels from the AVIRIS dataset
            run : If true retuns only data from coastlines
            max_runs :maximum ammount from the coastline desired 

        Returns:
            _type_: _description_
        """
        if run:
            import geopandas as gpd
            from shapely.geometry import Polygon
            data_update = []
            data_loc = 'data_trawler/utils/coastlines-split-4326/lines.shp'
            gdf = gpd.read_file(data_loc)
            i = 0
            for data in datas:
                loc = (float(data[40]),float(data[44])),(float(data[41]),float(data[45])),(float(data[42]),float(data[46])),(float(data[43]),float(data[47]))
                shape = Polygon(loc)
                if any(gdf.intersects(shape)):
                    good = True
                    print(data[36])
                    if good: 
                        data_update.append(data)   
                        print('good')
                if len(data_update)>=max_runs:
                    break
                i = i+1
        else:
            data_update = datas
        return data_update
    
    def is_downloadable(self,datas,labels):
        loc = labels.index('link_ftp')
        data_update = []
        for data in datas:
            url = data[loc]
            try:
                r = requests.get(url,stream=True)
                if r.status_code == 200:
                    data_update.append(data)   
            except:
                continue
        return data_update




    def download_one(self, entry:dict):
        """This dowloads one hyperspectral image from Aviris then changes the format to a geotiff. 

        Args:
            entry (dict): expects one set of data from the Aviris dataset

        Raises:
            requests.HTTPError: couldnot download the image requested
        """
        #Download_zip portion
        temp_save = '/tmp'
        url = entry.full_metadata['link_ftp']
        uid= entry.full_metadata['DownloadName']
        name = "{}-{}".format(entry.full_metadata["Site Name"].replace('/','-'), entry.full_metadata["Date"].replace('/','-'))
        r = requests.get(url,stream=True)

        zip_file = os.path.join(temp_save, uid)
        if r.status_code == 200:
            f = open(zip_file, 'wb')
            for chunk in r.iter_content(chunk_size=512 * 1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
            f.close()
            print('downloaded ' + uid)
        else:
            print('failed downloading ' + uid +' errorcode :' + str(r.status_code))
            raise requests.HTTPError
            
        # unzip portion 
        # TODO Where is extract location
        

        uid = uid.split('.')[0]
        Hyper_img = 'ort_img'
        header = Hyper_img + '.hdr'
        t = tarfile.open(zip_file, 'r')
        #extract_location =  os.path.join(self.datapath, self.full_tiff_dir, name)
        extract_path = os.path.join(self.datapath, "leftovers/", "aviris", uid)
        tiff_path = os.path.join(self.datapath, self.full_tiff_dir, name.replace('/','-') + ".tiff")

        if not os.path.exists(extract_path):
            os.makedirs(extract_path) 
        for member in t.getmembers():
            if Hyper_img in member.name:
                t.extract(member, extract_path)
                if header in member.name:
                    t.extract(member, extract_path)
                    
        os.remove(zip_file)
        
        # create geotiff poriton
        path = os.path.join(extract_path,uid)
        header_path = glob.glob(path+'*/*.hdr')[0]
        img = spectral.open_image(header_path)
    
        file_save = os.path.join(extract_path,'raw_geotiff')
        if not os.path.exists(file_save):
            os.makedirs(file_save)
        save = os.path.join(file_save, uid + '_geotiff.tiff') 
        image = img._memmap
        # angle = float(entry.full_metadata['Rotation'])
        # cur_size = float(entry.full_metadata['Pixel Size'])
        # size_goal = float(entry.full_metadata['Pixel Size'])
        # scale = size_goal/cur_size
        # img_scale=self.img_scale(image[:,:,1], scale)


        # rotated_img_layer= ndimage.rotate(img_scale, angle)
        


        lat1 = float(entry.full_metadata['Lat1'])
        long1 = float(entry.full_metadata['Lon1'])
        lat2 = float(entry.full_metadata['Lat2'])
        long2 = float(entry.full_metadata['Lon2'])
        lat3 = float(entry.full_metadata['Lat3'])
        long3 = float(entry.full_metadata['Lon3'])
        lat4 = float(entry.full_metadata['Lat4'])
        long4 = float(entry.full_metadata['Lon4'])
        # max_lon = max(long1,long2,long3,long4)
        # min_lon = min(long1,long2,long3,long4)
        # max_lat = max(lat1,lat2,lat3,lat4)
        # min_lat = min(lat1,lat2,lat3,lat4)
        # width = rotated_img_layer.shape[1]
        # height = rotated_img_layer.shape[0]    
        # area = GeoBBox((max_lon, max_lat), (min_lon, min_lat))

        bl = GroundControlPoint(img.nrows, 0, long1, lat1)
        br = GroundControlPoint(img.nrows, img.nbands, long2, lat2)
        tr = GroundControlPoint(0, img.nbands, long3, lat3)
        tl = GroundControlPoint(0, 0, long4, lat4 )

        counts = 0
    
        for sub in self.desired_output:
            counts+= len(sub)


        gcps = [tl, bl, br, tr]

        transform = from_gcps(gcps)
        crs = 'epsg:4326'

        # with rio.open(filepath, 'r+') as ds:
        #     ds.crs = crs
        #     ds.transform = transform
        with rasterio.open(
        save,
        'w',
        driver='GTiff',
        height=img.nrows,
        width=img.nbands,
        count=counts,
        dtype=img._memmap[0,0,0].dtype,
        crs=crs,
        transform=transform,
        ) as dst:
            i = 0
            for sub in self.desired_output:
                for set in sub:
                    i +=1
                    # sub_img=self.cal_sub(image[:,:,set])
                    # scale_img = self.img_scale(sub_img,scale)
                    # rotated_img= ndimage.rotate(scale_img, angle)
                    dst.write(image[:,:,set], i)
                    

        print(uid + '-geotiff created')
        os.rename(save, tiff_path)




        #TODO Lots of black space within geotiff
        pass

    # def update_metadata(self, entry:dict):

    #     self.name = "{}-{}".format(entry["Site Name"], entry["Date"].replace('/','-'))
    #     self.uid = entry["Name"]
    #     self.relpath = os.path.join(self.datapath, self.full_tiff_dir, self.name + ".tiff")
    #     self.required_metadata = {}
    #     self.required_metadata['channels'] = {0:'hyperspectral'}
    #     self.required_metadata['dataset'] = 'Aviris'
    #     self.required_metadata['gsd_m'] = entry['Pixel Size']
    #     self.required_metadata['date_collected'] = entry['Date']
    #     self.full_metadata = entry
    #     self.write_to_metadata_json(name, uid, relpath, required_metadata, full_metadata)

    def login(self):
        pass

    def import_data(self, data_loc):
        """Reads and extracts csv data from Aviris dataset. Currently It is downolading from a csv that is on a drive of Isaac Ege. 
        (that is currenlty saved in tmp)

        Args:
            data_loc : the location of csv data that contains the Aviris data

        Returns:
            data_updated : the information of the csv as header and data
        """
        file = open(data_loc)
        csvreader = csv.reader(file)
        header = []
        header = next(csvreader)
        data = []
        for row in csvreader:
            data.append(row)    
        return header, data 
    
    def extract_locations(self, data, labels, square):
        """extract the location of all the data and compares it to the data

        Args:
            data : a set of all of the data from Aviris
            labels : the labels from the Avirs data
            square : the bbox of the desired square for the data

        Returns:
            data_updated : a sub set of the data dictionary
        """
        
        area_want = square
        result = []
        for i in range(len(data)):
            Lon_lab = labels.index('Lon1')
            max_lon = float(max(data[i][Lon_lab:Lon_lab+4]))
            min_lon = float(min(data[i][Lon_lab:Lon_lab+4]))
            max_lat = float(max(data[i][Lon_lab+4:Lon_lab+8]))
            min_lat = float(min(data[i][Lon_lab+4:Lon_lab+8]))
            area = GeoBBox((max_lon, max_lat), (min_lon, min_lat))
            if area_want.intersects(area):
                result.append(data[i])
        return result
    
    def extract_dates(self, data,labels,start_date,end_date):
        """extracts the data based of the desired dates 

        Args:
            data : a set of all of the data from Aviris
            labels : the labels from the Avirs data
            start_date (string) : a date in the form of year-month-day ex'2000-12-10'
            end_date : a date in the form of year-month-day ex'2000-12-10'

        Returns:
            data_updated : a sub set of the data dictionary
        """

        Y, M, D = start_date.split('-')
        start_date = date(int(Y), int(M), int(D))
        Y, M, D = end_date.split('-')
        end_date = date(int(Y), int(M), int(D))
        result = []
        for i in range(len(data)):
            year_lab = labels.index('Year')
            Y = int(data[i][year_lab])
            month_lab = labels.index('Month')
            M = int(data[i][month_lab])
            day_lab = labels.index('Day')
            D = int(data[i][day_lab])
            

            imp_date = date(int(Y), int(M), int(D))
            if imp_date < end_date and start_date < imp_date:
                result.append(data[i])

        return result
    
    def extract_size(self,data,labels,max_size,min_size):
        """extracts the data based of the desired ground sample density

        Args:
            data : a set of all of the data from Aviris
            labels : the labels from the Avirs data
            max_size (float) : the file size in terms of pixel/meter
            min_size (float) : the file size in terms of pixel/meter

        Returns:
            data_updated : a sub set of the data dictionary
        """
        size_loc = labels.index('Pixel Size')
        result = []
        for i in range(len(data)):
            size = float(data[i][size_loc])
            if size < max_size and size > min_size:
                result.append(data[i])
        
        return result

    def extract_size_dl(self, data, labels, max_size, min_size):
        """extracts the data based of the desired file size

        Args:
            data : a set of all of the data from Aviris
            labels : the labels from the Avirs data
            max_size (float) : the file size in terms of GB
            min_size (float) : the file size in terms of GB

        Returns:
            data_updated : a sub set of the data dictionary
        """
        size_loc = labels.index('File Size (GB)')
        result = []
        for i in range(len(data)):
            size = float(data[i][size_loc])
            if size < max_size and size > min_size:
                result.append(data[i])
        return result
    
