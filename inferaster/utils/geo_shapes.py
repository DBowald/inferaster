import shapely
import shapely.geometry
from shapely.geometry import Polygon, Point, box
import warnings
import geopy
import geopy.distance
from shapely.errors import ShapelyDeprecationWarning
import re

warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

def format_latlon(deg_f):
    return re.sub("0*$", " ", "{:3.6f}".format(deg_f))

class GeoPoint(Point):
    def __init__(self, *args):
        super().__init__(*args)
    
    @property
    def north(self):
        return self.y

    @property
    def east(self):
        return self.x
    
    @property
    def down(self):
        return self.z
    
    def __str__(self):
        deg_e = format_latlon(self.east)
        deg_n = format_latlon(self.north)
        return "GeoPoint({}E, {}N)".format(deg_e, deg_n)
    
    def __repr__(self):
        return self.__str__()
    
    def __getitem__(self, idx):
        #assert(idx < 2)
        if idx == 0: 
            return self.east
        elif idx == 1: 
            return self.north
        elif idx == 2: 
            return self.down
    
    def __len__(self):
        return 2

class WgsPoint(GeoPoint, geopy.Point):
    def __new__(cls, lon=None, lat=None, *args):
        # geopy takes lat lon order
        return super().__new__(cls, lat, lon, *args)

    def __init__(self, *args) -> None:
        # Note geopy.Point initializes in __new__, shapely in __init__
        # Just to annoy me
        super(GeoPoint, self).__init__(*args)
    
    @property
    def lat(self):
        return self.latitude
    
    @lat.setter
    def lat(self, value: float):
        self.latitude = value
    
    @property
    def lon(self):
        return self.longitude
    
    @lon.setter
    def lon(self, value: float):
        self.longitude = value
    
    def __str__(self):
        deg_e = format_latlon(self.east)
        deg_n = format_latlon(self.north)
        return "WgsPoint({}°E, {}°N)".format(deg_e, deg_n)
    
class GeoBBox(Polygon):
    def __init__(self, pt1, pt2) -> None:
        n,w,s,e = self.calc_bounds(pt1, pt2)
        super().__init__(box(minx=w, maxy=n, 
                        maxx=e, miny=s))
        
        self.north = n
        self.west = w
        self.south = s
        self.east = e
    
    def calc_bounds(self, pt1, pt2):
        assert(pt1[0] != pt2[0] and pt1[1] != pt2[1])
        n,s = (pt1[1], pt2[1]) if pt1[1] > pt2[1] else (pt2[1], pt1[1])
        e,w = (pt1[0], pt2[0]) if pt1[0] > pt2[0] else (pt2[0], pt1[0])
        return n,w,s,e
    
    @property
    def geo_bounds(self):
        return {"west": self.west, "north": self.north, 
                "east": self.east, "south": self.south, }
    @property
    def nw(self):
        return GeoPoint(self.west, self.north)
    
    @property
    def se(self):
        return GeoPoint(self.east, self.south)
    
    @property
    def ne(self):
        return GeoPoint(self.east, self.north)
    
    @property
    def sw(self):
        return GeoPoint(self.west, self.south)
    
    def __str__(self):
        type_string = type_string = str(type(self))
        s = type_string.split('.')[-1].split('\'')[0] + '('
        for (k,v) in self.geo_bounds.items():
            s += "{}: {} ".format(k, format_latlon(v))
        return s + ')'
    
    def __repr__(self):
        return self.__str__()

class WgsBBox(GeoBBox):
    def __init__(self, pt1, pt2) -> None:
        super().__init__(pt1, pt2)
        """
        if type(NW) != WgsPoint or type(SE) != WgsPoint:
            self.NW = WgsPoint(self.NW)
            self.SE = WgsPoint(self.SE)
        """
    @property
    def nw(self):
        return WgsPoint(self.west, self.north)
    
    @property
    def se(self):
        return WgsPoint(self.east, self.south)

    @property
    def sw(self):
        return WgsPoint(self.west, self.south)
    
    @property
    def ne(self):
        return WgsPoint(self.east, self.north)

if __name__ == "__main__":
    ul = GeoPoint(0.12313123,0.423455624)
    br = GeoPoint(10.1241214,20.1241243124)
    bbox = GeoBBox(ul, br)
    up = GeoPoint(0.123566879,10.1223243347)
    print(up)
    print(bbox)