#import Tiles
#from Tiles import Tile
from typing import Tuple, List
import numpy as np
import abc
from geopy.distance import geodesic
import geopandas as geopd
import matplotlib.pyplot as plt

from data_trawler.utils.great_circles import move_along_meridian, move_along_parallel
from data_trawler.utils.geo_shapes import WgsBBox, WgsPoint, GeoPoint, GeoBBox

"""
class OsmTile(WgsBBox):
    def __init__(self, id_x, id_y) -> None:
        # Tile is just a WgsBBox with tile ids
        super().__init__(nw, se)
        self.id_x = id_x
        self.id_y = id_y
"""

def plot_shapely_geo(shapely_list, continent="North America", name="United States of America"):
    #gdf = geopd.GeoDataFrame(geometry=shapely_list)
    gdf = geopd.GeoDataFrame(geometry=[WgsBBox((-100.0, 50.0), (-60.0, 30.0))])
    world = geopd.read_file(geopd.datasets.get_path('naturalearth_lowres'))

    # We restrict to South America.
    ax = world[world.continent == continent].plot(
        color='white', edgecolor='black')

    # We can now plot our ``GeoDataFrame``.
    gdf.plot(ax=ax, color='red')
    plt.show()

def plot_shapely(shapely_list, continent="North America", name="United States of America"):
    for each_shapely_obj in shapely_list:
        plt.plot(*each_shapely_obj.exterior.xy)

    plt.show()
class TileID(GeoPoint):
    def __init__(self, id_x:int, id_y:int):
        assert(type(id_x) == int and type(id_y) == int)
        super().__init__(id_x, id_y)
    
    @property
    def x(self):
        return int(super().x)
    
    @property
    def y(self):
        return int(super().y)
"""
class TileIDBBox(GeoBBox):
    def __init__(self, id_1:TileID, id_2:TileID):
        super().__init__(id_1, id_2)

        self.north = int(self.north)
        self.east = int(self.east)
        self.south = int(self.south)
        self.west = int(self.west)

    @property
    def nw(self):
        return TileID(self.west, self.north)
    
    @property
    def se(self):
        return TileID(self.east, self.south)
    
    @property
    def ne(self):
        return TileID(self.east, self.north)
    
    @property
    def sw(self):
        return TileID(self.west, self.south)
    
    @property
    def geo_bounds(self):
        return {"north": self.north, "west": self.west,
                "south": self.south, "east": self.east} 
"""   

"""
class TileID(GeoPoint):
    def __init__(self, id_x:int, id_y:int):
        assert(type(id_x) == int and type(id_y) == int)
        super().__init__(id_x, id_y)
    
    @property
    def x(self):
        return int(super().x)
    
    @property
    def y(self):
        return int(super().y)
"""
"""
class TileBBox(GeoBBox):
    def __init__(self, id_1:TileID, id_2:TileID):
        super().__init__(id_1, id_2)
    
    @property
    def north(self):
        return int(super().north)
    
    @property
    def east(self):
        return int(super().east)
    
    @property
    def south(self):
        return int(super().south)
    
    @property
    def west(self):
        return int(super().west)
    
    @property
    def nw(self):
        return TileID(self.west, self.north)
    
    @property
    def se(self):
        return TileID(self.east, self.south)
    
    @property
    def ne(self):
        return TileID(self.east, self.north)
    
    @property
    def sw(self):
        return TileID(self.west, self.south)
    
    @property
    def geo_bounds(self):
        return {"north": self.north, "west": self.west,
                "south": self.south, "east": self.east}
"""

class Tile(WgsBBox):
    def __init__(self, id:TileID, parent_tileset) -> None:
        if(type(id) == tuple):
            id = TileID(id[0], id[1])
        assert(type(id.x) == int and type(id.y) == int)
        bounds = parent_tileset.get_wgs_box_by_id(id)
        super().__init__(bounds.nw, bounds.se)
        self.id = id
        self.parent_tileset = parent_tileset

    @property
    def x(self):
        return self.id.x

    @property
    def y(self):
        return self.id.y


class Tileset(abc.ABC):
    def __init__(self, bounds: WgsBBox) -> None:#, zoom: int) -> None:
        self.bounds = bounds
    
    @abc.abstractmethod
    def get_x_by_lon_lat(self, lon_deg, lat_deg)->int:
        pass

    @abc.abstractmethod
    def get_y_by_lat(self, lat_deg)->int:
        pass

    @abc.abstractmethod
    def get_wgs_box_by_id(self, id:TileID) -> WgsBBox:
        pass

    # TODO this should really probably be a generator instead of a list
    # TODO Or maybe this should be a geopandas df
    def get_tiles_from_wgs_bbox(self) -> List[Tile]:
        tile_list = []
        tile_bound_north = self.get_y_by_lat(self.bounds.north)
        tile_bound_south = self.get_y_by_lat(self.bounds.south)
        for y in range(tile_bound_north, tile_bound_south - 1, -1):
            # TODO: refactor this
            pm_tile = Tile(TileID(0, y), self)
            curr_lat = pm_tile.north
            tile_bound_west = self.get_x_by_lon_lat(self.bounds.west, curr_lat)
            tile_bound_east = self.get_x_by_lon_lat(self.bounds.east, curr_lat)
            for x in range(tile_bound_west, tile_bound_east + 1):
                #tile_id = GeoPoint(x,y)
                id = TileID(x, y)
                new_tile = Tile(id, self)
                tile_list.append(new_tile)
        return tile_list
    
    def get_tileid_by_wgs(self, wgs_pt:WgsPoint) -> TileID:
        id_x = self.get_y_by_lat(wgs_pt.lat)
        id_y = self.get_x_by_lon_lat(wgs_pt.lon, wgs_pt.lat)
        return TileID(id_x, id_y)
    
    def get_nw_by_id(self, id:TileID) -> WgsPoint:
        return self.get_wgs_box_by_id(id).nw
    
    def get_tile_by_id(self, id:TileID):
        return Tile(id, self)
    
    def plot_tiles(self):
        plot_shapely(self.get_tiles_from_wgs_bbox())

class OsmTileset(Tileset):
    """_summary_
    """
    def __init__(self, bounds: WgsBBox, zoom: int) -> None:
        super().__init__(bounds)
        self.zoom = zoom
        #self.tile_id_bbox = self.get_tile_id_bbox()

    def get_tiles_from_wgs_bbox(self) -> List[Tile]:
        tile_list = []
        tile_bound_north = self.get_y_by_lat(self.bounds.north)
        tile_bound_south = self.get_y_by_lat(self.bounds.south)
        # Stupid OSM defining south as positive
        for y in range(tile_bound_north, tile_bound_south + 1):
            # TODO: refactor this
            pm_tile = Tile(TileID(0, y), self)
            curr_lat = pm_tile.north
            tile_bound_west = self.get_x_by_lon_lat(self.bounds.west, curr_lat)
            tile_bound_east = self.get_x_by_lon_lat(self.bounds.east, curr_lat)
            for x in range(tile_bound_west, tile_bound_east + 1):
                #tile_id = GeoPoint(x,y)
                id = TileID(x, y)
                new_tile = Tile(id, self)
                tile_list.append(new_tile)
        return tile_list
    
    def get_y_by_lat(self, lat_deg):
        lat_rad = np.radians(lat_deg)
        id_y = int(np.floor((1.0 - np.arcsinh(np.tan(lat_rad)) / np.pi) / 2.0 * (2.0 ** self.zoom)))
        return id_y
    
    def get_x_by_lon_lat(self, lon_deg, lat_deg):
        lat_rad = np.radians(lat_deg)
        id_x = int(np.floor((lon_deg + 180.0) / 360.0 * (2.0 ** self.zoom)))
        return id_x
    
    #def get_tileid_by_wgs(self, wgs_pt:WgsPoint) -> TileID:
    #    id_x = self.get_y_by_lat(wgs_pt.lat)
    #    id_y = self.get_x_by_lon_lat(wgs_pt.lon, wgs_pt.lat)
    #    return TileID(id_x, id_y) 

    """
    def get_tileid_by_wgs(self, wgs_pt:WgsPoint#lon_deg: float, lat_deg: float) -> TileID:#Tuple[int, int]:

        Args:
            lat_deg (float): _description_
            lon_deg (float): _description_
            zoom (int): _description_

        Returns:
            Tuple[int, int]: _description_

        lat_rad = np.radians(lat_deg)
        id_x = int(np.ceil((lon_deg + 180.0) / 360.0 * (2.0 ** self.zoom)))
        id_y = int(np.floor((1.0 - np.arcsinh(np.tan(lat_rad)) / np.pi) / 2.0 * (2.0 ** self.zoom)))
        return TileID(id_x, id_y)
    """


    def get_wgs_box_by_id(self, id:TileID) -> WgsBBox:
        """_summary_

        Args:
            tile_id_x (int): _description_
            tile_id_y (int): _description_
            zoom (int): _description_

        Returns:
            Tuple[float, float, float, float]: _description_
        """
        west_deg = id.x / (2.0 ** self.zoom) * 360.0 - 180.0
        north_rad = np.arctan(np.sinh(np.pi * (1 - 2 * id.y / (2.0 ** self.zoom))))
        north_deg = np.degrees(north_rad)
        
        east_deg = (id.x + 1) / (2.0 ** self.zoom) * 360.0 - 180.0
        south_rad = np.arctan(np.sinh(np.pi * (1 - 2 * (id.y+1) / (2.0 ** self.zoom))))
        south_deg = np.degrees(south_rad)
        wgs_bbox = WgsBBox((west_deg, north_deg), (east_deg, south_deg))
        return wgs_bbox
    
    # def get_nw_by_id(self, id:TileID) -> WgsPoint:
    #     return self.get_wgs_box_by_id(id).nw
    
    # def get_tile_by_id(self, id:TileID):
    #    return Tile(id, self)
    
    def __str__(self) -> str:
        return str({k: str(v) for (k, v) in vars(self).items()})
    
class EquiviTilesTileset(Tileset):
    def __init__(self, bounds: WgsBBox, chip_size_m) -> None:
        super().__init__(bounds)
        self.chip_size_m = chip_size_m

    def get_x_by_lon_lat(self, lon_deg, lat_deg)->int:
        lat_parallel = WgsPoint(0.0, lat_deg)
        lon_degpm = geodesic(lat_parallel, WgsPoint(1, lat_deg)).m
        lon_dist_m = abs(lon_degpm * lon_deg)
        tile_id_x = np.sign(lon_deg) * np.floor(lon_dist_m/self.chip_size_m)
        #tile_id_x = np.ceil(lon_dist_m/self.chip_size_m)
        return int(tile_id_x)

    def get_y_by_lat(self, lat_deg)->int:
        origin = WgsPoint(0, 0)
        lat_parallel = WgsPoint(0.0, lat_deg)
        lat_dist_m = abs(geodesic(origin, lat_parallel).m)
        tile_id_y = np.sign(lat_deg) * np.ceil(lat_dist_m/self.chip_size_m)
        #tile_id_y = np.ceil(lat_dist_m/self.chip_size_m)
        return int(tile_id_y)

    def get_wgs_box_by_id(self, id:TileID) -> WgsBBox:
        origin = WgsPoint(0, 0)
        lat_dist = id.y * self.chip_size_m
        lat_parallel_pm = move_along_meridian(origin, lat_dist)
        assert(lat_parallel_pm.longitude == 0.0)

        lon_dist = self.chip_size_m * id.x
        nw_pt = move_along_parallel(lat_parallel_pm, lon_dist)
        assert(nw_pt.latitude == lat_parallel_pm.latitude)
        sw_pt = move_along_meridian(nw_pt, -self.chip_size_m)
        se_pt = move_along_parallel(sw_pt, self.chip_size_m)
        return WgsBBox(nw_pt, se_pt)


if __name__ == "__main__":

    #wgs_bbox_ex = WgsBBox((-84.375, 39.90973623453718), (-83.671875, 39.36827914916013))
    #import geopy
    #geopy.Point(1.0,2.0)
    b = GeoPoint(-95.50, 11.50)
    c = WgsPoint(b.east, b.north, 10)
    wgs_bbox_ex = WgsBBox((-84.2105, 39.7610), (-84.1857, 39.7515))
    print(wgs_bbox_ex)
    #tset = OsmTileset(wgs_bbox_ex, 7)
    tset = EquiviTilesTileset(wgs_bbox_ex, 200)
    tile_list = tset.get_tiles_from_wgs_bbox()
    for x in tile_list:
        print(x)
    print("total tiles: {}".format(len(tile_list)))
    print("hi")
