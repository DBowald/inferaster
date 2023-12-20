from inferaster.utils.geo_shapes import WgsPoint
from geopy.distance import geodesic

def move_along_parallel(pt, m) -> WgsPoint:
    # -m is west, +m is east
    parallel_pm = WgsPoint(0, pt.lat)
    parallel_1deg = WgsPoint(1, pt.lat)
    deg_per_m = 1/(geodesic(parallel_pm, parallel_1deg).m)
    new_lon = pt.longitude + (deg_per_m * m)
    return WgsPoint(new_lon, pt.lat)

def move_along_meridian(pt, m) -> WgsPoint:
    # -m is south, +m is north
    # 110947.2
    """
    meridian_eq = geopy.Point(0,pt.longitude)
    meridian_1deg = geopy.Point(1, pt.longitude)
    deg_per_m = 1/geopy.distance.geodesic(meridian_eq, meridian_1deg).m
    new_lat = pt.latitude + (deg_per_m * m)
    return geopy.Point(new_lat, pt.longitude)
    """
    # geodesic seems to be more accurate, and bearing is always 0
    dist = geodesic(meters=m)
    dst = dist.destination(pt, bearing=0)
    return WgsPoint(dst.longitude, dst.latitude)
