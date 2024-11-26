import argparse
import json
import random
import zlib
import argparse

import numpy as np
import skimage.draw
from tqdm import trange

from _util import to_bytes, from_bytes, SURFACES, DECORATIONS


HIGHWAY_WIDTHS = {
    "footway": 3,
    "service": 4,
    "cycleway": 3,
    "pedestrian": 3,
    "residential": 5,
    "path": 3,
    "primary": 6,
    "secondary": 6
}


def fit_array(a1, a1_min_x, a1_min_y, a2_min_x, a2_min_y, a2_size_x, a2_size_y):
    # fit array a1 into boundaries of a2
    offset_x = 0
    if a2_min_x < a1_min_x:
        offset_x = a1_min_x-a2_min_x
    elif a2_min_x > a1_min_x:
        a1 = a1[:, (a2_min_x-a1_min_x):]

    offset_y = 0
    if a2_min_y < a1_min_y:
        offset_y = a1_min_y-a2_min_y
    elif min_y > a1_min_y:
        a1 = a1[(a2_min_y-a1_min_y):, :]

    if a1.shape[1]+offset_x > a2_size_x:
        a1 = a1[:, :a2_size_x-offset_x]

    if a1.shape[0]+offset_y > a2_size_y:
        a1 = a1[:a2_size_y-offset_y, :]
    return a1, offset_x, offset_y


parser = argparse.ArgumentParser(description="Generate a map.dat file that can be read by world2minetest Mod")
parser.add_argument("--heightmap", type=argparse.FileType("rb"), help="Heightmap file generated by parse_heightmap_xyz.py", default=None)
parser.add_argument("--features", action="append", type=argparse.FileType("r"), help="features.json files generated by parse_features_osm.py or parse_features_dxf.py. Features of the same type in files specified earlier will be overridden.", default=None)
parser.add_argument("--buildings", type=argparse.FileType("rb"), help="buildings_cityjson.dat file generated by parse_cityjson.py. If this argument is used, buildings stored in a --features file will be ignored.", default=None)
parser.add_argument("--buildings-base-height", type=int, help="Subtracted from the height of every building. Defaults to 0.", default=0)
parser.add_argument("--incr", action="store_true", help="Add incremental map information to map.dat. Load new map data using the '/w2mt:incr' command. Use with caution and make a backup beforehand.")
parser.add_argument("--offsetx", type=int, help="EPSG:25832 x coordinate that will be x=0 in Minetest", default=0)
parser.add_argument("--offsetz", type=int, help="EPSG:25832 y coordinate that will be z=0 in Minetest (y is z in Minetest)", default=0)
parser.add_argument("--minx", type=int, help="Minimum EPSG:25832 x coordinate", default=0)
parser.add_argument("--maxx", type=int, help="Maximum EPSG:25832 x coordinate", default=0)
parser.add_argument("--miny", type=int, help="Minimum EPSG:25832 y coordinate (y is z in Minetest)", default=0)
parser.add_argument("--maxy", type=int, help="Maximum EPSG:25832 y coordinate (y is z in Minetest)", default=0)
parser.add_argument("--noheightreduction", action="store_true", help="Do not subtract the smallest height from every heightmap value")
parser.add_argument("--flat", action="store_true", help="If a --heightmap is specified, make the world flat, but subtract the heightmap value from each building coordinate")
parser.add_argument("--createimg", action="store_true", help="Create a .png visualization of every layer")
parser.add_argument("--verbose", "-v", action="store_true", help="More debug info")
parser.add_argument("--output", "-o", type=str, help="Output file. Defaults to world2minetest/map.dat", default="world2minetest/map.dat")

args = parser.parse_args()

min_x = args.minx
max_x = args.maxx
min_y = args.miny
max_y = args.maxy

if (args.heightmap is None or args.flat) and args.features is None:
    raise argparse.ArgumentTypeError("at least one of --heightmap (without --flat) or --features is required.")

if args.heightmap is not None:
    heightmap_min_x = from_bytes(args.heightmap.read(4))
    heightmap_min_y = from_bytes(args.heightmap.read(4))
    heightmap_size_x = from_bytes(args.heightmap.read(2))
    heightmap_size_y = from_bytes(args.heightmap.read(2))

    min_x = min_x if min_x is not None else heightmap_min_x
    max_x = max_x if max_x is not None else (heightmap_min_x+heightmap_size_x-1)
    min_y = min_y if min_y is not None else heightmap_min_y
    max_y = max_y if max_y is not None else (heightmap_min_y+heightmap_size_y-1)

    heightmap = np.frombuffer(zlib.decompress(args.heightmap.read()), dtype=np.uint8).reshape((heightmap_size_y, heightmap_size_x))
    size = (max_x-min_x+1, max_y-min_y+1)
    heightmap, h_offset_x, h_offset_y = fit_array(heightmap, heightmap_min_x, heightmap_min_y, min_x, min_y, size[0], size[1])
else:
    heightmap = None


features = {
    "areas": [],
    "highways": [],
    "buildings": [],
    "decorations": {}
}

for file in args.features or []:
    data = json.load(file)
    min_x = min_x if min_x is not None else data["min_x"]
    max_x = max_x if max_x is not None else data["max_x"]
    min_y = min_y if min_y is not None else data["min_y"]
    max_y = max_y if max_y is not None else data["max_y"]
    for key in features.keys():
        if key != "decorations":
            if key in data and data[key]:
                features[key] = data[key]
        else:
            if "decorations" in data and data["decorations"]:
                for key, value in data["decorations"].items():
                    if value:
                        features["decorations"][key] = value


size = (max_x-min_x+1, max_y-min_y+1)

print(f"from {min_x},{min_y} to {max_x},{max_y} (size: {size[0]},{size[1]})")
if min_x > max_x or min_y > max_y:
    raise ValueError("map size is invalid")

if not (min_x <= args.offsetx <= max_x and min_y <= args.offsetz <= max_y):
    raise ValueError(f"offset {(args.offsetx, args.offsetz)} is located outside of map")


LAYER_COUNT = 4
a = np.zeros((size[1], size[0], LAYER_COUNT), dtype=np.uint8)
# bytes (one for every layer):
# byte 0: y0: heightmap; floor goes up to this block.
# byte 1: surface type (block to place at y=y0; below is always stone)
# byte 2: y1: If y1<128, this is a decoration id (block to place at y=y0+1, and sometimes above (e.g. for trees)).
#             Otherwise, y1-127 is the minimum y coordinate of a building. If the building is standing on the ground: y1=y0+127+1.
# byte 3: y2: maximum y coordinate of a building. If y2>=128, the topmost block (at y=y2) is part of a roof and the maximum y coordinate is y2-127.


# HEIGHTMAP
if heightmap is not None and not args.flat:
    if not args.noheightreduction:
        heightmap_sub = heightmap.min()
    else:
        heightmap_sub = 0
    a[h_offset_y:h_offset_y+heightmap.shape[0]+1, h_offset_x:h_offset_x+heightmap.shape[1]+1, 0] = heightmap - heightmap_sub
else:
    heightmap_sub = 0
    FLAT_HEIGHT = 50
    a[:, :, 0] = FLAT_HEIGHT  # everywhere the same height


# FEATURES
def shift_coords(x_coords, y_coords):
    if type(x_coords) is list:
        x_res = []
        y_res = []
        for x, y in zip(x_coords, y_coords):
            if min_x <= x <= max_x and min_y <= y <= max_y:
                x = x-min_x
                y = y-min_y
                x_res.append(x)
                y_res.append(y)
        return x_res, y_res
    x, y = x_coords, y_coords
    if min_x <= x <= max_x and min_y <= y <= max_y:
        return x-min_x, y-min_y
    return None, None

for area in features["areas"]:
    x, y = shift_coords(area["x"], area["y"])
    if len(x) < 3:
        if args.verbose: print("Too few coordinates, ignoring area:", x, y, area)
        continue
    surface = area["surface"]
    xx, yy = skimage.draw.polygon(x, y)
    a[yy, xx, 1] = SURFACES[surface]
    if surface in ("water", "pitch", "playground", "sports_centre", "parking"):
        assert 0 <= int(round(a[yy, xx, 0].mean())) <= 255
        a[yy, xx, 0] = int(round(a[yy, xx, 0].mean()))  # flatten area
    if surface in ("park", "village_green"):
        # add a bit of random grass
        random.seed(0)
        for x, y in zip(xx, yy):
            if random.random() < 0.025:
                a[y, x, 2] = DECORATIONS["grass"]
    else:
        a[yy, xx, 2] = 0  # if areas overlap, this removes any previously generated grass


if args.buildings:
    print("Reading buildings file")
    count_points_in_area = 0
    count_points_out_of_area = 0
    buildings_file = args.buildings
    buildings_count = from_bytes(buildings_file.read(4))
    assert from_bytes(buildings_file.read(1)) == 0
    for _ in trange(buildings_count):
        new_building = {}
        surface_name_len_bytes = buildings_file.read(1)
        while (surface_name_len := from_bytes(surface_name_len_bytes)) != 0:
            surface_name = buildings_file.read(surface_name_len).decode("utf-8")
            roof_summand = 127 if surface_name == "roof" else 0
            is_ground = surface_name == "ground"
            pos_count = from_bytes(buildings_file.read(4))
            for _ in range(pos_count):
                x = from_bytes(buildings_file.read(4))
                y = from_bytes(buildings_file.read(4))
                z = from_bytes(buildings_file.read(4))
                if min_x <= x <= max_x and min_y <= y <= max_y:
                    count_points_in_area += 1
                    x = x-min_x
                    y = y-min_y
                    z -= heightmap_sub
                    z -= args.buildings_base_height
                    if args.flat:
                        if h_offset_x <= x < h_offset_x+heightmap.shape[1] and h_offset_y <= y < h_offset_y+heightmap.shape[0]:
                            z -= heightmap[y-h_offset_y, x-h_offset_x]
                        z += FLAT_HEIGHT
                    if is_ground:
                        if not args.flat:
                            assert 0 <= z <= 255
                            a[y, x, 0] = z
                        a[y, x, 1] = SURFACES["building_ground"]
                    else:
                        if z > 0:
                            if a[y, x, 2] >= 128:
                                a[y, x, 2] = min(a[y, x, 2], 127 + z)
                            else:
                                a[y, x, 2] = 127 + z
                            a[y, x, 3] = max(a[y, x, 3], roof_summand + z)
                else:
                    count_points_out_of_area += 1
            surface_name_len_bytes = buildings_file.read(1)
    if count_points_out_of_area > 0:
        print(f"Warning: {count_points_out_of_area}/{count_points_in_area+count_points_out_of_area} building points were outside the area and skipped")
else:
    for building in features["buildings"]:
        x_coords, y_coords = shift_coords(building["x"], building["y"])
        if len(x_coords) < 2:
            if args.verbose: print("Too few coordinates, ignoring building:", x_coords, y_coords, building)
            continue
        elif len(x_coords) == 2:
            xx, yy = skimage.draw.line(x_coords[0], y_coords[0], x_coords[1], y_coords[1])
        else:
            xx, yy = skimage.draw.polygon_perimeter(x_coords, y_coords)
        height = building.get("height")
        if height is None:
            height = building.get("levels")
            if height is not None:
                height *= 3
        ground_z = int(round(a[yy, xx, 0].mean()))
        assert 0 <= ground_z <= 255
        a[yy, xx, 0] = ground_z
        a[yy, xx, 2] = 127 + ground_z + 1
        if height is not None and building.get("is_part"):
            # only overwrite height if it is likely from the same building
            assert height >= 1
            a[yy, xx, 3] = ground_z + height
        else:
            a[yy, xx, 3] = np.maximum(a[yy, xx, 3], ground_z + (height or 1))

for highway in features["highways"]:
    x_coords, y_coords = shift_coords(highway["x"], highway["y"])
    surface = highway["surface"]
    surface_id = SURFACES[surface]
    layer = highway.get("layer", 0)
    width = HIGHWAY_WIDTHS.get(highway["type"], 3)
    height = -layer*3 if layer < 0 else 0
    for i in range(0, len(x_coords)-1):
        x1, y1 = x_coords[i], y_coords[i]
        x2, y2 = x_coords[i+1], y_coords[i+1]
        xx, yy = skimage.draw.line(x1, y1, x2, y2)
        if width != 1:
            # very naive implementation for widths, improvement needed
            positions = set()
            if width == 3:
                for x, y in zip(xx, yy):
                    positions.update((
                                    (x, y+1),
                        (x-1, y),   (x, y  ),   (x+1, y),
                                    (x, y-1)
                    ))
            elif width == 4:
                for x, y in zip(xx, yy):
                    positions.update((
                        (x-1, y+1), (x  , y+1), (x+1, y+1),
                        (x-1, y  ), (x  , y  ), (x+1, y  ),
                        (x-1, y-1), (x  , y-1), (x+1, y-1)
                    ))
            elif width == 5:
                for x, y in zip(xx, yy):
                    positions.update((
                                                (x  , y+2),
                                    (x-1, y+1), (x  , y+1), (x+1, y+1),
                        (x-2, y  ), (x-1, y  ), (x  , y  ), (x+1, y  ), (x+2, y),
                                    (x-1, y-1), (x  , y-1), (x+1, y-1),
                                                (x  , y-2)
                    ))
            elif width == 6:
                for x, y in zip(xx, yy):
                    positions.update((
                                    (x-1, y+1), (x  , y+2), (x+1, y+2),
                        (x-2, y+1), (x-1, y+1), (x  , y+1), (x+1, y+1), (x+2, y+1),
                        (x-2, y  ), (x-1, y  ), (x  , y  ), (x+1, y  ), (x+2, y  ),
                        (x-2, y-1), (x-1, y-1), (x  , y-1), (x+1, y-1), (x+2, y-1),
                                    (x-1, y-2), (x  , y-2), (x+1, y+2),
                    ))
            xx = []
            yy = []
            for x, y in positions:
                if 0 <= x < size[0] and 0 <= y < size[1]:
                    xx.append(x)
                    yy.append(y)
        if height != 0:
            assert 0 <= a[yy, xx, 0].mean() - height <= 255
            a[yy, xx, 0] = a[yy, xx, 0].mean() - height
        a[yy, xx, 1] = surface_id
        if layer >= 0:
            # remove anything above the surface (buildings, randomly added grass)
            a[yy, xx, 2] = 0
            a[yy, xx, 3] = 0

for deco, decorations in features["decorations"].items():
    id_ = DECORATIONS[deco]
    for decoration in decorations:
        x, y = shift_coords(decoration["x"], decoration["y"])
        if not x: # test if x is either None or []
            if args.verbose: print("Out of bounds, ignoring decoration:", x, y, decoration)
            continue
        if type(x) is list:
            for i in range(0, len(x)-1):
                x1, y1 = x[i], y[i]
                x2, y2 = x[i+1], y[i+1]
                xx, yy = skimage.draw.line(x1, y1, x2, y2)
                a[yy, xx, 2] = id_
        else:
            a[y, x, 2] = id_
        if deco in ("tree", "leaf_tree", "conifer", "bush"):
            # place dirt below tree
            a[y, x, 1] = SURFACES["dirt"]


offset_x = args.offsetx-min_x if args.offsetx is not None else 0
offset_z = args.offsetz-min_y if args.offsetz is not None else 0
out = args.output

print("offset x:", offset_x, "offset z:", offset_z)

if args.incr:
    with open(out, "rb") as f:
        version = from_bytes(f.read(1))
        min_version = from_bytes(f.read(1))
        if min_version > 1:
            raise ValueError(f"Can't add incremental map info; map.dat has newer version {version}")
        old_layer_count = from_bytes(f.read(1))
        old_floor_height = from_bytes(f.read(1))
        old_offset_x = from_bytes(f.read(2))
        old_offset_z = from_bytes(f.read(2))
        old_size_x = from_bytes(f.read(2))
        old_size_y = from_bytes(f.read(2))
        length_a = from_bytes(f.read(4))
        old_a_ = np.frombuffer(zlib.decompress(f.read(length_a)), dtype=np.uint8).reshape((old_size_y, old_size_x, old_layer_count))

    if old_layer_count != LAYER_COUNT:
        old_a_wrong_shape = old_a_
        old_a_ = np.zeros((old_size_y, old_size_x, LAYER_COUNT), dtype=np.uint8)
        old_a_[:,:,:old_layer_count] = old_a_wrong_shape

    old_a_, old_offset_x, old_offset_z = fit_array(old_a_, -old_offset_x, -old_offset_z, -offset_x, -offset_z, a.shape[1], a.shape[0])
    old_a = np.zeros(a.shape, dtype=np.uint8)
    old_a[old_offset_z:old_offset_z+old_a_.shape[0], old_offset_x:old_offset_x+old_a_.shape[1]] = old_a_

    diff = a != old_a

    changed_blocks = []

    block_x_start = -offset_x//16
    block_x_end = (-offset_x+a.shape[1])//16
    block_z_start = -offset_z//16
    block_z_end = (-offset_z+a.shape[0])//16
    print(f"checking blocks from {block_x_start},{block_z_start} to {block_x_end},{block_z_end} for changes")
    for block_x in range(block_x_start, block_x_end+1):
        for block_z in range(block_z_start, block_z_end+1):
            z1 = max(block_z*16+offset_z, 0)
            z2 = min(z1+16, diff.shape[0])
            x1 = max(block_x*16+offset_x, 0)
            x2 = min(x1+16, diff.shape[1])
            if diff[z1:z2, x1:x2].any():
                assert block_x < 2**15 and block_z < 2**15, (block_x, block_z)
                changed_blocks.append((block_x, block_z))
    print("changed blocks:", changed_blocks[:10], "..." if len(changed_blocks) > 10 else "")
    changed_blocks = zlib.compress(b"".join(to_bytes(x, 2) + to_bytes(z, 2) for x, z in changed_blocks), 9)
else:
    changed_blocks = b""


with open(out, "wb") as f:
    f.write(to_bytes(1, 1))  # version
    f.write(to_bytes(1, 1))  # minimum compatible version
    f.write(to_bytes(LAYER_COUNT, 1))
    f.write(to_bytes(a[offset_z, offset_x, 0], 1))  # height at spawnpoint
    f.write(to_bytes(offset_x, 2))
    f.write(to_bytes(offset_z, 2))
    f.write(to_bytes(a.shape[1], 2))
    f.write(to_bytes(a.shape[0], 2))
    a_compressed = zlib.compress(a.tobytes(), 9)
    f.write(to_bytes(len(a_compressed), 4))
    f.write(a_compressed)
    f.write(to_bytes(len(changed_blocks), 4))
    f.write(changed_blocks)

if args.createimg:
    import imageio

for i in range(3):
    layer = a[::-1,:,i]
    name = "layer" + ["0_height", "1_surface", "2_deco"][i]
    m = max(layer.max(), 1)
    print(name, "max value:", m)
    if args.createimg:
        imageio.imwrite(f"world2minetest/{name}.png", layer*int(255/m))
