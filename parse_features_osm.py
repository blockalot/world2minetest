import argparse
import orjson
from concurrent.futures import ThreadPoolExecutor
import threading
from collections import defaultdict

from pyproj import CRS, Transformer

from _util import SURFACES, DECORATIONS

parser = argparse.ArgumentParser(description="Parse OSM data")
parser.add_argument("file", type=argparse.FileType("r", encoding="utf-8"), help="GeoJSON file with OSM data")
parser.add_argument("--output", "-o", type=argparse.FileType("w"), help="Output file. Defaults to parsed_data/features_osm.json", default="./parsed_data/features_osm.json")

args = parser.parse_args()

# thread locks for the different lists to enable parallelization without concurrency conflicts
areas_lock = threading.Lock()
buildings_lock = threading.Lock()
decorations_lock = threading.Lock()
highways_lock = threading.Lock()

# transform EPSG:4326 to EPSG:25832
transform_coords = Transformer.from_crs(CRS.from_epsg(4326), CRS.from_epsg(25832)).transform
def get_nodepos(lat, lon):
    x, y = transform_coords(lat, lon)
    return int(round(x)), int(round(y))


def print_element(msg, e):
    print(msg, f"{e['id']} {e['type']}[{','.join(k+'='+v for k,v in e.get('tags', {}).items())}]")


node_id_to_blockpos = {}

def node_ids_to_node_positions(node_ids):
    x_coords = []
    y_coords = []
    for node_id in node_ids:
        x, y = node_id_to_blockpos[node_id]
        x_coords.append(x)
        y_coords.append(y)
    return x_coords, y_coords


data = orjson.loads(args.file.read())


min_x = None
max_x = None
min_y = None
max_y = None

def update_min_max(x_coords, y_coords):
    global min_x, max_x, min_y, max_y
    min_x = min(x_coords) if min_x is None else min(min_x, *x_coords)
    max_x = max(x_coords) if max_x is None else max(max_x, *x_coords)
    min_y = min(y_coords) if min_y is None else min(min_y, *y_coords)
    max_y = max(y_coords) if max_y is None else max(max_y, *y_coords)

def process_barrier(barrier):
    if barrier["tags"]["barrier"] in DECORATIONS:
        deco = barrier["tags"]["barrier"]
    else:
        deco = "barrier"
        print_element("Default barrier:", barrier)
    x_coords, y_coords = node_ids_to_node_positions(barrier["nodes"])
    update_min_max(x_coords, y_coords)
    with decorations_lock:
        res_decorations[deco].append({"x": x_coords, "y": y_coords})

def process_building(building):
    x_coords, y_coords = node_ids_to_node_positions(building["nodes"])
    if len(x_coords) < 2:
        print_element(f"Ignored, only {len(x_coords)} nodes:", building)

    tags = building["tags"]
    material = None

    if "building:material" in tags:
        if tags["building:material"] == "brick":
            material = "brick"
        else:
            print_element("Unrecognized building:material", building)
    is_building_part = "building:part" in tags

    try:
        levels = int(tags["building:levels"])
    except (KeyError, ValueError):
        levels = None

    try:
        height = int(float(tags["height"]))
    except (KeyError, ValueError):
        height = None
    else:
        height = min(height, 255)
    
    b = {
        "x": x_coords, 
        "y": y_coords, 
        "is_part": is_building_part, 
    }

    if height is not None:
        b["height"] = height
    if levels is not None:
        b["levels"] = levels
    if material is not None:
        b["material"] = material
    
    print('building successfully parsed')
    with buildings_lock:
        res_buildings.append(b)

def process_area(area):
    tags = area["tags"]
    surface = None
    if "surface" in tags and tags["surface"] in SURFACES:
        surface = tags["surface"]
    elif "natural" in tags:
        if tags["natural"] == "water":
            surface = "water"
        else:
            surface = "natural"
    elif "amenity" in tags:
        if tags["amenity"] in SURFACES:
            surface = tags["amenity"]
        else:
            surface = "amenity"
    elif "leisure" in tags:
        if tags["leisure"] in SURFACES:
            surface = tags["leisure"]
        else:
            surface = "leisure"
    elif "landuse" in tags:
        if tags["landuse"] == "residential":
            surface = "residential_landuse"  # "residential" is also a highway type
        elif tags["landuse"] == "reservoir":
            surface = "water"
        elif tags["landuse"] in SURFACES:
            surface = tags["landuse"]
        else:
            surface = "landuse"
    elif "railway" in tags:
        surface = "railway"
    if surface is None:
        print_element("Ignored, could not determine surface:", area)
        return
    x_coords, y_coords = node_ids_to_node_positions(area["nodes"])
    update_min_max(x_coords, y_coords)

    with areas_lock:
        res_areas.append({"x": x_coords, "y": y_coords, "surface": surface})

def process_highway(highway):
    tags = highway["tags"]

    if "railway" in tags:
        surface = "railway"
    elif tags["highway"] in SURFACES:
        surface = tags["highway"]
    elif "surface" in tags and tags["surface"] in SURFACES:
        surface = tags["surface"]
        print(highway)
        print("railway", highway)
    else:
        surface = "highway"
        print_element("Default highway:", highway)

    layer = tags.get("layer", 0)
    try:
        layer = int(layer)
    except ValueError:
        layer = 0
    if "tunnel" in tags and tags["tunnel"] != "building_passage":
        return
        if "layer" in tags:
            try:
                layer = int(tags["layer"])
            except ValueError:
                layer = -1
            if layer > 0:
                layer = 0
        else:
            layer = -1

    x_coords, y_coords = node_ids_to_node_positions(highway["nodes"])
    update_min_max(x_coords, y_coords)
    with highways_lock:
        res_highways.append({"x": x_coords, "y": y_coords, "surface": surface, "layer": layer, "type": tags["highway"]})

def process_railway(railway):
    tags = railway["tags"]

    layer = tags.get("layer", 0)
    try:
        layer = int(layer)
    except ValueError:
        layer = 0
    if "tunnel" in tags and tags["tunnel"] != "building_passage":
        return
        if "layer" in tags:
            try:
                layer = int(tags["layer"])
            except ValueError:
                layer = -1
            if layer > 0:
                layer = 0
        else:
            layer = -1

    x_coords, y_coords = node_ids_to_node_positions(railway["nodes"])
    update_min_max(x_coords, y_coords)
    with highways_lock:
        res_highways.append({"x": x_coords, "y": y_coords, "surface": "railway", "layer": layer, "type": "railway"})



def process_node(e):
    t = e["type"]
    if t != 'node':
        return
    blockpos = get_nodepos(e["lat"], e["lon"])
    node_id_to_blockpos[e["id"]] = blockpos
    tags = e["tags"]

    if not tags or ("natural" not in tags and "amenity" not in tags and "barrier" not in tags):
        return
    if "natural" in tags:
        if tags["natural"] in DECORATIONS:
            deco = tags["natural"]
        else:
            print_element("Unrecognized natural node:", e)
            return
    elif "amenity" in tags and tags["amenity"] in DECORATIONS:
        deco = tags["amenity"]
    elif "barrier" in tags:
        if tags["barrier"] in DECORATIONS:
            deco = tags["barrier"]
        else:
            deco = "barrier"
            print_element("Default barrier:", e)
    else:
        print_element("Ignored, could not determine decoration type:", e)
        return
    x, y = get_nodepos(e["lat"], e["lon"])
    update_min_max([x], [y])
    with decorations_lock:
        res_decorations[deco].append({"x": x, "y": y})

def process_element(e):
    if e["id"] == 63950590:
        print("one railway")
    t = e["type"]
    tags = e.get("tags")
    if t == "way":
        if not tags:
            print_element("Ignored, missing tags:", e)
            return
        if "area" in tags:
            process_area(e)
        elif "highway" in tags:
            process_highway(e)
        elif "railway" in tags:
            process_railway(e)
        elif "building" in tags or "building:part" in tags:
            process_building(e)
        elif "barrier" in tags:
            process_barrier(e)
        else:
            process_area(e)
    #     blockpos = get_nodepos(e["lat"], e["lon"])
    #     node_id_to_blockpos[e["id"]] = blockpos
    #     if tags and ("natural" in tags or "amenity" in tags or "barrier" in tags):
    #         process_node(e)
    elif t != "node":
        print(f"Ignoring element with unknown type '{t}'")





with ThreadPoolExecutor() as executor:
    res_areas = []
    res_buildings = []
    res_decorations = defaultdict(list)
    res_highways = []
    # We need to process nodes first to know the positions of the different node blocks
    # stored in node_id_to_blockpos
    # possible alternative: sort data['elements'] so that all nodes appear first
    # then process everything in one iteration through data['elements']
    # but: is that really more efficient?
    executor.map(process_node, data["elements"])
    executor.map(process_element, data["elements"])

print(f"\nfrom {min_x},{min_y} to {max_x},{max_y} (size: {max_x-min_x+1},{max_y-min_y+1})")

output_data = orjson.dumps({
    "min_x": min_x,
    "max_x": max_x, 
    "min_y": min_y,
    "max_y": max_y,
    "areas": res_areas,
    "buildings": res_buildings,
    "decorations": dict(res_decorations),
    "highways": res_highways
}, option=orjson.OPT_INDENT_2)  # Pretty-print with indentation

# Write the serialized byte string to the output file
args.output.write(output_data.decode("utf-8"))
