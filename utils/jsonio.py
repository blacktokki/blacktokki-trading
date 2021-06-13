import json
import os

def save_json(data, path):
    with open(os.path.join(path), 'w') as outfile:
        json.dump(data, outfile)


def load_json(path):
    with open(os.path.join(path), "r") as json_file:
        return json.load(json_file)
    return None