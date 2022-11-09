import json
import glob
import pprint as pp #Pretty printer

combined = []
for json_file in glob.glob("*.json"): #Assuming that your json files and .py file in the same directory
    with open(json_file, "rb") as infile:
        combined.append(json.load(infile))

with open("all_hackatons.json", "w") as outfile:
    json.dump(combined, outfile)
