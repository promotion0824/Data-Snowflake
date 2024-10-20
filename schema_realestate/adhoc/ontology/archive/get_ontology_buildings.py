import os
import json
import csv
from typing_extensions import Concatenate  
#directory = "C:/git/Ontology_troubleshooting/Willow"
directory = 'C:/git/opendigitaltwins-building/Ontology/Willow/'
header = ['path', 'key_value']
data_list = []  # create an empty list
output = 'C:/git/Willow.Dse.RealEstate.Snowflake/schema_realestate/adhoc/ontology/ontology_buildings.csv'

with open(output, 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(header)
for root, subdirectories, files in os.walk(directory):
    for subdirectory in subdirectories:
        foldername = os.path.join(root, subdirectory)
    for file in files:
        filename = os.path.join(root, file)
        print(filename)
        if filename.endswith(".json"):
            with open(filename, 'r') as f:
                with open(output, 'a', newline='') as outfile:
                    loaded_json = json.load(f)

                    filepath = filename.replace(directory,'Willow')
                    paths = filepath.split('\\')
                    #paths = "".join(paths)
                    data =(paths, loaded_json)
                    writer = csv.writer(outfile)
                    writer.writerow(data)
                    
outfile.close()
