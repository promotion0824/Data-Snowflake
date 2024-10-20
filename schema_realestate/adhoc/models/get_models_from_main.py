import os
import re
import csv

directory = 'C:/git/Data-RealEstate-Snowflake/schema_realestate/schema_level/'
output =    'C:/git/Data-RealEstate-Snowflake/schema_realestate/adhoc/models/models_in_sql.csv'
header = ['model', 'file']

with open(output, 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(header)
for root, subdirectories, files in os.walk(directory):
    for subdirectory in subdirectories:
        foldername = os.path.join(root, subdirectory)
    for file in files:
        filename = os.path.join(root, file)
        if filename.endswith(".sql"):
            with open(filename, 'r') as f:
                with open(output, 'a', newline='') as outfile:
                    loaded_csv = csv.reader(f)
                    for row in f:
                        if ("model_id " in row
                        or "model_id_asset " in row
                        or "model_id_capability " in row
                        or "dtmi:com:willowinc:" in row
                        ):
                            matches = re.finditer("'(.+?)'", row)
                            filepath = filename.replace(directory,'')
                            writer = csv.writer(outfile)
                            for match in matches:
                                modelId = row[match.start():match.end()]
                                print(modelId)
                                writer.writerow([modelId,filepath])
                    
outfile.close()


