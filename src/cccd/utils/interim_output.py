import gzip
import json
import os

df_named_path_template = "work/{name}/bins/{file}.out.json.gz"

def save_to_json(obj, run_name, file_name):
    df_path = df_named_path_template.replace("{name}", run_name).replace("{file}",file_name)
    with gzip.open(df_path,"wt") as f:
        json.dump(obj, f)
    