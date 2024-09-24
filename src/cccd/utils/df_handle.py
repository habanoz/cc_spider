import pandas as pd
import os

df_path_template = "work/{name}/bins/df.out"
df_named_path_template = "work/{name}/bins/{file}.out"

def get_df_object(name: str, columns):
    df_path = df_path_template.replace("{name}", name)

    if not os.path.exists(df_path):
        df = pd.DataFrame(columns=columns)
        df.to_pickle(df_path)
    
    return pd.read_pickle(df_path)

def save_df_object(df, name):
    df_path = df_path_template.replace("{name}", name)
    df.to_pickle(df_path)

def save_named_df_object(df, run_name, file_name):
    df_path = df_named_path_template.replace("{name}", run_name).replace("{file}",file_name)
    df.to_pickle(df_path)