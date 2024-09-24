import pandas as pd
import argparse
import glob
import os

def main(pattern, output_path, repo_id, token):
    if os.path.dirname(output_path):
        os.makedirs(os.path.dirname(output_path))
    
    files = glob.glob(pathname=pattern)
    dfs = [pd.read_pickle(file) for file in files]
    
    merged_df = pd.concat(dfs)
    merged_df.to_pickle(output_path)
    
    print("Done!")
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", type=str, required=True, help="File name pattern to find pandas frames to merge. Provide exact path for single files.")
    parser.add_argument("--output_path", type=str, required=True, default=None, help="PAth for the merged file.")
    parser.add_argument("--repo_id", type=str, required=False, default=None, help="Repository name. Leave empty if you do not want to push.")
    parser.add_argument("--token", type=str, required=False, default=None, help="HF Hub token. If you use huggingface-cli login or huggingface-hub login, leave empty!")
    args = parser.parse_args()
    
    main(args.pattern, args.output_path, args.repo_id, args.token)
