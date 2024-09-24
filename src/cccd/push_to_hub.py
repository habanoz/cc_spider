import pandas as pd
import argparse
import glob
from datasets import Dataset, load_dataset

def de_duplicate_by_id(hf_dataset):
    ids = set()
    len0 = len(hf_dataset['train'])
    
    def hf_filter(x):
        if x['Id'] in ids:
            return False
        ids.add(x['Id'])
        return True

    hf_dataset = hf_dataset.filter(hf_filter)
    len1 = len(hf_dataset['train'])
    
    print(f"Initial size: {len0}, size after ID deduplication {len1}")
    
    return hf_dataset

def main(pattern, repo_id, token):
    data_files = glob.glob(pattern)
    data_files = sorted(data_files, reverse=True) # most recent first
    
    hf_dataset = load_dataset("json", data_files=data_files)
    hf_dataset = de_duplicate_by_id(hf_dataset)
    
    hf_dataset.push_to_hub(repo_id, token=token, private=True)
    print("Done!")
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", type=str, required=True, help="File name pattern to find 'json.gzip' files to merge. Provide exact path for a single file.")
    parser.add_argument("--repo_id", type=str, required=False, default=None, help="Repository name.")
    parser.add_argument("--token", type=str, required=False, default=None, help="HF Hub token. If you use huggingface-cli login or huggingface-hub login, leave empty!")
    args = parser.parse_args()
    
    main(args.pattern, args.repo_id, args.token)
