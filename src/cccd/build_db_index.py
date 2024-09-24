import os
import glob
import logging
import argparse
import pandas as pd
from datetime import datetime
import tqdm
from pathlib import Path
from .db.db_manager import DatabaseManager
from .db.dao.web_doc_dao import WebDocDao
from .model.web_doct import WebDocT

logger = logging.getLogger(__name__)

def main(pattern, root_dir):
    files = glob.glob(pattern)
    files = sorted(files, reverse=True) # most recent first
    
    
    logger.info(f"Reading {len(files)} into data frame. May take time and consume a lot of memory!") 
    
    df = pd.concat([pd.read_json(file, lines=True) for file in files ])
    logger.info(f"Reading into data frame is done! {len(df)} rows loaded.")
    
    df = df.loc[df['length']>10_000]
    logger.info(f"Filtered short documents (below 10K)! {len(df)} rows remained.")
    
    df = df.loc[df['mime-detected']=='text/html']
    logger.info(f"Filtered non 'text/html items! {len(df)} rows remained.")
    
    df.fillna(value={"status":0}, inplace=True)
    df = df.loc[df['status']<300]
    logger.info(f"Filtered non-successful items! {len(df)} rows remained.")
    
    df = df.drop_duplicates(subset=["urlkey"], keep="first")
    logger.info(f"Duplicate urlkeys removed! {len(df)} rows remained.")
    
    df = df.drop_duplicates(subset=["digest"], keep="first")
    logger.info(f"Duplicate digest removed! {len(df)} rows remained.")
    
    
    db_file_name = Path(f"{root_dir}/sqlite/main.db").resolve()
    os.makedirs(os.path.dirname(db_file_name), exist_ok=True)
    
    db_manager = DatabaseManager(f"sqlite:///{db_file_name}")
    web_doc_dao = WebDocDao(db_manager)
    
    chunk_size=10_000
    for i in tqdm.tqdm(range(0, len(df), chunk_size)):
        docs = [WebDocT(id=None, url=doc.url, filename=doc.filename, offset=doc.offset, length=doc.length) for doc in df.iloc[i:i+chunk_size].itertuples()]
        web_doc_dao.add_batch(docs)
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", type=str, required=True, help="File pattern to read. Only '*.gz' files are expected!")
    parser.add_argument("--root", type=str, required=True, help="Provide root-dir to create db file.")
    args = parser.parse_args()

    pattern = args.pattern
    root_dir = args.root

    main(pattern, root_dir)