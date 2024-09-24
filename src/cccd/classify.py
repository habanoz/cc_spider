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
import joblib
import os
import re
import urllib.parse

logger = logging.getLogger(__name__)

def prep_url(url):
    url = urllib.parse.unquote(url)

    # Find the index of the first occurrence of "//"
    protocol_end_index = url.find("://") + 3

    # Find the index of the first occurrence of "/" after the protocol
    domain_end_index = url.find("/", protocol_end_index)+1

    # Extract the path and query components
    url = url[domain_end_index:]

    url = re.sub(r"\b[2][0]\d{2}\b","<year>",url)
    url = re.sub(r"\b[1][9]\d{2}\b","<year>",url)
    url = re.sub(r"\b[1-9][0-9]{3,}\b","<number>",url)
    url = re.sub(r"\b[1-9][0-9]{2}\b","<3number>",url)
    url = re.sub(r"\b[1-9][0-9]\b","<2number>",url)
    
    return url

def process_row(row):
    url = row.url
    row['url_p'] = prep_url(url)
    return row 


def main(root_dir):
    
    db_file_name = Path(f"{root_dir}/sqlite/main.db").resolve()
    db_manager = DatabaseManager(f"sqlite:///{db_file_name}")
    web_doc_dao = WebDocDao(db_manager)
    
    loaded_vectorizer = joblib.load('.models/news_vectorizer.joblib')
    loaded_cls = joblib.load('.models/news_classifier.joblib')

    def classify_url(urls):
        urls = [prep_url(url) for url in urls]
        url_vectorized = loaded_vectorizer.transform(urls)
        return loaded_cls.predict(url_vectorized)

    total_news = 0
    results = web_doc_dao.get_unclassified_urls()
    all_ids=[id for id,_ in results]
    all_urls=[url for _,url in results]
    n_urls = len(all_urls)
    
    batch_size=100
    for i in tqdm.trange(0,len(all_urls),batch_size):
        batch_urls = all_urls[i:i+batch_size]
        batch_ids = all_ids[i:i+batch_size]
        urls= [url[0] for url in batch_urls]
        
        predictions = classify_url(urls)
        predictions = [prediction=='1' for prediction in predictions]
        total_news+= sum(predictions)
        web_doc_dao.update_label_batch(batch_ids, predictions)
    
    if n_urls:
        print(f"Total Records {n_urls}; Total News: {total_news}, ratio: {total_news/n_urls}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=str, required=True, help="Provide root-dir to containing db file.")
    args = parser.parse_args()

    root_dir = args.root

    main(root_dir)