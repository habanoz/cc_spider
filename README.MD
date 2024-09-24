## cc-spider

A simple data collection and cleaning pipeline based on common crawl and datatrove. It is designed to collect text from a designated list of domains.

- Create an cdx index of urls to fetch from a domain
- Build a db index of cdx data to keep track of fetching
- Parallel fetch chunks from common crawl using the offset and length information from cdx indexing data.
- Extract text and apply heuristic based quality filters
- Run deduplication to remove near duplicate documents

### Build CDX Index

Edit and use provided bash shell

```bash
build_cdx_index.sh
```
Alternatively directly run following command(edit before using):
```bash
python src/cccd/cdx-index-client.py -c all https://www.aa.com.tr/tr/* -p 1 --max-retries 10 -j -z -d cdx_root/aa
```

### Initialize Fetcher Database

```bash
python -m src.cccd.build_db_index --pattern "cdx_root/aa/prefix-t24.com.tr-CC-MAIN-*" --root work/aa
```

### Download CC Files

Use CDX index to download the files.

```bash
python -m src.cccd.chunk_downloader --root work/aa
```

### Extract Text and Apply Filters

This step requires datatrove to be installed. Edit python file before running. 

```bash
python datatrove/process_common_crawl_fetched_files.py
```

### Deduplicate

This step requires datatrove to be installed. Edit python file before running. 

```bash
python datatrove/local_minhash_deduplication.py
```