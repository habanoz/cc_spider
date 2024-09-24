
import os
import json
import gzip
import asyncio
import aiohttp
import logging
import tenacity
import argparse
from tqdm.asyncio import tqdm
from datetime import datetime
from .core.response_exceptions import TransientResponseException, PersistentResponseException
from .utils.fetchers import fetch_chunk, fetch_url
from .db.dao.web_doc_dao import WebDocDao
from .db.db_manager import DatabaseManager
from .model.web_doct import WebDocT, WebDocResultT
from .utils.clean_up import html_cleanup

logger = logging.getLogger(__name__)


async def response_status_check(response):
    if response.status in [403, 503]:
        raise TransientResponseException(response.status)

    if response.status >= 400:
        raise PersistentResponseException(response.status)


class BulkCCFetcher:
    retry_limit = 10
    retry_wait_min = 1
    retry_wait_mul = 2
    retry_wait_max = 10*60
    parallelism = 30

    def __init__(self, root_dir: str, db_manager, chunk_size, max=None, use_fallback=True, archive=False, skip_first=None):
        self.root_dir = root_dir
        self.use_fallback = use_fallback
        self.max = max
        self.skip_first = skip_first
        self.chunk_size = chunk_size
        self.db_manager = db_manager
        self.archive = archive

        assert max is None or max >= chunk_size

        self.semaphore = asyncio.BoundedSemaphore(value=self.parallelism)
        self.fallback_semaphore = asyncio.BoundedSemaphore(value=1)

    async def try_fetch_chunk(self, doc:WebDocT, client) -> WebDocResultT:
        async with self.semaphore:
            async for attempt in tenacity.AsyncRetrying(
                wait=tenacity.wait_exponential(
                    min=self.retry_wait_min,
                    max=self.retry_wait_max, multiplier=self.retry_wait_mul) + tenacity.wait_random(1, 5),
                stop=tenacity.stop_after_attempt(self.retry_limit),
                retry=tenacity.retry_if_exception_type(
                    TransientResponseException)
            ):
                with attempt:
                    raw = await fetch_chunk(doc.filename, doc.offset, doc.length, client)
                    raw = html_cleanup(raw)
                    return WebDocResultT(id=doc.id,url=doc.url, raw=raw)
        
        assert False

    async def try_fetch_fallback(self, doc:WebDocT, client) -> WebDocResultT:
        async with self.fallback_semaphore:
            async for attempt in tenacity.AsyncRetrying(
                wait=tenacity.wait_exponential(
                    min=self.retry_wait_min,
                    max=self.retry_wait_max, multiplier=self.retry_wait_mul) + tenacity.wait_random(1, 5),
                stop=tenacity.stop_after_attempt(self.retry_limit),
                retry=tenacity.retry_if_exception_type(
                    TransientResponseException)
            ):
                with attempt:
                    raw = await fetch_url(doc.url, client)
                    return WebDocResultT(id=doc.id,url=doc.url, raw=raw)
        
        assert False

    async def try_fetch_doc(self, doc:WebDocT, client) -> WebDocResultT:
        try:
            return await self.try_fetch_chunk(doc, client)
        except (tenacity.RetryError, PersistentResponseException):
            logger.warning(f"Maximum retry (chunk) reached:{doc}")

            if self.use_fallback:
                try:
                    return await self.try_fetch_fallback(doc, client)
                except tenacity.RetryError:
                    logger.warning(f"Maximum retry (url) reached:{doc}")
                except:
                    logger.exception(f"Fetching (url) failed:{doc}")
        except:
            logger.exception(f"Fetching (chunk) failed:{doc}")

        return WebDocResultT(id=doc.id,url=doc.url, raw=None)

    async def fetch_docs(self, docs: list[WebDocT], completed:int, client) -> list[WebDocResultT]:
        """
        Fetch chunks from the given docs
        """
        tasks = [self.try_fetch_doc(doc, client) for doc in docs]
        results = await tqdm.gather(*tasks, desc=f"{completed} completed")
        non_null_results = [elements for elements in results if elements is not None]
        return non_null_results

    async def process(self):
        logger.info("Starting processing")
        os.makedirs(f"{self.root_dir}/fetched",exist_ok=True)
    
        web_doc_dao = WebDocDao(self.db_manager)
        completed = 0
        depleted = False
        
        async with aiohttp.ClientSession(raise_for_status=response_status_check) as client:
            while not depleted and (self.max is None or completed < self.max):
                docs = web_doc_dao.get_incomplete(limit=chunk_size, offset=self.skip_first)

                fetched = await self.fetch_docs(docs, completed, client)
                print("Saving please wait...", end="\r")
                
                succ_docs = [doc for doc in fetched if doc.raw is not None]
                if succ_docs:
                    mode = 'w' if not self.archive else 'wt'
                    open_func = open if not self.archive else gzip.open
                    suffix = '' if not self.archive else '.gz'

                    with open_func(f"{self.root_dir}/fetched/docs-{succ_docs[0].id}.jsonl{suffix}", mode) as f_res:
                        for web_doc_res_t in succ_docs:
                            json_line = json.dumps(web_doc_res_t._asdict())
                            f_res.write(json_line)
                            f_res.write("\n")
                        
                web_doc_dao.update_done_batch(fetched)
                print ("", end="\r")
                
                depleted = len(docs) < chunk_size
                completed += len(docs)
                
        logger.info(f"Completed processing of {completed} documents")


def main(root_dir, max, chunk_size, archive, skip):
    
    db_file_name = f"{root_dir}/sqlite/main.db"
    if not os.path.exists(db_file_name):
        raise ValueError(f"DB file '{db_file_name}' not found! Create the DB first!")
    
    db_manager = DatabaseManager(f"sqlite:///{db_file_name}")
    fetcher = BulkCCFetcher(root_dir=root_dir, db_manager=db_manager,
                            chunk_size=chunk_size, max=max, use_fallback=True, archive=archive,skip_first=skip)

    asyncio.run(fetcher.process())
    
    print("Processing completed...")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-size", type=int, default=1_000,
                        help="The size of chunks to fetch from docs table. Set to 0 (zero) to disable chunking.")
    parser.add_argument("--max", type=int, default=None,
                        help="Provide the maximum number of items to process. By default there is no limit!")
    parser.add_argument("--skip", type=int, default=None,
                        help="Number of items to skip. By default all items are processed! Use skip and chunk-size together for pagination like behavior. Use skip and max together for partitioning.")
    parser.add_argument("--root", type=str, required=True,
                        help="Root directory of the work.")
    parser.add_argument("--archive", action='store_true',
                        help="Whether to produce gzip files.")
    args = parser.parse_args()

    max = args.max
    chunk_size = args.chunk_size if args.chunk_size else None
    root_dir = args.root
    archive = args.archive
    skip = args.skip
    
    if max is not None:
        chunk_size = min(max, chunk_size)

    log_file = "logs/app.log"

    # rotate the log file
    if os.path.exists(log_file):
        new_log_file = f'logs/app-{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        os.rename(log_file, new_log_file)

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(filename=log_file, level=logging.INFO, format=FORMAT)

    logger.info(f"Starting. {args}")
    main(root_dir=root_dir, max=max, chunk_size=chunk_size,archive=archive, skip=skip)
    logger.info("Completed")
