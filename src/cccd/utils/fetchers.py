import io
import logging
from typing import Optional
import requests
from fastwarc.warc import ArchiveIterator, WarcRecordType

logger = logging.getLogger(__name__)


async def fetch_chunk(filename, offset, length, client) -> Optional[dict]:
    """
    Fetch the chunk from the given doc
    """
    async with client.get(
        f'https://data.commoncrawl.org/{filename}',
        headers={'Range': f'bytes={offset}-{offset + length - 1}'}
    ) as response:
        assert response.status in [200, 206]
        content = await response.read()
        
        with io.BytesIO(content) as stream:
            # In fact we have single element, iterator is used for api convenience
            for record in ArchiveIterator(stream, record_types=WarcRecordType.response):
                page_text = record.reader.read().decode(encoding='UTF-8')

                return page_text

    return None


def fetch_chunk_sync(filename, offset, length) -> Optional[dict]:
    """
    Fetch the chunk from the given doc
    """
    response = requests.get(
        f'https://data.commoncrawl.org/{filename}',
        headers={'Range': f'bytes={offset}-{offset + length - 1}'}
    )
    assert response.status_code in [200, 206]
    content = response.content
    
    with io.BytesIO(content) as stream:
        # In fact we have single element, iterator is used for api convenience
        for record in ArchiveIterator(stream, record_types=WarcRecordType.response):
            page_text = record.reader.read().decode(encoding='UTF-8')

            return page_text

    return None


async def fetch_url(url, client) -> Optional[dict]:
    """
    Fetch the url
    """
    async with client.get(url) as response:
        assert response.status in [200]
        return await response.text()
