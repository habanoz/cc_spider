import sys
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    GopherQualityFilter,
    GopherRepetitionFilter,
    NewsFilter,
    URLFilter,
    FineWebQualityFilter
)
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.utils.typeshelper import Languages
from datatrove.pipeline.formatters import QuoteFixer

SUBDIR="aa"
MAIN_OUTPUT_PATH = f"/media/melek/depo/work/{SUBDIR}/processed/extract_text"
TOTAL_TASKS = 4

executor = LocalPipelineExecutor(
    pipeline=[
        JsonlReader(f"/media/melek/depo/work/{SUBDIR}/fetched/", text_key="raw"),
        Trafilatura(favour_precision=True,timeout=0.99),
        QuoteFixer(),
        GopherRepetitionFilter(exclusion_writer=JsonlWriter(f"{MAIN_OUTPUT_PATH}/removed/repetitive"),language=Languages.turkish),
        GopherQualityFilter(exclusion_writer=JsonlWriter(f"{MAIN_OUTPUT_PATH}/removed/quality"), language=Languages.turkish, max_non_alpha_words_ratio=0.7),
        FineWebQualityFilter(language=Languages.turkish),
        JsonlWriter(f"{MAIN_OUTPUT_PATH}/text"),
    ],
    tasks=TOTAL_TASKS,
    logging_dir=f"{MAIN_OUTPUT_PATH}/logs/extract_text"
)

if __name__ == '__main__':    
    executor.run()