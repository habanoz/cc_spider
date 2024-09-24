import sys
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    GopherQualityFilter,
    GopherRepetitionFilter,
    FineWebQualityFilter
)
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.utils.typeshelper import Languages
from datatrove.pipeline.formatters import QuoteFixer

stop_words=["ve","bir","da","de","bu","için","ile","olarak","hakkında","çok","ise","ama","gibi","ne","var","bu","şu","o","biz","siz","onlar","ben","sen","benim","ya","göre","kadar","sonra","her","diye","yüzde","ilgili" ]

SUBDIR="aa"
MAIN_OUTPUT_PATH = f"/media/melek/depo/work/{SUBDIR}/processed/extract_text"
TOTAL_TASKS = 4

executor = LocalPipelineExecutor(
    pipeline=[
        JsonlReader(f"/media/melek/depo/work/{SUBDIR}/fetched/", text_key="raw"),
        Trafilatura(favour_precision=True,timeout=0.99),
        QuoteFixer(),
        GopherRepetitionFilter(exclusion_writer=JsonlWriter(f"{MAIN_OUTPUT_PATH}/removed/repetitive"),language=Languages.turkish),
        GopherQualityFilter(exclusion_writer=JsonlWriter(f"{MAIN_OUTPUT_PATH}/removed/quality"), language=Languages.turkish, max_non_alpha_words_ratio=0.7,stop_words=stop_words),
        FineWebQualityFilter(language=Languages.turkish),
        JsonlWriter(f"{MAIN_OUTPUT_PATH}/text"),
    ],
    tasks=TOTAL_TASKS,
    logging_dir=f"{MAIN_OUTPUT_PATH}/logs/extract_text"
)

if __name__ == '__main__':    
    executor.run()