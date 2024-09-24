from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.dedup import MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import (
    MinhashConfig,
    MinhashDedupBuckets,
    MinhashDedupCluster,
    MinhashDedupFilter,
)
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.utils.hashing import HashConfig
from datatrove.utils.typeshelper import Languages


# you can also change ngrams or the number of buckets and their size here
minhash_config = MinhashConfig(
    hash_config=HashConfig(precision=64),
    num_buckets=450,
    hashes_per_bucket=20,
)  # better precision -> fewer false positives (collisions)

SUBDIR="corpus"
S3_MINHASH_ROOT = f"/media/melek/depo/work/{SUBDIR}/processed" 
S3_MINHASH_BASE_PATH = S3_MINHASH_ROOT+"/dedup"

S3_LOGS_FOLDER = S3_MINHASH_BASE_PATH+"/logs/my_minhash_logs_path/"
LOCAL_LOGS_FOLDER = S3_MINHASH_BASE_PATH+"/logs/my_local_folder_for_slurm_logs/"

TOTAL_TASKS = 4

# this is the original data that we want to deduplicate
INPUT_READER = JsonlReader(f"/media/melek/depo/work/corpus/source/")

# stage 1 computes minhash signatures for each task (each task gets a set of files)
stage1 = LocalPipelineExecutor(
    pipeline=[
        INPUT_READER,
        MinhashDedupSignature(
            output_folder=f"{S3_MINHASH_BASE_PATH}/signatures", config=minhash_config, language=Languages.turkish
        ),
    ],
    tasks=TOTAL_TASKS,
    logging_dir=f"{S3_LOGS_FOLDER}/signatures",
)

# stage 2 finds matches between signatures in each bucket
stage2 = LocalPipelineExecutor(
    pipeline=[
        MinhashDedupBuckets(
            input_folder=f"{S3_MINHASH_BASE_PATH}/signatures",
            output_folder=f"{S3_MINHASH_BASE_PATH}/buckets",
            config=minhash_config
        ),
    ],
    tasks=minhash_config.num_buckets,
    logging_dir=f"{S3_LOGS_FOLDER}/buckets",
    depends=stage1,
)

# stage 3 creates clusters of duplicates using the results from all buckets
stage3 = LocalPipelineExecutor(
    pipeline=[
        MinhashDedupCluster(
            input_folder=f"{S3_MINHASH_BASE_PATH}/buckets",
            output_folder=f"{S3_MINHASH_BASE_PATH}/remove_ids",
            save_cluster_id=True,
            config=minhash_config,
        ),
    ],
    tasks=1,
    logging_dir=f"{S3_LOGS_FOLDER}/clusters",
    depends=stage2,
)

# stage 4 reads the original input data and removes all but 1 sample per duplicate cluster
# the data must match exactly stage 1, so number of tasks and the input source must be the same
stage4 = LocalPipelineExecutor(
    pipeline=[
        INPUT_READER,
        TokensCounter(),  # nice way to see how many tokens we had before and after deduplication
        MinhashDedupFilter(
            input_folder=f"{S3_MINHASH_BASE_PATH}/remove_ids",
            exclusion_writer=JsonlWriter(f"{S3_MINHASH_BASE_PATH}/removed"),
            load_cluster_ids=True,
        ),
        JsonlWriter(output_folder=f"{S3_MINHASH_BASE_PATH}/deduplicated_output"),
    ],
    tasks=TOTAL_TASKS,
    logging_dir=f"{S3_LOGS_FOLDER}/filter",
    depends=stage3,
)

if __name__ == '__main__':    
    stage4.run()
