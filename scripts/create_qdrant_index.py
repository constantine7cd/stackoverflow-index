import json
import logging
import os
from pathlib import Path
from typing import Any

import pretty_logging
from qdrant_client import QdrantClient, models

_log = logging.getLogger(Path(__file__).stem)

HOST = "qdrant"
COLLECTION_NAME = "stackoverflow_question_pages"
EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
DUMP_DIR = Path("data/stackoverflow_questions_json_dump")

PAYLOAD_BATCH_SIZE = 1000
CLIENT_BATCH_SIZE = 8
REMOVE_IF_COLLECTION_EXISTS = True


class SuppressStdout:
    def __enter__(self):
        self.null_fd = os.open(os.devnull, os.O_RDWR)
        self.old_stdout_fd = os.dup(1)
        self.old_stderr_fd = os.dup(2)
        os.dup2(self.null_fd, 1)
        os.dup2(self.null_fd, 2)

    def __exit__(self, exc_type, exc_value, traceback):
        os.dup2(self.old_stdout_fd, 1)
        os.dup2(self.old_stderr_fd, 2)
        os.close(self.null_fd)
        os.close(self.old_stdout_fd)
        os.close(self.old_stderr_fd)


def read_file(dump_dir: Path, file_hash: str) -> dict[str, Any]:
    subfolder = dump_dir / file_hash[:2] / file_hash[2:4]
    file_path = subfolder / f"{file_hash}.json"
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def prepare_insertion_batch(hashes: list[str], dump_dir: Path) -> list[dict[str, Any]]:
    index_text, payload = [], []
    for file_hash in hashes:
        data = read_file(dump_dir, file_hash)
        itext, pload = data["Title"], data
        index_text.append(itext)
        payload.append(pload)
    return index_text, payload


if __name__ == "__main__":
    pretty_logging.setup(logging.INFO)

    hashes_path = DUMP_DIR / "hashes.json"
    _log.info(f"Loading hashes from {hashes_path}...")
    with open(hashes_path, "r") as f:
        hashes = json.load(f)
    _log.info(f"Hashes to process: {len(hashes)}")

    api_key = os.getenv("QDRANT__SERVICE__API_KEY")
    client = QdrantClient(host=HOST, api_key=api_key, https=False)
    client.set_model(EMBEDDING_MODEL)
    if client.collection_exists(COLLECTION_NAME):
        if not REMOVE_IF_COLLECTION_EXISTS:
            raise FileExistsError(f"Collection {COLLECTION_NAME} already exists")
        _log.warning("Collection already exists. Removing...")
        client.delete_collection(COLLECTION_NAME)
        
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=client.get_fastembed_vector_params(on_disk=True),
        optimizers_config=models.OptimizersConfigDiff(
            indexing_threshold=0,
        ),
        quantization_config=models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(
                type=models.ScalarType.INT8,
                quantile=0.99,
                always_ram=True,
            ),
        ),
    )
    for i in range(0, len(hashes), PAYLOAD_BATCH_SIZE):
        batch_hashes = hashes[i : i + PAYLOAD_BATCH_SIZE]
        index_text, payload = prepare_insertion_batch(batch_hashes, DUMP_DIR)
        ids = range(i, i + len(batch_hashes))

        with SuppressStdout():
            client.add(
                collection_name=COLLECTION_NAME,
                documents=index_text,
                metadata=payload,
                ids=ids,
                parallel=0,
                batch_size=CLIENT_BATCH_SIZE,
            )
        if i % 10_000 == 0:
            _log.info(f"Processed {i + len(batch_hashes)} hashes")

    _log.info("Indexing complete")
    _log.info("Optimizing collection...")
    client.update_collection(
        collection_name=COLLECTION_NAME,
        optimizer_config=models.OptimizersConfigDiff(indexing_threshold=20000),
    )
    _log.info("Done")
