import logging
import os
from pathlib import Path
from typing import Any

import pretty_logging
from create_qdrant_index import COLLECTION_NAME, HOST, SuppressStdout
from qdrant_client import QdrantClient, models

_log = logging.getLogger(Path(__file__).stem)


def compute_aux_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    num_answers = len(metadata["answers"])
    num_comments = len(metadata["comments"])
    return {
        "num_answers": num_answers,
        "num_comments": num_comments,
    } 


if __name__ == "__main__":
    pretty_logging.setup(logging.INFO)

    api_key = os.getenv("QDRANT__SERVICE__API_KEY")
    client = QdrantClient(host=HOST, api_key=api_key, https=False, timeout=20)

    batch_size = 100
    offset = None 

    client.update_collection(
        collection_name=COLLECTION_NAME,
        optimizer_config=models.OptimizersConfigDiff(indexing_threshold=0),
    )

    num_processed = 0
    while True:
        with SuppressStdout():
            points, next_offset = client.scroll(
                collection_name=COLLECTION_NAME, 
                limit=batch_size, 
                offset=offset
            )
        if not points:
            break

        update_operations = []
        for point in points:
            update_operations.append(
                models.SetPayloadOperation(
                    set_payload=models.SetPayload(
                        points=[point.id],
                        payload=compute_aux_metadata(point.payload)
                    )
                )
            )
        with SuppressStdout():
            client.batch_update_points(
                collection_name=COLLECTION_NAME,
                update_operations=update_operations
            )

        offset = next_offset
        if not offset:
            break

        num_processed += len(points)
        if num_processed % 10_000 == 0:
            _log.info(f"Processed {num_processed} points")
    _log.info(f"Processed {num_processed} points in total")

    _log.info(f"Optimizing collection...")
    client.update_collection(
        collection_name=COLLECTION_NAME,
        optimizer_config=models.OptimizersConfigDiff(indexing_threshold=0),
    )

    _log.info(f"Creating index on num_answers field...")
    client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="num_answers",
        field_schema="integer"
    )
    _log.info("Done")
