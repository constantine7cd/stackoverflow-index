import hashlib
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pretty_logging
import psycopg2 as pg
from pretty_logging import tqdm_logger

_log = logging.getLogger(Path(__file__).stem)

"""
Tables:

-- Posts TABLE
CREATE TABLE Posts (
    Id                     int PRIMARY KEY    ,
    PostTypeId             int not NULL       ,
    AcceptedAnswerId       int                ,
    ParentId               int                ,
    CreationDate           timestamp not NULL ,
    Score                  int                ,
    ViewCount              int                ,
    Body                   text               ,
    OwnerUserId            int                ,
    LastEditorUserId       int                ,
    LastEditorDisplayName  text               ,
    LastEditDate           timestamp          ,
    LastActivityDate       timestamp          ,
    Title                  text               ,
    Tags                   text               ,
    AnswerCount            int                ,
    CommentCount           int                ,
    FavoriteCount          int                ,
    ClosedDate             timestamp          ,
    CommunityOwnedDate     timestamp          ,
    jsonfield              jsonb
);

-- Comments TABLE
CREATE TABLE Comments (
    Id                     int PRIMARY KEY    ,
    PostId                 int not NULL       , 
    Score                  int not NULL       ,
    Text                   text               ,
    CreationDate           timestamp not NULL , 
    UserId                 int                ,
    jsonfield              jsonb
);


-- QuestionAnswer TABLE
CREATE TABLE QuestionAnswer (
    QuestionId int,
    AnswerId   int,
    PRIMARY KEY (QuestionId, AnswerId)
);


-- PostComments TABLE
CREATE TABLE PostComments (
    PostId int,
    CommentId  int,
    PRIMARY KEY (PostId, CommentId)
);
"""


def fetch_question_ids(
    cur,
    reuse_question_id_dump: bool = False,
    save_question_id_dump: bool = True,
    question_id_dump_path: Path | None = None,
) -> list[int]:
    _log.info("Fetching question ids...")
    question_id_dump_path = question_id_dump_path or Path("question_id_dump.json")

    if Path(question_id_dump_path).exists() and reuse_question_id_dump:
        _log.warning(f"Reusing question id dump from {question_id_dump_path}...")
        with open(question_id_dump_path, "r") as f:
            question_indices = json.load(f)
    else:
        cur.execute("SELECT Id FROM Posts WHERE PostTypeId = 1")
        question_indices = [index for index, in cur.fetchall()]

        if save_question_id_dump:
            _log.info(f"Saving question id dump to {question_id_dump_path}...")
            with open(question_id_dump_path, "w") as f:
                json.dump(question_indices, f)
    return question_indices


def get_post_comments(
    cur, post_id: int, comment_fields: list[str], comment_fields_str: str | None = None
) -> list[dict[str, Any]]:
    if comment_fields_str is None:
        comment_fields_str = ", ".join(comment_fields)

    cur.execute(f"SELECT CommentId FROM PostComments WHERE PostId = {post_id}")
    comment_ids = [comment_id for comment_id, in cur.fetchall()]
    if comment_ids:
        if len(comment_ids) == 1:
            condition = f"Id = {comment_ids[0]}"
        else:
            array = f"ARRAY[{', '.join(map(str, comment_ids))}]"
            condition = f"Id = ANY({array})"

        cur.execute(f"SELECT {comment_fields_str} FROM Comments WHERE {condition}")
        return [
            {field: value for field, value in zip(comment_fields, comment_data)}
            for comment_data in cur.fetchall()
        ]
    return []


def get_question_answers(
    cur,
    question_id: int,
    answer_fields: list[str],
    comment_fields: list[str],
    answer_fields_str: str | None = None,
    comment_fields_str: str | None = None,
) -> list[int]:
    if answer_fields_str is None:
        answer_fields_str = ", ".join(answer_fields)
    if comment_fields_str is None:
        comment_fields_str = ", ".join(comment_fields)

    cur.execute(f"SELECT AnswerId FROM QuestionAnswer WHERE QuestionId = {question_id}")
    answers = []
    for (answer_id,) in cur.fetchall():
        answer_dict = {}
        cur.execute(f"SELECT {answer_fields_str} FROM Posts WHERE Id = {answer_id}")
        answer_data = cur.fetchone()
        answer_dict.update(
            {field: value for field, value in zip(answer_fields, answer_data)}
        )
        answer_dict["comments"] = get_post_comments(
            cur, answer_id, comment_fields, comment_fields_str
        )
        answers.append(answer_dict)
    return answers


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def save_question_dump(question_dict: dict[str, Any], export_dir: Path) -> None:
    question_id = question_dict["Id"]
    question_id_hash = hashlib.sha512(str(question_id).encode()).hexdigest()

    subfolder = export_dir / question_id_hash[:2] / question_id_hash[2:4]
    subfolder.mkdir(parents=True, exist_ok=True)

    dump_path = subfolder / f"{question_id_hash}.json"
    with open(dump_path, "w") as f:
        json.dump(question_dict, f, indent=4, default=json_serial)
    return dump_path


if __name__ == "__main__":
    pretty_logging.setup(logging.INFO)

    question_fields = [
        "Id",
        "Body",
        "Title",
        "Tags",
        "AcceptedAnswerId",
        "OwnerUserId",
        "CreationDate",
        "Score",
        "LastEditDate",
    ]
    question_fields_str = ", ".join(question_fields)

    answer_fields = [
        "Id",
        "ParentId",
        "Body",
        "Tags",
        "OwnerUserId",
        "CreationDate",
        "Score",
        "LastEditDate",
    ]
    answer_fields_str = ", ".join(answer_fields)

    comment_fields = [
        "Text",
        "CreationDate",
        "UserId",
        "Score",
    ]
    comment_fields_str = ", ".join(comment_fields)

    conn_parameters = {
        "dbname": "dump",
        "host": "postgres",
        "port": 5432,
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "options": "-c search_path=public",
    }
    reuse_question_id_dump = True
    save_question_id_dump = False

    export_dir = Path("data/stackoverflow_questions_json_dump")
    if export_dir.exists():
        raise FileExistsError(f"{export_dir} already exists.")

    archive_hashes = set()
    with pg.connect(**conn_parameters) as conn:
        with conn.cursor() as cur:
            question_indices = fetch_question_ids(
                cur,
                reuse_question_id_dump=reuse_question_id_dump,
                save_question_id_dump=save_question_id_dump,
            )
            _log.info(f"Processing {len(question_indices)} questions...")
            for question_id in tqdm_logger(question_indices):
                question_dict = {
                    "answers": [],
                    "comments": [],
                }

                cur.execute(
                    f"SELECT {question_fields_str} FROM Posts WHERE Id = {question_id}"
                )
                question_data = cur.fetchone()
                question_dict.update(
                    {
                        field: value
                        for field, value in zip(question_fields, question_data)
                    }
                )
                question_dict["comments"] = get_post_comments(
                    cur, question_id, comment_fields, comment_fields_str
                )
                question_dict["answers"] = get_question_answers(
                    cur,
                    question_id,
                    answer_fields,
                    comment_fields,
                    answer_fields_str,
                    comment_fields_str,
                )
                dump_path = save_question_dump(question_dict, export_dir)
                archive_hashes.add(dump_path.stem)
    
    if len(archive_hashes) != len(question_indices):
        raise ValueError("Some questions were not archived due to error or hash collision.")
    _log.info("All questions archived successfully.")

    _log.info(f"Saving hashes into {export_dir / 'hashes.json'}...")
    with open(export_dir / "hashes.json", "w") as f:
        json.dump(list(archive_hashes), f, indent=4)
    _log.info("Done.")
