import json
import logging
import os
from datetime import datetime

from src.utils.utils import connect_s3

BUCKET = os.getenv("S3_BUCKET")
BRONZE_PREFIX = "🟤 [BRONZE] -"


def insert_bronze_s3(list_detalhes: list[dict]):
    logging.info(f"{BRONZE_PREFIX} Iniciando inserção no S3")

    s3 = connect_s3()

    day = datetime.now().strftime("%Y-%m-%d")
    key = f"bronze/pokemon/ingestion_date={day}/pokemon.json"

    body = json.dumps(list_detalhes, ensure_ascii=False, indent=2)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    logging.info(f"{BRONZE_PREFIX} Inserção concluída | Bucket={BUCKET} | Key={key}")