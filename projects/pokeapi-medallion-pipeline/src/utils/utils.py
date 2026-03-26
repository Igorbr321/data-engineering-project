import logging
import time
import os
import boto3

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from snowflake.connector.errors import DatabaseError as SnowflakeDatabaseError

from dotenv import load_dotenv

load_dotenv()


# =============================
# Conexão com S3
# =============================

def connect_s3():
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION")

    if not aws_access_key or not aws_secret_key:
        raise ValueError("Credenciais AWS não encontradas no .env")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    logging.info("Conectado ao S3")

    return s3


# =============================
# Conexão para armazenar log
# =============================

def save_logs(program, table, execution_time, error):
    from datetime import datetime

    logs = ""

    # Ler o conteúdo do arquivo de log
    with open("app.log", "r", encoding="utf-8") as log_file:
        logs = log_file.read()

    df = pd.DataFrame(
        [
            {
                "programa": program,
                "tabela": table,
                "duracao": execution_time,
                "logs": logs,
                "erro": error,
                "data_processamento": datetime.now(),
            }
        ]
    )

    conn = connect_dw()
    with conn.connect() as sql:
        df.to_sql(
            "logs_processos",
            sql,
            schema="BRONZE",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1,
        )
        sql.commit()
        logging.info(f"Log de execução salvo no banco de dados.")


# =============================
# Inicialização de logging
# =============================


def init_logging(layer: str):
    import warnings
    from sqlalchemy.exc import SAWarning

    log_dir = os.path.join("logs", layer)
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "pipeline.log")

    logging.basicConfig(
        filename=log_file,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8",
    )

    # ignora logs de INFO e DEBUG do Snowflake
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

    warnings.filterwarnings(
        "ignore",
        category=SAWarning,
        message="The GenericFunction 'flatten' is already registered and is going to be overridden.",
    )


# =============================
# Tempo de Execução
# =============================

def execution_time(start_time):
    import time

    execution_time = time.time() - start_time
    minutes = int(execution_time // 60)
    seconds = int(execution_time % 60)

    return round(minutes + (seconds / 100), 2)


