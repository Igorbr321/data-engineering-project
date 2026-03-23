import logging
import time
import os

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from snowflake.connector.errors import DatabaseError as SnowflakeDatabaseError

from dotenv import load_dotenv

load_dotenv()


# =============================
# Conexão com DW - Snowflake
# =============================

def connect_dw(retries=10, delay=60):
    db_url = (
        f"snowflake://{os.getenv('USER_DW')}:"
        f"{os.getenv('PASSWORD_DW')}@"
        f"{os.getenv('HOST_DW')}/"
        f"{os.getenv('NAME_DW')}?"
        f"warehouse={os.getenv('WAREHOUSE_DW')}"
    )

    for attempt in range(1, retries + 1):
        try:
            conn = create_engine(db_url)
            with conn.connect() as sql:
                sql.execute(text("select 1"))
                logging.info("Conectado ao DW")
            return conn
        except (OperationalError, DBAPIError, SnowflakeDatabaseError) as e:
            logging.info(f"Tentativa {attempt} falhou: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise


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
        filemode="a",
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


