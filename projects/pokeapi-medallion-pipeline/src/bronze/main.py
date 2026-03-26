import logging
import time

from src.bronze.extract_bronze import extract_pokemons_bronze
from src.bronze.insert_bronze_s3 import insert_bronze_s3
from src.utils.utils import init_logging, execution_time

BRONZE_PREFIX = "🟤 [BRONZE] -"

def main():
    init_logging("bronze")

    error = "N"
    start_time = time.time()

    try:
        list_detalhes = extract_pokemons_bronze()
        logging.info(f"{BRONZE_PREFIX} Registros extraídos: {len(list_detalhes)}")

        insert_bronze_s3(list_detalhes)

    except Exception as e:
        logging.error(f"❌Erro durante a execução: {e}")
        raise

    finally:
        exec_time = execution_time(start_time)
        logging.info(f"✅ Fim do processo. Tempo de execução: {exec_time} minutos.")


if __name__ == "__main__":
    main()