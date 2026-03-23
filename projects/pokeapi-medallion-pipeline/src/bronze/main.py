import logging
import time

from src.bronze.extract_bronze import extract_pokemons_bronze
from src.utils.utils import init_logging, execution_time

def main():
    init_logging('bronze')

    error = "N"
    start_time = time.time()

    try:
        df = extract_pokemons_bronze()

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
        error = "S"

    finally:
        exec_time = execution_time(start_time)
        logging.info(f"Fim do processo. Tempo de execução: {exec_time} minutos.")

if __name__ == "__main__":
    main()
