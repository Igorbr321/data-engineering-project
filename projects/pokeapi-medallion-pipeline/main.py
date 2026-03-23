import logging

from extract_pokemons import extract_endpoints
from utils import setup_logging

def main():
    setup_logging()

    extract_endpoints()




if __name__ == "__main__":
    main()
