import logging
import sys
import  os

logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(os.getenv("ETL_LOG", "./debug.log")),
        ]
    )

from logocrawler import LogoExtractor
from logocrawler import _sqlite_db

if __name__ == '__main__':
    domains = []
    for line in sys.stdin:
        domains.append(line.strip())
    extractor = LogoExtractor()
    try:
        extractor.run(domains)
    except KeyboardInterrupt:
        _sqlite_db.close()
        logging.info("Received keyboard interrupt. Exitting...")
        sys.exit()
            