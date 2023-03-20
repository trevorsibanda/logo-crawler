"""

"""


import logging
import urllib
import time
import requests
from threading import RLock
import random
import csv
import sys
import sqlite3
import os


from bs4 import BeautifulSoup
import etl
import selector

# default config for logo extractor
default_config = {
    "name": os.getenv("ETL_NAME", "logo-extractor"),
    "cache_db": os.getenv("ETL_CACHE_DB" ,"./websites.sqlite"),
    "log": os.getenv("ETL_LOG", "./debug.log"),
    "max_cache_age":  int(os.getenv("ETL_MAX_CACHE_AGE", 24 * 60 * 60 * 7)), #7 days
    "workers": int(os.getenv("ETL_WORKERS", 4))
}

# made global to bypass pickling error
_csv_writer = csv.writer(sys.stdout)

#lock for writing to stdout
_loader_lock = RLock()

#sqlite db
# Todo: can't pickle sqlite3.Connection, using default global var which presents several problems 
_sqlite_db = sqlite3.connect(default_config["cache_db"])

# > This class is a pipeline that extracts logos from a website, transforms them into a format that
# can be loaded into a database, and loads them into a database
#
class LogoExtractor(etl.ETLPipeline):
    def __init__(self, config: dict = default_config) -> None:
        """
        Initialize the LogoExtractor with a config.
        
        :param config: a dictionary of configuration parameters
        :type config: dict
        """
        self._max_age = 60 * 60 * 6 #6 hours
        extractor = CachedCrawler()
        transformer = ExtractLogos()
        loader  = InMemoryAndSTDOUTLoader()
        super().__init__(config, extractor, transformer, loader)



class CachedCrawler(etl.Extract):
    """
    CachedCrawler gets a domain as input and retrieves the page from the Internet. The page is cached
    in a sqlite database for purposes of reducing the time spent in I/O in development. No cache purging is implemented.
    """
    def __init__(self) -> None:
        cursor = _sqlite_db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS site_cache (domain TEXT, html TEXT, status INT NOT NULL, retrieved INT NOT NULL);")            
        super().__init__()
            
            

    # list of user agents to randomly using in requests
    _user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.155 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0',
        'Opera/9.80 (iPhone; Opera Mini/10.2.0/37.6334; U; nl) Presto/2.12.423 Version/12.16',
        'Mozilla/5.0 (Android 4.2.2;) AppleWebKit/1.1 Version/4.0 Mobile Safari/1.1',
        'Mozilla/5.0 (X11; Linux x86_64; rv:28.0) Gecko/20100101 Firefox/28.0 Iceweasel/28.0'
    ]

    
    def extract(self, domain: str):
        """
        extract takes a domain name as an argument, and returns the HTML content of the website
        successful requests are cached.
        
        :param domain: The domain name to extract the robots.txt file from
        :type domain: str
        :return: The content of the page.
        """
        try:
            local = self.fetch_cache(domain)
            if local is not None:
                return local
            
            url = "https://{}/".format(domain)
            headers = {
                'User-Agent': random.choice(self._user_agents)
            }
            page = requests.get(url, allow_redirects = True, headers=headers)
            self.insert_cache(domain, page)
            return page.content
        except Exception as e:
            logging.warn("Failed to load resource %s with error %s", domain, e)
            return None
    
    def insert_cache(self, domain, page):
        try:
            if _sqlite_db is None:
                logging.debug("Nothing to cache, db not initialized")
                return None
            cursor = _sqlite_db.cursor()
            res = cursor.execute("INSERT INTO site_cache(domain, html, status, retrieved) VALUES(?,?,?,?)", (domain, page.content, page.status_code, time.time()))  
            _sqlite_db.commit()
        except Exception as e:
            logging.debug('Failed to cache %s with error %s', domain, e)   
            return None

    def fetch_cache(self, domain):
        try:
            maxAge = time.time() - (60*60*6) #6 hours
            cursor = _sqlite_db.cursor()
            res = cursor.execute("SELECT html FROM site_cache WHERE domain = ? AND retrieved >= ?", (domain, maxAge)).fetchone()
            if len(res) > 0:
                return res[0]
            return None
        except Exception as e:
            logging.debug("Failed to load %s from cache with error %s", domain, e)
            return None


class ExtractLogos(etl.Transform):
    # A list of selectors that are used to extract the logo from the HTML.
    selectors = [
        selector.og_logo(),
        selector.og_image(),
        selector.appletouch_startupimg(),
        selector.shortcut_icon(),
        selector.appletouch(),
        selector.maskicon(),
        selector.favicon(),
        selector.fluidicon()
    ]

    def process(self, domain: str, html: str, eager: bool = False):
        """
        process takes a domain name and a HTML string, and returns a list of urls of extracted
        logos
        
        :param domain: The domain of the website being scraped
        :type domain: str
        :param html: The HTML of the page to be processed
        :type html: str
        :param eager: If True, the selector will be applied to the entire document. If False, the
        selector will be applied to the first element that matches the selector, defaults to False
        :type eager: bool (optional)
        :return: A list of dictionaries.
        """
        self.domain = domain
        self.eager = eager
        soup = BeautifulSoup(html, 'html.parser')
        result = []
        for selector in self.selectors:
            res = selector.apply(soup)
            if res is None:
                logging.debug("Selector %s failed on %s", selector.name, domain)
            fixed = []
            for logo in res:
                (ltype,uri) = logo
                fixed.append((ltype, self.normalize_url(domain, uri)))  
            result += fixed
            if eager and len(fixed) > 0:
                break
        return result
    
    def normalize_url(self, domain: str, uri: str):
        """
        > This function takes a domain and URL/URI and normalizes it to an absolute path
        
        :param uri: The URI to be normalized
        :type uri: str
        :return: The normalized url.
        """
        return requests.compat.urljoin('http://{}'.format(domain), uri)

# > It's a subclass of `etl.Load` that stores the results in memory and prints them to STDOUT
class InMemoryAndSTDOUTLoader(etl.Load):
    def __init__(self) -> None:
        self._results = dict()
        self._header = False
        _csv_writer.writerow(("domain", "logo", "source"))
        super().__init__()

    def insert(self, source, result):
        _loader_lock.acquire()
        self._results[source] = result
        self.print_entry(source, result)
        _loader_lock.release()
        
    
    def print_entry(self, domain, res):
        l = len(res)
        logging.debug("Found %d potential logos for %s", l, domain)
        if l == 0:
            return
        (logo_type, url) = res[0]
        _csv_writer.writerow((domain, url, logo_type))

    