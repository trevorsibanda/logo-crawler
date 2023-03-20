"""

"""

import os
import logging

import time
import enum
from queue import Queue
from multiprocessing import Pool

class ETLStatus(enum.IntEnum):
    Failed = -1
    Complete = 0
    Extracting = 1
    Transforming = 2
    Loading = 3


# > The Extract class is an abstract class that defines the extract method and the handler method
class Extract:
    def extract(self, source):
        pass

    def handler(ex: Exception) -> None:
        pass


class Transform:

    def __init__(self) -> None:
        pass

    def process(self, data):
        pass

# > The `Load` class is a base class for loading data into a database
class Load:
    def insert(self, result):
        pass

    def handler(ex: Exception) -> None:
        pass

# Track ETL Process metrics
class ETLMetrics:
    def __init__(self) -> None:
        self.metrics = dict()
        self.metrics['tasks'] = dict()
    
    def start(self, key, status: ETLStatus):
        """
        Track the start of a process

        :param key: The key of the task
        :param status: The status of the task
        :type status: ETLStatus
        """
        if key not in self.metrics['tasks']:
           self.metrics['tasks'][key] = {
               ETLStatus.Extracting: 0.00,
               ETLStatus.Loading: 0.00,
               ETLStatus.Transforming: 0.00,
               'status': None,
               'last_start': time.time()
           }
        self.set_status(key, status)
    
    def failed(self, key):
        if key not in self.metrics['tasks']:
            logging.debug("Failed unknown task %s", key)
            return
        mt = self.metrics['tasks'][key]
        lastStatus = mt['status']
        mt[lastStatus] = -1 * (time.time() - mt['last_start']) 
        mt['last_start'] = time.time()
        self.metrics['tasks'][key] = mt

    
    def end(self, key):
        """
        Track the end of a process
        
        :param key: The key of the task
        :param status: The status of the task.
        :type status: ETLStatus
        """
        if key not in self.metrics['tasks']:
            raise RuntimeError("Cannot end unknown task")
        mt = self.metrics['tasks'][key]
        lastStatus = mt['status']
        mt[lastStatus] = time.time() - mt['last_start'] 
        mt['last_start'] = time.time()
        self.metrics['tasks'][key] = mt
        
        
    def set_status(self, key, status: ETLStatus):
        """
        Update the status of a task

        :param key: The name of the task
        :param status: ETLStatus.STARTED
        :type status: ETLStatus
        """
        if key not in self.metrics['tasks']:
            raise RuntimeError("Cannot end unknown task")
        mt = self.metrics['tasks'][key]
        if mt['status'] == status:
            logging.debug('Metrics error. Already started %s, ignoring.', status)
            return
        lastStatus = mt['status']
        mt[lastStatus] = time.time() - mt['last_start'] 
        mt['last_start'] = time.time()
        self.metrics['tasks'][key] = mt


# The ETLPipeline class takes in a config, an extractor, a transformer, and a loader.
# It has a status, and it has a run method that extracts, transforms, and loads data
class ETLPipeline:
    def __init__(self,config: dict, extract : Extract, transform: Transform, loader: Load , metrics: ETLMetrics = ETLMetrics()) -> None:
        """
        An ETL pipeline is the set of processes used to move data from a source or multiple sources into
        a database such as a data warehouse.
        
        ETL stands for “extract, transform, load,” the three interdependent processes of data integration
        used to pull data from one database and move it to another.
        
        :param config: This is a dictionary that contains the configuration for the ETL process
        :param extract: Extract instance to pull data from source.
        :type extract: Extract
        :param transform: Transform instance to process data.
        :type transform: Transform
        :param loader: Load instance to place the dataset into a data lake house.
        :type loader: Load
        """
        self.name = config['name']
        self.config = config
        self._extractor = extract
        self._transformer = transform
        self._loader = loader
        self._metrics = metrics
        self._status = 'idle'
        pass

    def report(self):
        print("*"*20)
        print("Status: ", self._status)
        self._metrics.print_report()

    def set_status(self, source, _status: ETLStatus):
        """
        This function sets the status of the ETL job to the status passed in as a parameter
        
        :params source: Index used for tracking
        :param _status: The status of the ETL job
        :type _status: ETLStatus
        """
        self._metrics.set_status(source, _status)

    def extract(self, source):
        """
        Given a source, extract data
        
        :param data: The data to be extracted
        """
        self._metrics.start(source, ETLStatus.Extracting)
        data = self._extractor.extract(source)
        self._metrics.end(source)
        return data
        
    def load(self, source, result):
        """
        Load the result into a data warehouse
        
        :param source: Index used for tracking
        :param result: The result of the transformation
        """
        self._metrics.start(source, ETLStatus.Loading)
        self._loader.insert(source, result)
        self._metrics.end(source)
        
    def transform(self, source, data):
        """
        Process the extracted data

        :param source: Index used for tracking
        :param data: The data to be transformed
        """
        self._metrics.start(source, ETLStatus.Transforming)
        logging.debug("Processing %s", source)
        result = self._transformer.process(source, data)
        self._metrics.end(source)
        return result

    def runner(self, source):
        """
        The runner function is responsible for extracting data from a source, transforming it and
        loading it into a destination
        
        :param source: The source of the data. This could be a file, a database, a web service, etc
        """
        logging.debug("Running %s", source)
        try:
            data = self.extract(source)
            
            if data is None:
                logging.warn("Failed to extract from %s", source)
                self.set_status(source, ETLStatus.Failed)
                return
            logging.debug("Extracted %dKb from %s", len(data)/1024, source)
            result = self.transform(source, data)
            if result is None:
                logging.warn("Transform failed on %s", source)
                self.set_status(source, ETLStatus.Failed)
                return
            self.load(source, result)
            logging.debug("Completed %s", source)
        except Exception as e:
            self.set_status(source, ETLStatus.Failed)
            logging.error("Failed to process request with error %s", e)
    
    def error_handler(self, error):
        logging.warn("Threadpool error handler got %s", error)

    def run(self, queue):
        """
        If there's a task in the queue, extract the data, transform it, and load it
        """
        logging.info("%s ETL runner started", self.name)
        nprocesses = 4
        if 'workers' in self.config:
            logging.info("Number of workers not set, defaulting to %d", nprocesses)
            nprocesses = self.config['workers']
        self._status = 'running'
        results = None
        pool = Pool(processes=nprocesses)
        results = pool.map_async(self.runner, queue, error_callback = self.error_handler)
        pool.close()
        pool.join()
        self._status = 'complete'
        