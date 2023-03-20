# Website logo extractor

Simple multithreaded Python program that reads a list of sites from stdin and visits each site,
attempting to figure out the website's logo.

## Description

This application is modelled as an ETL pipeline. Where the HTTP request or cache load is the `extraction` phase,
applying a range of `beautifulsoup` selectors to extract and normalize logo links as the `transform` phase and
storing all extracted links in memory and writing them to stdout as the `load` phase.

This pattern seemed intuitive and easily allows extending the functionality of the program and modelling it
in a way that is both horizontally and vertically scalable. Scalability can be implemented at most layers of the 
application with minimal third party dependencies.


To understand the source code. Start at `py/logocrawler/main.py` then read `py/logo/logocrawler.py`

## Getting Started

Using nix in the root directory:

```bash
$ nix-shell
$ wc -l websites.csv #number of sites to crawl
$ ./runner.sh
```

Several parameters are configurable as environment parameters inside nix-shell before running the program

```bash
$ export ETL_WORKERS = 4 #number of concurrent workers
$ export ETL_LOG = $(pwd)/debug.log #log file set to debug level
$ export ETL_CACHE_DB = "./websites.sqlite" #sqlite cache db
$ export ETL_MAX_CACHE_AGE=36000 #max age of downloaded site landing page
$ export ETL_NAME="logo-extractor" #etl pipeline name
$ 
```

### Dependencies

* [beautifulsoup4](https://pypi.org/project/beautifulsoup4/)
* [requests](https://pypi.org/project/requests/)

### Key features

* Multithreaded concurrent pipelines
* Scalable design, either vertically or horizontally
* Minimum external dependencies
* Error reporting
* Caching of HTTP requests
* Documented code
* Easily extensible to add new logo extraction rules

### Limitations/TODO

* Cannot read websites from stdin in a non-blocking manner
* Selectors used to retrieve logos are not exhaustive
* Metrics tracking is very rudimentary and reporting function is not implemented 
* Global shared cache db resource and stdout loader lock
* Any modifications needed to be made to files/folders
* Not enough time to write unit tests


## Authors

Trevor Sibanda  
[@trevorsibanda](https://github.com/trevorsibanda)

## Version History

* 0.1
    * Initial Release
