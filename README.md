# Introduction


# Quick Start Guide

1. Ensure Python 3.6+ is installed
2. Clone the repo: ```git clone https://github.com/aivoric/Data-Engineering-ND-Modelling-Postgres-Project-1.git```
3. Enter the downloaded filter and create a local environment: ```python3 -m venv env```
4. Install dependencies: ```pip install -r requirements.txt```

The next steps assume you have Postgres up and running with the following defaults:
```"host=127.0.0.1 dbname=studentdb user=postgres password=postgres"```

5. Run create_tables.py to setup the database: ```python create_tables.py```
6. Run etl.py which will process the json data inside the data folder: ```python etl.py```
7. Run test.py which will query the create data and show sample data in the terminal: ```python test.py```

# Setting Up Postgres Locally

Postgres can be tricky tp setup locally. Here is a useful guide for Mac users (sorry Windows users!):
https://www.codementor.io/@engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb

# Database Structure