# Project: Sparkify Data ETL 

This project was completed on 5th June 2021.

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 

All their data is stored in JSON files and is difficult to analyze. The requirements are to process all the data stored inside the JSON
files, transform it, and then store it inside a Postgres database so that it can be easily queried by Sparkify's analytics team.

This project hence contains:
1. Scripts to setup the database structure with the necessary tables for sparkifydb.
2. ETL pipeline to process the json data.
3. Tests to ensure data has been inserted into sparkifydb correctly.

## Quick Start Guide

1. Ensure Python 3.6+ is installed
2. Clone the repo: ```git clone https://github.com/aivoric/Data-Engineering-ND-Modelling-Postgres-Project-1.git```
3. Enter the downloaded folder and create a virtual environment: ```python3 -m venv env```
4. Install dependencies: ```pip install -r requirements.txt```

The next steps assume you have Postgres up and running with the following defaults:
```"host=127.0.0.1 dbname=studentdb user=postgres password=postgres"```

5. Run create_tables.py to setup the database: ```python create_tables.py```
6. Run etl.py which will process the json data inside the /data folder: ```python etl.py```
7. Run test.py which will query the inserted data and show sample data in the terminal: ```python test.py```

## Setting Up Postgres Locally

Postgres can be tricky tp setup locally. Here is a useful guide for Mac users (sorry Windows users!):
https://www.codementor.io/@engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb


## Notes on Files

```sql_queries.py``` contains all the SQL statements for creating, inserting and selecting data for 5 tables:
* songs
* artists
* users
* time
* songplays

```create_tables.py``` contains the logic for connecting to Postgres, and re-creating the entire sparkify database structure. Only run this file when you want to drop the entire database and re-create it from scratch. It uses SQL scripts contained in ```sql_queries.py```

```etl.py``` contains the logic for the data pipeline. It processes files located in the ```/data``` folder and then uses SQL scripts contained in ```sql_queries.py``` to insert data into sparkifydb.

```test.py``` uses SQL scripts contained in ```sql_queries.py``` to check that data has been inserted into the database.

```etl.ipyng``` can be ignored. It was used for the purpose of developing the etl pipeline.