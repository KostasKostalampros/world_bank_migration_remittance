# Country Migration And Remittance Data Analysis
Time period (2010-2016)

The goal of this task is to pro-grammatically collect migration and remittance data from the World Bank website for all available years.
(http://www.worldbank.org/en/topic/migrationremittancesdiasporaissues/brief/migration-remittances-data)


This is the proposed architecture of the solution
![Top Level Workflow](https://github.com/KostasKostalampros/world_bank_migration_remittance/blob/master/img/workflow.png)

Proposed tables for the **_staging_** schema:
- country (id, country)
- remittance (country_origin_id, country_destination_id, remittance)
- migration (country_origin_id, country_destination_id, migration)

Proposed tables for the **_dwh_** schema:
- country (id, country)
- corridor (country_origin_id, country_destination_id, remittance, migration)


In the end, we would like this data to be inside a postgresql database that could answer this questions:
- Top 10 country_to_country by number of migrants
- Top 10 country_to_country by volume of remittances
- Top 10 sending countries
- Top 10 receiving countries
- Top 10 Net senders
- Top 10 Net receivers


## First part – Collect and Load

[collect_and_load_to_database.py](scripts/collect_and_load_to_database.py)

The python process carries out the following tasks:
1. Downloads xlsx files from world bank 
2. Creates a csv file from each xslx
3. Parses each csv data file
4. Create three aggregate csv files - country.csv, migration.csv and remittance.csv 
5. Connects to a PostgreSQL engine set up in Amazon RDS service
6. Creates a schema called _staging_ and three tables (country, remittance and migration) according to the three available csv files
7. Imports each of the csv data files (country.csv, migration.csv, remittance.csv) to its respective table 

Some other sub-tasks carried out during parsing:
- Cleansing of the input files by removing extra spaces, commas from values, stars (*), and N/A values
- Unique ids generated for countries appeared between Afghanistan and before total World figure including Other North and Other South.


## Second Part – Process for Data Warehouse 

[dwh_process.sql](scripts/dwh_process.sql)

A free tier service of PostgreSQL engine in Amazon WS can be created to store the migration and remittance data. See example of connection details below:
- **Endpoint:** xxx.xxx.eu-west-2.rds.amazonaws.com
- **Port:** 5431
- **Database:** main_database
- **User:** postgres
- **Password:** postgres

The second part of the process will be carried out in PostgreSQL engine included the following tasks:
1. Create new schema for the data warehouse called _dwh_
2. Copy the country table from the staging schema across
3. Create a new table _dwh.corridor_ as product of a query joining the _staging.remittance_ and _staging.migration_ tables

## Third part - Answering to Questions

All queries have been stored in [scripts/answers.sql](scripts/answers.sql)