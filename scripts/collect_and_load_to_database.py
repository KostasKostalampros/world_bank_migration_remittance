import csv
import psycopg2
import xlrd
import urllib.request
from collections import OrderedDict


def excel_to_csv(url_path, xlsx_file_path, csv_file_path, file_name):
    """Parse excel file and generate the identical csv file.

    Keyword arguments:
    url_path -- string
    xlsx_file_path -- string	
    csv_file_path -- string	
    file_name -- string	

    Return string of the csv file path
    """

    # File paths to store the data files
    xlsx_file_path+=file_name + ".xlsx"
    csv_file_path+=file_name + ".csv"
    
    urllib.request.urlretrieve(url_path, xlsx_file_path)

    wb = xlrd.open_workbook(xlsx_file_path)
    sheet_names = wb.sheet_names()
    sh = wb.sheet_by_name(sheet_names[0])

    # Create csv file and write xlsx data
    with open(csv_file_path, 'w', newline="") as opened_file:
        wr = csv.writer(opened_file, quoting=csv.QUOTE_ALL)
        for rownum in range(sh.nrows):
            wr.writerow(sh.row_values(rownum))

    return csv_file_path


def generate_country_table(source_urls, csv_file_path):
    """Parse input csv files and generate an aggregate csv file with id, country.

    Keyword arguments:
    source_urls -- list
    csv_file_path -- string	

    Return list of aggregated country data
    """

    country_id = ["id"]
    country_name = []

    # Add all countries found in all files and then deduplicate the file for double entries
    for url in source_urls:
        with open(url, "r") as opened_file:
            # Skip the first two rows
            reader = csv.reader(opened_file)
            next(reader)
            next(reader)

            for row in reader:
                # read all rows until "World" or "TOTAL" or "Unidentified*"
                if row[0] == "World" or row[0] == "TOTAL" or row[0] == "Unidentified*": break
                country_name.append(row[0])

    deduped_country_name = list(set(country_name))
    deduped_country_name.sort()
    deduped_country_name.insert(0,"country")
    country_id.extend(list(range(1, len(deduped_country_name))))

    country_table = [country_id, deduped_country_name]

    # Export country(country_id, country_name) table into csv format
    with open(csv_file_path + "country.csv", "w", newline="") as file:
        writer = csv.writer(file, delimiter=";")
        for iter in range(len(country_table[0])):
            writer.writerow([x[iter] for x in country_table])

    return country_table


def parse_csv_and_return_data(source_urls, csv_file_path, country_table, figure_column_name):
    """Parse input csv files and generate an aggregate csv file.

    Keyword arguments:
    source_urls -- list
    csv_file_path -- string
    country_table -- list
    figure_column_name -- string	

    Return list of aggregated data
    """

    # Define all table columns as lists
    global figure
    list_date = ["Date"]
    list_country_origin = ["country_origin_id"]
    list_country_destination = ["country_destination_id"]
    list_figures = [figure_column_name]

    # Iterate all incoming source urls
    for source_url in source_urls:

        with open(source_url, "r") as file:
            reader = csv.reader(file)
            # Skip the first row
            next(reader)

            country_destination_row = next(reader)  # gets the destination country row

            # Read date from file
            date = source_url[-8:-4] +"-01-01"

            if figure_column_name == "migration":
                column_end = -2
            if figure_column_name == "remittance":
                column_end = -1

            # Parse and store names of destination countries excluding first cell and World total
            country_destination_names = []
            for element in country_destination_row[1:column_end]:
                country_destination_names.append(element)


            for row in reader:
                # read all rows until "World" or "TOTAL" or "Unidentified*"
                if row[0] == "World" or row[0] == "TOTAL" or row[0] == "Unidentified*": break

                # Reade country name and then retrieve id from country_table
                country_origin_name = row[0]
                country_table_index = country_table[1].index(country_origin_name)
                country_origin_name_id = country_table[0][country_table_index]

                index_country_destination = 0
                for element in row[1:column_end]:
                    # add country_destination_id
                    country_destination_name = country_destination_names[index_country_destination]
                    try:
                        country_table_index = country_table[1].index(country_destination_name)
                    except ValueError:
                        continue
                    country_destination_name_id = country_table[0][country_table_index]
                    list_country_destination.append(country_destination_name_id)
                    # add date
                    list_date.append(date)
                    # add country_origin_id
                    list_country_origin.append(country_origin_name_id)
                    # add migration figure
                    figure = element.strip().replace(",","").replace("*","").replace("N/A", "")
                    list_figures.append(figure)

                    index_country_destination+=1

    final_table = [list_date, list_country_origin, list_country_destination, list_figures]

    # Export migration(country_origin_id, country_destination_id, migration) table into csv format
    with open(csv_file_path + figure_column_name + ".csv", "w", newline="") as file:
        writer = csv.writer(file, delimiter=";")
        for i in range(len(final_table[0])):
            writer.writerow([x[i] for x in final_table])

    return final_table




def execute_create_schema_postresql(cursor, schema_name):
    """Execute a CREATE PostgreSQL query as specified by function arguments.

    Keyword arguments:
    cursor -- psycopg2 connection cursor
    schema_name -- string

    Return None
    """

    sql_string = "CREATE SCHEMA " + schema_name + ";"

    cursor.execute(sql_string)

    return None


def execute_create_table_postresql(cursor, table_name, table_columns):
    """Execute a CREATE PostgreSQL query as specified by function arguments.

    Keyword arguments:
    cursor -- psycopg2 connection cursor
    table_name -- string
    table_columns - dict {column_name: data_type [other_constrains,]}

    Return None
    """

    sql_string = "CREATE TABLE " + table_name + "("

    iter=0 # Formatting iterator
    for key, value in table_columns.items():
        if iter==0:
            sql_string+="\n" + key + " " + value
        else:
            sql_string+=",\n" + key + " " + value

        iter+=1

    sql_string+="\n);"

    cursor.execute(sql_string)

    return None



def import_postgresql_table_data(cursor, table_name, file_path, file_type="CSV", header=False):
    """Execute a \COPY PostgreSQL query to import data from data to a table.

    Keyword arguments:
    cursor -- psycopg2 connection cursor
    table_name -- string
    file_path -- string
    file_type -- string (default CSV)	
    delimiter -- string (default ',')
    header -- boolean (default False)

    Return None
    """

    # Format header argument
    if header:
        header_string = "HEADER"
    else:
        header_string = ""

    string_copy_statement = "COPY " + table_name + " FROM stdin WITH DELIMITER ';' " +  file_type + " " + header_string

    # Copy data from file to table
    with open(file_path, "r") as file:
        cursor.copy_expert(string_copy_statement, file)

    #with open("country.csv", "r") as file:
        #cursor.copy_expert("""COPY staging.country FROM stdin WITH #DELIMITER ';' CSV HEADER""", file)

    return None




def main():

    # File paths to save datasources
    xlsx_file_path = "../datasets/excel_files/"
    csv_file_path = "../datasets/csv_files/"

    xslx_file_urls = OrderedDict([
	                 ("http://www.knomad.org/sites/default/files/2017-11/bilateralremittancematrix2016_Nov2017.xlsx", "bilateralremittancematrix2016"),
                     ("http://pubdocs.worldbank.org/en/892861508785913333/bilateralremittancematrix2015-Oct2016.xlsx", "bilateralremittancematrix2015"),
                     ("http://pubdocs.worldbank.org/pubdocs/publicdoc/2015/10/936571445543163012/bilateral-remittance-matrix-2014.xlsx",   "bilateralremittancematrix2014"),
                     ("http://pubdocs.worldbank.org/pubdocs/publicdoc/2015/9/103071443117530921/Bilateral-Remittance-Matrix-2013.xlsx","bilateralremittancematrix2013"),
                     ("http://pubdocs.worldbank.org/pubdocs/publicdoc/2015/9/106901443117530403/Bilateral-Remittance-Matrix-2012.xlsx","bilateralremittancematrix2012"),
                     ("http://pubdocs.worldbank.org/pubdocs/publicdoc/2015/9/919711443117529856/Bilateral-Remittance-Matrix-2011.xlsx","bilateralremittancematrix2011"),
                     ("http://pubdocs.worldbank.org/pubdocs/publicdoc/2015/9/895701443117529385/Bilateral-Remittance-Matrix-2010.xlsx","bilateralremittancematrix2010"),
                     ("http://pubdocs.worldbank.org/pubdocs/publicdoc/2015/10/38881445543162029/bilateral-migration-matrix-2013-0.xlsx","bilateralmigrationmatrix2013"),
                     ("http://siteresources.worldbank.org/INTPROSPECTS/Resources/334934-1110315015165/T1.Estimates_of_Migrant_Stocks_2010.xls","bilateralmigrationmatrix2010")
                     ])

    csv_files = []
    # Convert xlsx files to csv
    for url_path, file_name in xslx_file_urls.items():
        print("Downloading file %s..."%(file_name))
        csv_files.append(excel_to_csv(url_path, xlsx_file_path, csv_file_path, file_name))

    # Construct country table
    country_table = generate_country_table(csv_files, csv_file_path)

    # Filter csv_files to keep only migration data
    migration_csv_files = [file for file in csv_files if 'migration' in file]
    # Generate migration table
    migration_table = parse_csv_and_return_data(migration_csv_files, csv_file_path, country_table, "migration")

    # Filter csv_files to keep only remittance data
    remittance_csv_files = [file for file in csv_files if 'remittance' in file]
    # Generate remittance table
    remittance_table = parse_csv_and_return_data(remittance_csv_files, csv_file_path, country_table, "remittance")


    # Connect to a PostgreSQL database
    try:
        connection = psycopg2.connect("host=worldremmit-kostas-instance.cmqcobkebaom.eu-west-2.rds.amazonaws.com \
                                      port=5431 \
                                      dbname=main_database \
                                      user=postgres \
                                      password=postgres")
    except:
        print("Unable to connect to the database")

    cursor = connection.cursor()
    # Create schema, country, migration and remittance tables and insert data in the database
    staging_schema_name = "staging"
    execute_create_schema_postresql(cursor, staging_schema_name)

    # Create staging.country table (id, country)
    country_table_name = "staging.country"
    country_table_column_dictionary = OrderedDict([
                                      ("id", "INTEGER PRIMARY KEY"),
                                      ("country", "TEXT")
                                      ])
    execute_create_table_postresql(cursor, country_table_name, country_table_column_dictionary)
    
    # Commit all executions to the server
    connection.commit()

    # Create staging.remittance table (country_origin_id, country_destination_id, remittance)
    remittance_table_name = "staging.remittance"
    remittance_table_column_dictionary = OrderedDict([
                                         ("date", "DATE"),
                                         ("country_origin_id", "INTEGER REFERENCES staging.country(id)"),
                                         ("country_destination_id", "INTEGER REFERENCES staging.country(id)"),
                                         ("remittance", "NUMERIC")
                                         ])
    execute_create_table_postresql(cursor, remittance_table_name, remittance_table_column_dictionary)

    # Create staging.migration table (country_origin_id, country_destination_id, migration)
    migration_table_name = "staging.migration"
    migration_table_column_dictionary = OrderedDict([
                                        ("date", "DATE"),
                                        ("country_origin_id", "INTEGER REFERENCES staging.country(id)"),
                                        ("country_destination_id", "INTEGER REFERENCES staging.country(id)"),
                                        ("migration", "NUMERIC")
                                        ])
    execute_create_table_postresql(cursor, migration_table_name, migration_table_column_dictionary)
    
    # Commit all executions to the server
    connection.commit()

    # Copy csv data into newly created Postgresql tables
    import_postgresql_table_data(cursor, "staging.country", csv_file_path + "country.csv", "CSV", True)
    import_postgresql_table_data(cursor, "staging.remittance", csv_file_path + "remittance.csv", "CSV", True)
    import_postgresql_table_data(cursor, "staging.migration", csv_file_path + "migration.csv", "CSV", True)
    
    # Commit all executions to the server
    connection.commit()

    # Close the PostgreSQL server connection
    connection.close()
	

if __name__ == "__main__":
    main()