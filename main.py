"""This script connects with Eurostat API and extracts data from dataset 'Area of the regions by metropolitan regions'.
The script then saves the data in the database of your selection.

The script requires a config file that is imported and that contains information about MSSQL server, database name,
database login database password and database schema used.

Example config file code:
sql_server = ''
database_name = ''
db_login = ''
db_pass = ''
DB_SCHEMA = ''
"""

import urllib
import pandas as pd
import pyodbc
import sqlalchemy as sql
import config
import metropolitan_region_area as mra


def open_database_connection() -> sql.engine:
    """Function for creating sqlalchemy engine used to upload data to database"""
    sql_server = config.sql_server
    database_name = config.database_name
    db_login = config.db_login
    db_pass = config.db_pass
    driver = "{ODBC Driver 17 for SQL Server}"

    odbc_str = 'DRIVER='+driver+';SERVER='+sql_server+';PORT=1433;UID='+db_login+';DATABASE=' + database_name + ';PWD='\
               + db_pass
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)

    return sql.create_engine(connect_str)


def main():
    """Main function generating the data and uploading it into database."""

    engine = open_database_connection()
    regions_full = pd.DataFrame
    values_full = pd.DataFrame

    """Extracting area by metropolitan region data for years 2020-2022"""
    for y in range(2020, 2023):
        """Building URL, generating response and extracting data from the response"""
        area_url = mra.build_request_url(y)
        response = mra.generate_response(area_url)
        regions = mra.extract_regions(response["dimension"]["metroreg"]["category"])
        values = mra.extract_values(response, regions, y)

        """Checking if it's the first year extracted. If not, a list of dataframe is create. One is an already existing
        dataframe that is holding data from previous responses and the other is newly extracted data"""
        if y == 2020:
            region_dfs_to_concat = [regions]
            values_dfs_to_concat = [values]
        else:
            region_dfs_to_concat = [regions, regions_full]
            values_dfs_to_concat = [values, values_full]

        regions_full = pd.concat(region_dfs_to_concat)
        values_full = pd.concat(values_dfs_to_concat)

    regions_full.drop_duplicates(inplace=True)
    regions_full.to_sql(con=engine, name="metropolitan_regions", if_exists="replace", schema=config.DB_SCHEMA)
    values_full.to_sql(con=engine, name="metropolitan_area", if_exists="replace", schema=config.DB_SCHEMA)
    engine.dispose()

if __name__ == "__main__":
    main()
