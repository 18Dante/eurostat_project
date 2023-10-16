import requests
import urllib
import pandas as pd
import pyodbc
import sqlalchemy as sql
import os
from requests import HTTPError
import config

def open_database_connection() -> sql.engine:
    sql_server = config.sql_server
    database_name = config.database_name
    db_login = config.db_login
    db_pass = config.db_pass
    driver = "{ODBC Driver 17 for SQL Server}"

    odbc_str = 'DRIVER='+driver+';SERVER='+sql_server+';PORT=1433;UID='+db_login+';DATABASE='+ database_name + ';PWD='+ db_pass
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)

    return sql.create_engine(connect_str)



def build_request_url(year: int) -> str:
    request_url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/met_d3area" \
                  f"?format=JSON&time={str(year)}&unit=KM2&landuse=TOTAL&lang=en"
    return request_url


def generate_response(url: str) -> dict:
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_response = response.json()
        return json_response
    except HTTPError as http_err:
        print(f"HTTP error occured: {http_err}")
    except Exception as exc:
        print(f"Other error occured: {exc}")

def extract_regions(dimension_metro_reg_category: dict) -> pd.DataFrame:
    regions_df = pd.DataFrame.from_dict(dimension_metro_reg_category)
    regions_df.reset_index(inplace=True)
    regions_df.rename({"level_0": "region_code"}, axis=1, inplace=True)
    # regions_s = pd.Series(metro_reg, name="region_label")
    # regions_s.index.name = "region_code"
    return regions_df

def extract_values(response: dict, regions: pd.DataFrame, year: int) -> pd.DataFrame:
    values_s = pd.Series(response["value"])
    values_s.index = values_s.index.astype(dtype="int64")
    values_df = values_s.to_frame().join(regions)
    values_df = values_df.rename({0: "area_in_km2"}, axis=1)
    values_df = values_df.sort_index()
    values_df.drop("index", axis=1, inplace=True)
    values_df["year"] = year
    print(values_df.columns)
    return values_df


engine = open_database_connection()
regions_full = pd.DataFrame
values_full = pd.DataFrame

for y in range(2020, 2023):
    area_url = build_request_url(y)
    response = generate_response(area_url)
    regions = extract_regions(response["dimension"]["metroreg"]["category"])
    values = extract_values(response, regions, y)
    if y == 2020:
        region_dfs_to_concat = [regions]
        values_dfs_to_concat = [values]
    else:
        region_dfs_to_concat = [regions, regions_full]
        values_dfs_to_concat = [values, values_full]

    regions_full = pd.concat(region_dfs_to_concat)
    values_full = pd.concat(values_dfs_to_concat)

DB_SCHEMA = "eurostat_project"
regions_full.drop_duplicates(inplace=True)
regions_full.to_sql(con=engine, name="metropolitan_regions", if_exists="replace", schema=DB_SCHEMA)
values_full.to_sql(con=engine, name="metropolitan_area", if_exists="replace", schema=DB_SCHEMA)
engine.dispose()
# print(regions_full)
