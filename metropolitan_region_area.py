import requests
import pandas as pd
from requests import HTTPError


def build_request_url(year: int) -> str:
    """Building an URL for a get request from Eurostat API"""
    request_url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/met_d3area" \
                  f"?format=JSON&time={str(year)}&unit=KM2&landuse=TOTAL&lang=en"
    return request_url


def generate_response(url: str) -> dict:
    """Generating response from Eurostat API in JSON format"""
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
    """Extracting data about regions from API response"""
    regions_df = pd.DataFrame.from_dict(dimension_metro_reg_category)
    regions_df.reset_index(inplace=True)
    regions_df.rename({"level_0": "region_code"}, axis=1, inplace=True)
    return regions_df


def extract_values(response: dict, regions: pd.DataFrame, year: int) -> pd.DataFrame:
    """Extracting values from API response"""
    values_s = pd.Series(response["value"])
    values_s.index = values_s.index.astype(dtype="int64")
    values_df = values_s.to_frame().join(regions)
    values_df = values_df.rename({0: "area_in_km2"}, axis=1)
    values_df = values_df.sort_index()
    values_df.drop("index", axis=1, inplace=True)
    values_df["year"] = year
    return values_df
