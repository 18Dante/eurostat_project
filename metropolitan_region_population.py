import requests
import pandas as pd
from requests import HTTPError


def build_request_url(year: int, sex: str, age_group: str) -> str:
    """Building an URL for a get request from Eurostat API"""
    request_url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/met_pjangrp3" \
                  f"?format=JSON&time={str(year)}&unit=NR&sex={sex}&age={age_group}&lang=en"
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


def extract_values(response: dict, regions: pd.DataFrame, year: int, sex: str, age_group: str) -> pd.DataFrame:
    """Extracting values from API response"""
    values_s = pd.Series(response["value"])
    values_s.index = values_s.index.astype(dtype="int64")
    values_df = values_s.to_frame().join(regions)
    values_df = values_df.rename({0: "population"}, axis=1)
    values_df = values_df.sort_index()
    values_df.drop("index", axis=1, inplace=True)
    values_df["year"] = year
    if sex == "T":
        values_df["sex"] = "Total"
    elif sex == "M":
        values_df["sex"] = "Male"
    elif sex == "F":
        values_df["sex"] = "Female"
    values_df["age_group"] = age_group
    return values_df

# test_url = build_request_url(2021, 'T', 'Y_LT5')
#
# resp = generate_response(test_url)
#
# regs = extract_regions(resp["dimension"]["metroreg"]["category"])
#
# vals = extract_values(resp, regs, 2021)
# print(vals)
def generate_dataframe():
    regions_list = []
    values_list = []
    regions_full = pd.DataFrame
    values_full = pd.DataFrame
    for y in range(2020, 2023):
        for s in ["T", "M", "F"]:
            for a in ['Y_LT5', 'Y5-9', 'Y10-14', 'Y15-19', 'Y20-24', 'Y25-29', 'Y30-34', 'Y35-39', 'Y40-44', 'Y45-49',
                      'Y50-54', 'Y55-59', 'Y60-64', 'Y65-69', 'Y70-74', 'Y75-79', 'Y80-84', 'Y85-89', 'Y_GE90', 'UNK']:
                url = build_request_url(y, s, a)
                res = generate_response(url)
                regs = extract_regions(res["dimension"]["metroreg"]["category"])
                vals = extract_values(res, regs, y, s, a)

                regions_list.append(regs)
                values_list.append(vals)

    values_full = pd.concat(values_list)
    return values_full
