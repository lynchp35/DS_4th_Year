import pandas as pd 
import numpy as np 
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf


@pandas_udf('long')
def fix_price(value: str) -> str:
    try:
        return str(int(value))
    except:
        if value == "Price on Application":
            return "PoA"
        else:
            new_value = "".join(value.split("(€")[1][:-1].split(","))
            return new_value

def find_county(address: str, counties: np.ndarray) -> str:
    try:
        countyName = address.split(",")[-1].split("Co. ")[-1].strip()
        if countyName not in counties and countyName.split(" ")[0] not in counties:
            return np.nan
        else:
            return countyName
    except:
        print(f"Address: {address} through up an error")

def find_province(county: str, county_dict: dict) -> str:
    if county in county_dict:
        return county_dict[county]
    elif county.split(" ")[0] == "Dublin":
        return county_dict["Dublin"]
    else:
        print(f"County: {county} was not found in dictionary")

        
def create_county_dict(df: pd.DataFrame) -> dict:
    county_dict = {}
    for i in range(len(df)):
        county_dict[df["County"][i]] = df["Province"][i]
    return county_dict

if main == "__name__":
    spark = SparkSession.builder.getOrCreate()
    pd_df = pd.read_csv("data/daft_from_page_0_till_page_987_by_20.csv")

    if "Unnamed: 0" in pd_df.columns:
        pd_df.drop(columns=["Unnamed: 0"], inplace=True)
    province_df = pd.read_csv("data/county_data.csv")
    
    df = spark.createDataFrame(pd_df)

