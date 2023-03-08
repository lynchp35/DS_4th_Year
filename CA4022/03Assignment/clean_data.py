import pandas as pd 
import numpy as np 
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf

# @pandas_udf('long') I need to look into how to use UDF and PySpark
def fix_price(value: str) -> str:

    """
    Currently price is saved as a sring due to some values being 'Price on Application' 
    and others are listed in £ and € e.g. £100,000 (€115,000). 

    We will take in the value and either remove the £ price or save 'Price on Application'
    as 'PoA'
    """

    try:
        return str(int(value))
    except:
        if value == "Price on Application":
            return "PoA"
        else:
            new_value = "".join(value.split("(€")[1][:-1].split(","))
            return new_value

def find_county(address: str, counties: np.ndarray) -> str:

    """
    The address is comma seperated with the county at the end. (Some adresses are missing the county).
    We use the array counties to check if the county found is actually a county.

    We also need to check for counties such as Dublin 1, Dublin 2 etc.

    """

    try:
        countyName = address.split(",")[-1].split("Co. ")[-1].strip()
        if countyName not in counties and countyName.split(" ")[0] not in counties: # First check if the county is not in the array then checks for Dublin adresses.
            return np.nan
        else:
            return countyName
    except:
        print(f"Address: {address} through up an error")

def find_province(county: str, county_dict: dict) -> str:

    """"
    This function aims to find the province using the county found with the last function.

    It takes the county in and dictionary to return the correct province.
    """

    if county in county_dict: # Works for Counties outside of Dublin
        return county_dict[county]
    elif county.split(" ")[0] == "Dublin": # Works for Dublin
        return county_dict["Dublin"]
    else:
        print(f"County: {county} was not found in dictionary")

        
def create_county_dict(df: pd.DataFrame) -> dict:

    """
    Takes in dataframe with the columns County and Province,
    then returns a dictionary with the county as the key and the province as the value.
    This dictionary is used in the function above.
    """

    county_dict = {}
    for i in range(len(df)):
        county_dict[df["County"][i]] = df["Province"][i]
    return county_dict

if main == "__name__":
    spark = SparkSession.builder.getOrCreate()
    pd_df = pd.read_csv("data/daft_from_page_0_till_page_987_by_20.csv")

    if "Unnamed: 0" in pd_df.columns:
        pd_df.drop(columns=["Unnamed: 0"], inplace=True)
    
    df = spark.createDataFrame(pd_df) # I don't know how this works
    province_df = pd.read_csv("data/county_data.csv")
    county_dict = create_county_dict(province_df)

    df["Price"] = df["Price"].apply(lambda x: fix_price(x))
    df["County"] = df["Address"].apply(lambda x: find_county(x, province_df["County"].values))

    df.dropna(subset=["County"], inplace=True) # Haven't tested this so it maybe wrong.

    df["Province"] = df["Address"].apply(lambda x: find_province(x, county_dict))

    # If the listing is a site there won't be any beds or baths, current value = Null
    df["Bed"][df["Property_type"] == "Site"] = 0
    df["Bath"][df["Property_type"] == "Site"] = 0


    # I am not sure if there is anything else that we would need to clean.
    # In the next section we could look into imputing some null values and maybe include a hierarchical structure for  Property_type.
    # Currently 11 Property_types but users may want to look at a higher level view.
    # May also look into D3.js for visualisations.


