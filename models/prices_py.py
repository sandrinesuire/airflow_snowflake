import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col
import numpy as np
import pandas as pd


def model(dbt, session):

    dbt.config(packages=["pandas", "numpy"])

    # my_sql_model_df = dbt.ref("_prices")
    data = dbt.source("dbt_ssuire","snowtable").to_pandas()
    # to test source available : dbt test --select source:dbt_ssuire.snowtable

    data=data[["MYDATE", "ADJCLOSE"]]
    data["MYDATE"] = pd.to_datetime(data['MYDATE'], format='%Y-%m-%d')
    data["ADJCLOSE"] = data['ADJCLOSE'].astype(float)
    data=data.sort_values('MYDATE')
    data=data.set_index("MYDATE")

    data.rename({"ADJCLOSE": "PRICE"}, axis=1, inplace=True)
    data["RET"] = data["PRICE"].pct_change()

    data["LOG_RET"] = np.log(data["PRICE"]/data["PRICE"].shift())

    data = data.iloc[1:]
    data["CUM_RET"] = (data["RET"]+1).cumprod() -1
    data["CUM_LOG_RET"] = (data["LOG_RET"]+1).cumprod() -1

    data["YTD_RET"]=data.groupby([data.index.year])[["PRICE", 'RET']].apply(
    lambda x: (x["PRICE"]/(x["PRICE"].iloc[0]/(x["RET"].iloc[0]+1))-1.0).to_frame("YTD_RET"))
    data["QTD_RET"]=data.groupby([data.index.year, data.index.quarter])[["PRICE", 'RET']].apply(
    lambda x: (x["PRICE"]/(x["PRICE"].iloc[0]/(x["RET"].iloc[0]+1))-1.0).to_frame("MTD_RET"))
    data["MTD_RET"]=data.groupby([data.index.year, data.index.month])[["PRICE", 'RET']].apply(
    lambda x: (x["PRICE"]/(x["PRICE"].iloc[0]/(x["RET"].iloc[0]+1))-1.0).to_frame("MTD_RET"))
    data["WTD_RET"]=data.groupby([data.index.strftime('%W %y')])[["PRICE", 'RET']].apply(
    lambda x: (x["PRICE"]/(x["PRICE"].iloc[0]/(x["RET"].iloc[0]+1))-1.0).to_frame("MTD_RET"))

    
    data["MA12"]=data["PRICE"].rolling(10).mean()
    data["EMA12"]=data["PRICE"].ewm(span=10, adjust=False).mean() # poids exponentielle en fonction de la date
    data["DD"] = data["PRICE"]/(data["PRICE"].expanding().max()) - 1.0
    data["PEAK"] = data["PRICE"].expanding().max()
    data["PEAK_PER"] = 0
    def f(df):
        compl=np.zeros(data.shape[0])
        for i in range(len(compl)-1):
            if data.iloc[i]["PRICE"] != data.iloc[i]["PEAK"]:
                compl[i]=compl[i-1]+1
        return compl

    data["PEAK_PER"]=f(data)
    data["RET_ROL_VOL_6MTH"] = data["RET"].rolling(window=126).std()
    data["RET_ROL_6MTH_SHARP"] = data["RET"].rolling(window=126).mean()/data["RET"].rolling(window=126).std()
    data["PRICE_ROLL_MDD_12MTH"]=data["DD"].rolling(window=252, min_periods=1).min()


    data=data.reset_index()
    data["MYDATE"]=data["MYDATE"].astype(str)
    return data