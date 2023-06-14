import glob
import pandas as pd
from datetime import datetime


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


columns = ['Name', 'Market Cap (US$ Billion)']


def extract():
    # Write your code here
    fname = "bank_market_cap_1.json"
    df = extract_from_json(fname)
    df2 = df[columns]
    return df2

    # Write your code here


df_exr = pd.read_csv("exchange_rates.csv", index_col=0)
exchange_rate = df_exr.loc["GBP", "Rates"]
print(df_exr.head())
print()
print("exchange_rate = ", exchange_rate)


def transform(dataFrame, ex_rate):
    # Write your code here
    usd2gbp = dataFrame.loc["USD", "Rates"] / ex_rate
    df_usd = extract()
    df_usd.loc[:, "Market Cap (GBP$ Billion)"] = round(df_usd.iloc[:, 1] / usd2gbp, 3)
    df_gbp = df_usd.drop("Market Cap (US$ Billion)", axis=1)
    return df_gbp, usd2gbp


def load(data_frame, filename):
    data_frame.to_csv(filename, index=False)
    # Write your code here

# load(df_gbp, "bank_market_cap_gbp.csv")
# !ls
# !cat bank_market_cap_gbp.csv
def log(msg):
    # Write your code here
    with open("log.txt", "a") as f:
        dt = datetime.today().strftime("%Y-%m-%d %H:%M-%S")
        f.write(str(dt) + "   " + str(msg) + "\n")
# Write your code here
log("ETL Job Started")
log("Extract phase Started")

# !cat log.txt
# !rm log.txt
# Call the function here
df0 = extract()
# Print the rows here
df0.head()
# Write your code here
log("Extract phase Ended")
# Write your code here
log("Transform phase Started")
# Call the function here
df_gbp, usd2gbp = transform(df_exr, exchange_rate)
# Print the first 5 rows here
print(df_gbp.head())
# Write your code here
log("Transform phase Ended")
# Write your code here
log("Load phase Started")
# Write your code here
load(df_gbp, "bank_market_cap_gbp.csv")
# Write your code here
log("Load phase Ended")