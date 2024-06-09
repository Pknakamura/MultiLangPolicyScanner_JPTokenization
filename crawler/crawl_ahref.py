import requests
from bs4 import BeautifulSoup
import pandas as pd
import json

def get_top_website(country):
    url = "https://www.ahrefs.com/top/" + country
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # look for tbody table
    tables = soup.find_all("tbody")


    top100 = tables[0]

    # create an empty dataframe with columns rank, url, traffic, increase_traffic
    df_website = pd.DataFrame(columns=["rank", "url", "traffic", "increase_traffic"])
    # create a dictionary with keys rank, url, traffic, increase_traffic 
    list_website = []

    dict_website = {}

    for row in top100.find_all("tr"):
        cell_values = [cell.text for cell in row.find_all("td")]
        cell_values.pop(1)

        url = cell_values[1]
        rank = cell_values[0]
        traffic = cell_values[2]
        increase_traffic = cell_values[3]

        # create a new dictionary for each row
        dict_website = {
            "rank": rank,
            "url": url,
            "traffic": traffic,
            "increase_traffic": increase_traffic
        }

        # add to list
        list_website.append(dict_website)


        df_website = df_website._append(pd.Series(cell_values, index=df_website.columns), ignore_index=True)
    
    # print(list_website)
    return df_website

# MAIN FUNCTION

country = ["korea", "china", "japan"]

for country in country:

    top100 = get_top_website(country)
    # write to json
    # with open(f"../data/top100_{country}_ahref_april2024.json", "w") as f:
    #     json.dump(top100, f, ensure_ascii=True, indent=4)

    # print(top100)

    # save to csv and in the name append ahref and april2024
    top100.to_csv(f"./data/top100_{country}_ahref_april2024.csv", index=False)

    # print(top100)