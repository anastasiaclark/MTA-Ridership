# -*- coding: utf-8-*-

'''This script scrapes the ridership data from MTA's website and
combines it with the previous ridership data stored at GIS lab.
'''

from functools import reduce

import os
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup

# configure logger
logger = logging.getLogger (__name__)
logger.setLevel (logging.INFO)
handler = logging.FileHandler ("error_log.log")
handler.setLevel (logging.ERROR)
formatter = logging.Formatter ("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter (formatter)
logger.addHandler (handler)

previous_file = "/Users/anastasiaclark/MyStaff/Git_Work/MTA-Ridership/updates/2017/updates_july2017.xls"

# read-in previous ridership
# change this to point to the present file that will be updated
old = pd.read_excel (previous_file,
                     sheet_name="export",
                     index_col=0, )

# old=pd.read_csv(r'S:/LibShare/Shared/Divisions/Graduate/GEODATA/MASS_Transit/mta_ridership/updates/2017/updates_may2017.csv',encoding = 'ISO-8859-1',index_col=0)## change this

# urls for MTA's Annual, Weekday and Weekend ridership data
url0 = "http://web.mta.info/nyct/facts/ridership/ridership_sub_annual.htm"
url1 = "http://web.mta.info/nyct/facts/ridership/ridership_sub.htm"
url2 = "http://web.mta.info/nyct/facts/ridership/ridership_sub_weekend.htm"

# will be used to name folder
year = input ("Type in the year: ")

# will be used to name csv of updates scraped from the web
month = input (
    "Type in first three letters of the month: "
)

stations_w_spelling_erros = {
    "Far Rockaway-MottAv": "Far Rockaway-Mott Av",
    "39 Av-Dutch kills": "39 Av-Dutch Kills",
    "39 Av- Dutch Kills": "39 Av-Dutch Kills",
    "Pennylvania Av": "Pennsylvania Av",
    "Parksde Av": "Parkside Av",
}


def clean_station_names(col):
    clean = (
        col.replace ("Â", "").replace (")", "").strip ()
    )  # ascii character that throws an error
    return clean


def order_train_column(col):
    return " ".join (sorted ([s.upper () for s in col.split (" ")]))


if not os.path.exists ("updates/{}".format (year)):
    os.makedirs ("updates/{}".format (year))


def scrape_rdiership_data(url):
    '''Scrape ridership data from MTA's website and format it into a neat DataFrame
    Params:
        url (str): the webiste url
    Retirns:
        df (DataFrame): pandas DataFrame with ridership data
    '''

    try:
        r = requests.get (url)
        # print(r.encoding)  # check encoding
        data = r.text
        soup = BeautifulSoup (data, "lxml")
        print ("Scarping ", soup.find ("h1").text.strip ())  # table title
        name_out = soup.find ("h1").text.strip () + ".csv"
        table = soup.find ("table", id="subway")
        rows = table.find_all ("tr", attrs={"class": None})
        th_tags = rows[0].findAll ("th")
        col_names = [c.strip () for c in [th.get_text () for th in th_tags]]
        col_names.insert (8, "% change")
        col_names.insert (1, "trains")
        table_rows = []
        for row in rows[1:]:  # get rows from header down
            cells = row.findAll ("td")
            # get actual text from cells and remove more than one space in between strings
            table_row = [" ".join (c.get_text ().strip ("\t\n\r").split ()) for c in cells]

            # in 2018 the table structure has sliglty changed
            # (thus check to start from first non-empty row after the header)
            if not any (table_row):
                continue
            else:
                trains = []
                subway_obj = row.find_all ("img")
                if subway_obj:
                    subways = [str (i) for i in subway_obj]
                    for i in subways:
                        train_match = re.search (r"(\w\s|\d\s)subway", i)
                        if train_match:
                            trains.append (train_match.group ().split (" subway")[0])
                        else:
                            train_match = re.search (r"(\w|\d).png", i)
                            if train_match:
                                trains.append (
                                    train_match.group ().split (".png")[0]
                                )  # in one of the links 2 train is empty in alt= pattern;

                    trainst_str = " ".join (trains)  # use image of the train service instead
                else:
                    trainst_str = "None"
                cells = row.findAll ("td")
                table_row = [
                    " ".join (c.get_text ().strip ("\t\n\r").split ()) for c in cells
                ]  # removes more than one space in between strings
                table_row.insert (1, trainst_str)
                table_rows.append (table_row)
        df = pd.DataFrame (data=table_rows, columns=col_names)

        df = df.replace ({",": ""}, regex=True)
        # the star character appears in the stops with temporary closures
        df = df.replace ({"\*": ""}, regex=True)
        # strip trailing spaces
        df.applymap (lambda x: x.strip () if isinstance (x, str) else x)
        df = df.apply (pd.to_numeric, errors="ignore")  # turn string-numbers to numeric

        drop_cols = [c for c in df.columns if "change" in c.lower () or "rank" in c.lower ()]
        df.drop (drop_cols, 1, inplace=True)  # delete change and rank columns

        df["Station (alphabetical by borough)"] = (
            df["Station (alphabetical by borough)"]
                .str.replace ("Â", "")
                .str.replace (")", "")
                .str.replace ("(", "")
                .str.strip ()
        )

        totals = [
            "Brooklyn",
            "Bronx",
            "Manhattan",
            "Queens",
            "Systemwide Adjustment",
            "System Total",
        ]
        mask = df["Station (alphabetical by borough)"].isin (totals)  ## boolean mask
        df = df[~mask]  # delete summary rows

        # rename numerical columns; Ex.2010 into tot2010 and avwkdy10
        # using regex, find columns with year in them
        year_cols = [c[0] for c in [re.findall (r"\d{4}", i) for i in df.columns] if c != []]
        if "Annual" in name_out:
            new_names = ["tot" + c for c in year_cols]
        elif "Weekday" in name_out:
            new_names = ["avwkdy" + c[2:] for c in year_cols]
        elif "Weekend" in name_out:
            new_names = ["avwken" + c[2:] for c in year_cols]

        new_names_d = {y: renamed for y, renamed in zip (year_cols, new_names)}
        df.rename (columns=new_names_d, inplace=True)
        df = df.replace (to_replace=[None, "None", ""], value=np.nan).dropna (how="all")

        # fix spelling mistakes for some stations
        df.replace (
            {"Station (alphabetical by borough)": stations_w_spelling_erros}, inplace=True
        )
        df["trains"] = df["trains"].apply (lambda x: order_train_column (x))

        # station names are not unique; make a unique column for joining them together
        df["join_id"] = df["Station (alphabetical by borough)"] + df["trains"]

        return df
    except Exception as e:
        logger.exception ("Unexpected exception occurred while scraping")
        raise


# function call in a loop
combined = []  # this list will hold all the tables scraped from the above links
for url in urls:
    df = scrape_rdiership_data (url)
    combined.append (df)

# merge all the tables in the combined list; drop repeating columns; rename to match the format
updates = reduce (
    lambda left, right: pd.merge (
        left, right, left_on="join_id", right_on="join_id", how="outer"
    ),
    combined,
)

updates["Station (alphabetical by borough)"] = updates[
    "Station (alphabetical by borough)"
].fillna (
    updates["Station (alphabetical by borough)_x"].fillna (
        updates["Station (alphabetical by borough)_y"]
    )
)

updates = updates.drop (
    [
        "Station (alphabetical by borough)_x",
        "Station (alphabetical by borough)_y",
        "join_id",
        "trains_x",
        "trains_y",
    ],
    1,
)

updates.rename (
    columns={"Station (alphabetical by borough)": "complex_nm"}, inplace=True
)
updates.to_csv (f"updates/{year}/combined_ridership{year}.csv", index=False)

# create a subset of non-overlapping columns; 
# create unique id to do table join and join with the scraped updates

cols_overlap = [
    c for c in updates.columns if c in old.columns
]  # find overlapiing columns

start_of_overlap = "tot" + min (
    [c[0] for c in [re.findall (r"\d{4}", i) for i in cols_overlap] if c != []]
)  # common columns to both tables

# non-overlapping columns from old table to keep
keep_cols = [
    c for c in old.columns if c not in cols_overlap or c in ("trains", "complex_nm")
]
keep_cols.extend ([start_of_overlap])
keep_old = old[
    keep_cols
].copy ()  # create a subset with non-overlapping columns + one tot{year}

keep_old.reset_index (inplace=True)
keep_old["unique_id"] = (
    keep_old["complex_nm"] + "_" + keep_old[start_of_overlap].astype (str)
)
keep_old.drop (start_of_overlap, 1, inplace=True)
keep_old.rename (columns={"complex_nm": "complex_nm_old"}, inplace=True)

updates["unique_id"] = (
    updates["complex_nm"] + "_" + updates[start_of_overlap].astype (str)
)
updates.rename (columns={"trains": "trains_old"}, inplace=True)
updated = keep_old.merge (updates, how="outer", on="unique_id")

# arrange columns in the desired order and write out the result in updates folder
sorted_cols = updated.columns.sort_values ()
tot_cols = [c for c in sorted_cols if "tot" in c]
wkd_cols = [c for c in sorted_cols if "avwkdy" in c]
wken_cols = [c for c in sorted_cols if "avwken" in c]
starting_cols = [
    "complex_id",
    "complex_nm_old",
    "complex_nm",
    "trains_old",
    "trains",
    "station_ct",
    "bcode",
    "stop_lat",
    "stop_lon",
]
final_col_order = starting_cols + tot_cols + wkd_cols + wken_cols
final_col_order.extend (["srv_notes", "unique_id"])
df_out = updated[final_col_order]
df_out.to_csv ("updates/{}/updates_{}{}.csv".format (year, month, year), encoding="utf-8")

print ("All done")
