# -*- coding: utf-8-*-

'''This script scrapes the ridership data from MTA's website and
combines it with the previos ridership data stored at GIS lab.
Before running, change the path to the data on line 100 (if reading excel format) 
or 101 (if reading csv fomrat)'''

import pandas as pd
from bs4 import BeautifulSoup
import requests, re, os
from functools import reduce

url0 = 'http://web.mta.info/nyct/facts/ridership/ridership_sub_annual.htm'
url1 = 'http://web.mta.info/nyct/facts/ridership/ridership_sub.htm/robots.txt'
url2 = 'http://web.mta.info/nyct/facts/ridership/ridership_sub_weekend.htm/robots.txt'
combined = []  # this list will hold all the tables scraped from the above links
year = input('Type in the year: ')  # will be used to name folder
month = input('Type in first three letters of the month: ')  # will be used to name csv of updates scraped from the web


def remove_comma(col):
    no_comma = col.replace(',', '')
    return no_comma


def clean_station_names(col):
    clean = col.replace(u'Â', '').replace(')', '').strip()  # ascii character that throws an error
    return clean


if not os.path.exists('updates/{}'.format(year)):
    os.makedirs('updates/{}'.format(year))

for url in [url0, url1, url2]:
    r = requests.get(url)
    print(r.encoding)  # check encoding
    data = r.text
    soup = BeautifulSoup(data, 'lxml')
    print(soup.find('h1').text.strip())  # table title
    name_out = soup.find('h1').text.strip() + '.csv'
    table = soup.find('table', id="subway")
    rows = table.find_all('tr', attrs={'class': None})
    th_tags = rows[0].findAll('th')
    col_names = [c.strip() for c in [th.get_text() for th in th_tags]]
    col_names.insert(8, '% change')
    col_names.insert(1, 'trains')
    table_rows = []
    for row in rows[1:]:# get rows from header down
        cells = row.findAll('td')
        table_row = [" ".join(c.get_text().strip('\t\n\r').split()) for c in
                     cells]  # removes more than one space in between strings
        if table_row is False:
            pass # in 2018 the table structure has sliglty changed (thus check to start from first non-empty row after the header)
        
        else:
            trains = []
            subway_obj = row.find_all('img')
            if subway_obj:
                subways = [str(i) for i in subway_obj]
                for i in subways:
                    train_match = re.search(r'(\w\s|\d\s)subway', i)
                    if train_match:
                        train = train_match.group().split(' subway')[0]
                        trains.append(train)
                    else:
                        train_match = re.search(r'(\w|\d).png', i)
                        if train_match:
                            trains.append(train_match.group().split('.png')[
                                              0])  # in one of the links 2 train is empty in alt= pattern;
                trainst_str = ' '.join(trains)  # use image of the train service instead
            else:
                trainst_str = 'None'    
            cells = row.findAll('td')
            table_row = [" ".join(c.get_text().strip('\t\n\r').split()) for c in
                         cells]  # removes more than one space in between strings
            table_row.insert(1, trainst_str)
            table_rows.append(table_row)
    df = pd.DataFrame(data=table_rows, columns=col_names)

    for col in df.columns:
        df[col] = df[col].apply(lambda x: remove_comma(x))

    df = df.apply(pd.to_numeric, errors='ignore')  # turn string-numbers to numeric

    drop_cols = [c for c in df.columns if 'change' in c.lower() or 'rank' in c.lower()]
    df.drop(drop_cols, 1, inplace=True)  # delete change and rank columns

    df[df.columns[0]] = df[df.columns[0]].apply(lambda x: clean_station_names(x))

    totals = ['Brooklyn', 'Bronx', 'Manhattan', 'Queens', 'Systemwide Adjustment', 'System Total']
    mask = df['Station (alphabetical by borough)'].isin(totals)  ## bolean mask
    df = df[~mask]  # delete summary rows

    # rename numerical columns; Ex.2010 into tot2010 and avwkdy10
    # using regex, find columns with year in them
    year_cols = [c[0] for c in [re.findall(r'\d{4}', i) for i in df.columns] if c != []]
    if 'Annual' in name_out:
        new_names = ['tot' + c for c in year_cols]
    elif 'Weekday' in name_out:
        new_names = ['avwkdy' + c[2:] for c in year_cols]
    elif 'Weekend' in name_out:
        new_names = ['avwken' + c[2:] for c in year_cols]

    new_names_d = {y: renamed for y, renamed in zip(year_cols, new_names)}
    df.rename(columns=new_names_d, inplace=True)
    combined.append(df)

# merge all the tables in the combined list; drop repeating columns;rename to match the format
updates = reduce(lambda left, right: pd.merge(left, right, left_on='Station (alphabetical by borough)', 
                                              right_on='Station (alphabetical by borough)'), combined)

updates = updates.drop(
    ['trains', 'trains_y'], 1)
updates.rename(columns={'Station (alphabetical by borough)': 'complex_nm', 'trains_x': 'trains'}, inplace=True)
updates.to_csv('updates/{}/combined_ridership{}.csv'.format(year, year))

# read-in old ridership,create a subset of non-overlapping columns; create unique id to do table join and join with the scraped updates
old = pd.read_excel('/Users/anastasiaclark/MyStaff/Git_Work/MTA-Ridership/updates/2017/updates_july2017.xls',
                    sheet_name='export', index_col=0)  # change this
# old=pd.read_csv(r'S:/LibShare/Shared/Divisions/Graduate/GEODATA/MASS_Transit/mta_ridership/updates/2017/updates_may2017.csv',encoding = 'ISO-8859-1',index_col=0)## change this
cols_overlap = [c for c in updates.columns if c in old.columns]  ## find overlapiing columns
start_of_overlap = 'tot' + min(
    [c[0] for c in [re.findall(r'\d{4}', i) for i in cols_overlap] if c != []])  # common columns to both tables
keep_cols = [c for c in old.columns if c not in cols_overlap]  # non-overlapping columns from old table to keep
keep_cols.extend(['complex_nm', 'trains', start_of_overlap])
keep_old = old[keep_cols].copy()  # create a subset with non-overlapping columns + one tot{year}
keep_old.reset_index(inplace=True)
keep_old['unique_id'] = keep_old['complex_nm'] + '_' + keep_old[start_of_overlap].astype(str)
keep_old.drop(start_of_overlap, 1, inplace=True)
keep_old.rename(columns={'complex_nm': 'complex_nm_old'}, inplace=True)

updates['unique_id'] = updates['complex_nm'] + '_' + updates[start_of_overlap].astype(str)
updates.rename(columns={'trains': 'trains_old'}, inplace=True)
updated = keep_old.merge(updates, how='outer', on='unique_id')

# arrange columns in the desired order and write out the result in updates folder
sorted_cols = updated.columns.sort_values()
tot_cols = [c for c in sorted_cols if 'tot' in c]
wkd_cols = [c for c in sorted_cols if 'avwkdy' in c]
wken_cols = [c for c in sorted_cols if 'avwken' in c]
starting_cols = ['complex_id', 'complex_nm_old', 'complex_nm', 'trains_old', 'trains', 'station_ct', 'bcode',
                 'stop_lat', 'stop_lon']
final_col_order = starting_cols + tot_cols + wkd_cols + wken_cols
final_col_order.extend(['srv_notes', 'unique_id'])
df_out = updated[final_col_order]
df_out.to_csv('updates/{}/updates_{}{}.csv'.format(year, month, year), encoding='utf-8')

print('All done')
