import pandas as pd
from pathlib import *

def data_clean(load_data):
    load_data = load_data[['Raise.me Code'] + ['First Name'] + ['Last Name'] + ['Email'] + ['Mobile'] + ['Projected Graduation Year'] + ['DOB'] + ['Academic Interest Name 1'] + ['Academic Interest Name 2'] + ['Street Address'] + ['City'] + ['Subdivision'] + ['Postal Code']+ ['Country'] + ['Gender']  + ['CEEB Code'] + ['Race/Ethnicity'] + ['ACT Score']]
    load_data['Concat ID'] = load_data['First Name'] + load_data['Last Name'] + load_data['Street Address'].str[:10]
    load_data['Concat ID'] = load_data['Concat ID'].str.lower()
    load_data['First Name'] = load_data['First Name'].str.title()
    load_data['Last Name'] = load_data['Last Name'].str.title()
    load_data['Street Address'] = load_data['Street Address'].str.title()
    load_data['City'] = load_data['City'].str.title()
    print(load_data.head())

def imports():
    file = Path("200519_RaiseMe_original.xlsx")
    if file.exists ():
        data = pd.read_excel("200519_RaiseMe_original.xlsx")
    else:
        print("RaiseMe file not found")

    major_file = Path("major_decoder.xlsx")
    if major_file.exists():
        major_data = pd.read_excel("major_decoder.xlsx")
        major_data = major_data[['Major / Program'] + ['UK Major']]
    else:
        print("Major Decoder file not found")

    return data, major_file

def main():
    new_import, major_data = imports()
    RaiseMe_data = data_clean(new_import)
    RaiseMe_data = data_compare(YouVisit_data, major_data)


main()
