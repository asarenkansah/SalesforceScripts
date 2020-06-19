import pandas as pd
from pathlib import *
from datetime import date


def data_reorder(final_data):
    final_data = final_data.reindex(columns=['PURCHASE_ID__C', 'SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'CONCATID__C', 'EMAIL__C', 'BIRTHDATE__C', 'GENDER__C', 'ADDRESS_LINE_1__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C','COUNTRY__C', 'MOBILE__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'MAJOR_OF_INTEREST__C', 'SECONDARY_MAJOR_OF_INTEREST__C','Race/Ethnicity','AMERICAN_INDIAN_ALASKAN_NATIVE__C', 'ASIAN__C', 'BLACK_AFRICAN_AMERICAN__C', 'WHITE_CAUCASIAN__C', 'Native Hawaiian/Pacific Islander', 'Hispanic/Latino', 'Race/Ethnicity Unknown'])
    return final_data

def data_rename(final_data):
    final_data = final_data.rename(columns={'Academic Interest Name 2':'SECONDARY_MAJOR_OF_INTEREST__C', 'Academic Interest Name 1':'MAJOR_OF_INTEREST__C','Gender':'GENDER__C', 'DOB':'BIRTHDATE__C', 'Raise.me Code' : 'PURCHASE_ID__C', 'First Name' : 'FIRST_NAME__C', 'Last Name' : 'LAST_NAME__C', 'Email' : 'EMAIL__C', 'Concat ID' : 'CONCATID__C' , 'Street Address' : 'ADDRESS_LINE_1__C' , 'City' : 'CITY__C', 'Subdivision' : 'STATE__C', 'Postal Code' : 'ZIP_CODE__C', 'Mobile': 'MOBILE__C', 'Projected Graduation Year': 'HS_GRADUATION_YEAR__C', 'CEEB Code': 'HS_CEEB_CODE__C', 'Country' : 'COUNTRY__C', })
    return final_data

def data_clean(load_data):
    load_data = load_data[['Raise.me Code'] + ['First Name'] + ['Last Name'] + ['Email'] + ['Mobile'] + ['Projected Graduation Year'] + ['DOB'] + ['Academic Interest Name 1'] + ['Academic Interest Name 2'] + ['Street Address'] + ['City'] + ['Subdivision'] + ['Postal Code']+ ['Country'] + ['Gender']  + ['CEEB Code'] + ['Race/Ethnicity']]
    load_data['Concat ID'] = load_data['First Name'] + load_data['Last Name'] + load_data['Street Address'].str[:10]
    load_data.loc[load_data["Concat ID"].isnull(),'Concat ID'] = load_data["First Name"] + load_data["Last Name"]
    load_data['Concat ID'] = load_data['Concat ID'].str.lower()
    load_data['First Name'] = load_data['First Name'].str.title()
    load_data['Last Name'] = load_data['Last Name'].str.title()
    load_data['Street Address'] = load_data['Street Address'].str.title()
    load_data['City'] = load_data['City'].str.title()
    load_data['AMERICAN_INDIAN_ALASKAN_NATIVE__C'] = ""
    load_data['ASIAN__C']= ""
    load_data['BLACK_AFRICAN_AMERICAN__C']= ""
    load_data['WHITE_CAUCASIAN__C']= ""
    load_data['Native Hawaiian/Pacific Islander']= ""
    load_data['Hispanic/Latino']= ""
    load_data['Race/Ethnicity Unknown']= ""
    load_data['Source'] = ""
    load_data['LOAD_DATE__C'] = ""
    load_data['YEAR__C'] = ""
    load_data['TERM__C'] = ""
    load_data['STUDENT_STATUS__C'] = ""
    load_data['STUDENT_TYPE__C'] = ""
    load_data['Gender'] = load_data['Gender'].map({'M' : 'Male', 'F': 'Female'})

    for index, row in load_data.iterrows():
        load_data.at[index,"SOURCE__C"] = "RaiseMe"
        load_data.at[index,"TERM__C"] = "Fall"
        load_data.at[index,"STUDENT_STATUS__C"] = "Inquiry"
        load_data.at[index,"STUDENT_TYPE__C"] = "Freshman"

    return load_data

def imports():

    file = Path("200616_RaiseMe_original.csv")
    if file.exists ():
        data = pd.read_csv("200616_RaiseMe_original.csv")
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
    RaiseMe_data = data_rename(RaiseMe_data)
    RaiseMe_data = data_reorder(RaiseMe_data)
    RaiseMe_data.to_csv('RaiseMe_upgrade.csv', index=False)

main()
