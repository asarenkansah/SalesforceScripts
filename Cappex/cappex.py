import pandas as pd
from pathlib import *
from datetime import date

def data_reorder(final_data):
    print(final_data.head())
    final_data = final_data.reindex(columns=['SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'EMAIL__C', 'BIRTHDATE__C', 'GENDER__C', 'CONCATID__C', 'ADDRESS_LINE_1__C', 'ADDRESS_LINE_2__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C', 'COUNTRY__C', 'MOBILE__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'ACT/SAT Max Cumulative', 'MAJOR_OF_INTEREST__C', 'SECONDARY_MAJOR_OF_INTEREST__C', 'AMERICAN_INDIAN_ALASKAN_NATIVE__C', 'ASIAN__C', 'BLACK_AFRICAN_AMERICAN__C', 'WHITE_CAUCASIAN__C', 'Hispanic/Latino', 'Race/Ethnicity Unknown'])
    return final_data

def data_rename(final_data):
    final_data = final_data.rename(columns={'Inquiry Product' : 'SOURCE__C', 'First Name': 'FIRST_NAME__C', 'Last Name':'LAST_NAME__C', 'Concat ID':'CONCATID__C', 'Email Address':'EMAIL__C', 'Birth Date':'BIRTHDATE__C', 'Gender':'GENDER__C', 'Address1':'ADDRESS_LINE_1__C', 'Address2':'ADDRESS_LINE_2__C', 'City':'CITY__C', 'State':'STATE__C', 'Zip Code':'ZIP_CODE__C', 'Country':'COUNTRY__C', 'Primary Phone':'MOBILE__C', 'Expected HS Graduation Date':'HS_GRADUATION_YEAR__C', 'ACT Composite':'ACT/SAT Max Cumulative', 'CEEB Code': 'HS_CEEB_CODE__C', 'Major 1':'MAJOR_OF_INTEREST__C', 'Major 2':'SECONDARY_MAJOR_OF_INTEREST__C'})
    return final_data

def data_clean(load_data):
    load_data = load_data[['Inquiry Product'] + ['First Name'] + ['Last Name'] + ['Concat ID'] + ['Email Address'] + ['Birth Date'] + ['Gender'] + ['Address1'] + ['Address2'] + ['City'] + ['State'] + ['Zip Code'] + ['Country'] + ['Primary Phone'] + ['Expected HS Graduation Date'] + ['ACT Composite'] + ['CEEB Code'] + ['Major 1'] + ['Major 2'] + ['Ethnicity - Fixed List']]
    load_data['Concat ID'] = load_data['First Name'] + load_data['Last Name'] + load_data['Address1'].str[:10]
    load_data['Concat ID'] = load_data['Concat ID'].str.lower()
    load_data['First Name'] = load_data['First Name'].str.title()
    load_data['Last Name'] = load_data['Last Name'].str.title()
    load_data['Address1'] = load_data['Address1'].str.title()
    load_data['City'] = load_data['City'].str.title()
    load_data['Expected HS Graduation Date'] = load_data['Expected HS Graduation Date'].str[-4:]
    load_data['AMERICAN_INDIAN_ALASKAN_NATIVE__C'] = ""
    load_data['ASIAN__C']= ""
    load_data['BLACK_AFRICAN_AMERICAN__C']= ""
    load_data['WHITE_CAUCASIAN__C']= ""
    load_data['Native Hawaiian/Pacific Islander']= ""
    load_data['Hispanic/Latino']= ""
    load_data['Race/Ethnicity Unknown']= ""
    load_data['LOAD_DATE__C'] = ""
    load_data['YEAR__C'] = ""
    load_data['TERM__C'] = ""
    load_data['STUDENT_STATUS__C'] = ""
    load_data['STUDENT_TYPE__C'] = ""
    load_data['Gender'] = load_data['Gender'].map({'M' : 'Male', 'F': 'Female'})
    load_data['Country'] = load_data['Country'].map({'United States': 'US'})

    for index, row in load_data.iterrows():
        if(row['Inquiry Product'] == 'Greenlight'):
            load_data.at[index,"Inquiry Product"] = "Cappex Greenlight"
        load_data.at[index,"TERM__C"] = "Fall"
        load_data.at[index,"STUDENT_STATUS__C"] = "Inquiry"
        load_data.at[index,"STUDENT_TYPE__C"] = "Freshman"

    return load_data

def imports():
    file = Path("200515_Cappex_original.csv")
    if file.exists ():
        data = pd.read_csv("200515_Cappex_original.csv", encoding = "ISO-8859-1")
    else:
        print("Cappex file not found")

    major_file = Path("major_decoder.xlsx")
    if major_file.exists():
        major_data = pd.read_excel("major_decoder.xlsx")
        major_data = major_data[['Major / Program'] + ['UK Major']]
    else:
        print("Major Decoder file not found")

    return data, major_file

def main():
    new_import, major_data = imports()
    cappex_data = data_clean(new_import)
    cappex_data = data_rename(cappex_data)
    cappex_data = data_reorder(cappex_data)
    cappex_data.to_csv('Cappex_upgrade.csv', index=False)


main()
