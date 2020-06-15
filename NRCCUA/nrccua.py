import pandas as pd
from pathlib import *
from datetime import date

def data_reorder(final_data):
    final_data = final_data.reindex(columns=['Purchase ID', 'SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'EMAIL__C', 'BIRTHDATE__C', 'GENDER__C', 'CONCATID__C', 'ADDRESS_LINE_1__C', 'ADDRESS_LINE_2__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C', 'COUNTRY__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'MAJOR_OF_INTEREST__C', 'SECONDARY_MAJOR_OF_INTEREST__C', 'AMERICAN_INDIAN_ALASKAN_NATIVE__C', 'ASIAN__C', 'BLACK_AFRICAN_AMERICAN__C', 'WHITE_CAUCASIAN__C', 'Hispanic/Latino', 'Race/Ethnicity Unknown'])
    return final_data

def data_rename(final_data):
    final_data = final_data.rename(columns={'Sequence':'Purchase ID', 'FirstName':'FIRST_NAME__C', 'LastName':'LAST_NAME__C', 'Email':'EMAIL__C', 'BirthDate':'BIRTHDATE__C', 'Gender':'GENDER__C', 'Contact ID':'CONCATID__C', 'Address':'ADDRESS_LINE_1__C', 'Address2':'ADDRESS_LINE_2__C', 'City':'CITY__C','State':'STATE__C','Zipcode':'ZIP_CODE__C','GraduationYear':'HS_GRADUATION_YEAR__C','Ceeb':'HS_CEEB_CODE__C','Major01':'MAJOR_OF_INTEREST__C','Major02':'SECONDARY_MAJOR_OF_INTEREST__C'})
    return final_data

def data_clean(load_data):
    load_data = load_data[['Sequence']+['FirstName']+['LastName']+['Address']+['Address2']+['City']+['State']+['Zipcode']+['Email']+['Phone']+['CellPhone']+['Ceeb']+['BirthDate']+['Gender']+['GraduationYear']+['Major01']+['Major02']+['Race01']+['Race02']+['Race03']+['Race04']]
    load_data['Concat ID'] = load_data['FirstName'] + load_data['LastName'] + load_data['Address'].str[:10]
    load_data['Concat ID'] = load_data['Concat ID'].str.lower()
    load_data['FirstName'] = load_data['FirstName'].str.title()
    load_data['LastName'] = load_data['LastName'].str.title()
    load_data['Address'] = load_data['Address'].str.title()
    load_data['Address2'] = load_data['Address2'].str.title()
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
    load_data['MOBILE__C'] = ""
    load_data['COUNTRY__C'] = ""
    load_data['Gender'] = load_data['Gender'].map({'M' : 'Male', 'F': 'Female'})

    for index, row in load_data.iterrows():
        if(row['Concat ID'] == ""):
            load_data.at[index,"Inquiry Product"] = row['First Name'] + row['Last Name']
        if(row['CellPhone'] == ""):
            load_data.at[index,'MOBILE__C'] = row['CellPhone']
        else:
            load_data.at['MOBILE__C'] = row['Phone']
        load_data.at[index,"SOURCE__C"] = "NRCCUA DSC"
        load_data.at[index,"TERM__C"] = "Fall"
        load_data.at[index,"STUDENT_STATUS__C"] = "Inquiry"
        load_data.at[index,"STUDENT_TYPE__C"] = "Freshman"
        load_data.at[index, 'COUNTRY__C'] = "US"

    return load_data

def imports():
    file = Path("200515_NRCCUA_original.csv")
    if file.exists ():
        data = pd.read_csv("200515_NRCCUA_original.csv", encoding = "ISO-8859-1")
    else:
        print("NRCCUA file not found")

    major_file = Path("major_decoder.xlsx")
    if major_file.exists():
        major_data = pd.read_excel("major_decoder.xlsx")
        major_data = major_data[['Major / Program'] + ['UK Major']]
    else:
        print("Major Decoder file not found")

    return data, major_file

def main():
    new_import, major_data = imports()
    nrccua_data = data_clean(new_import)
    nrccua_data = data_rename(nrccua_data)
    nrccua_data = data_reorder(nrccua_data)
    nrccua_data.to_csv('NRCCUA_upgrade.csv', index=False)


main()
