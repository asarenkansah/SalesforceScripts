import pandas as pd
from pathlib import *
from datetime import date

def data_reorder(final_data):
    final_data = final_data.reindex(columns=['SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'CONCATID__C', 'EMAIL__C', 'ADDRESS_LINE_1__C', 'ADDRESS_LINE_2__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C', 'MOBILE__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'Academic Interests', 'COUNTRY__C'])
    return final_data

def data_rename(final_data):
    final_data = final_data.rename(columns={'Visitor Id' : 'Purchase ID', 'First Name' : 'FIRST_NAME__C', 'Last Name' : 'LAST_NAME__C', 'Email' : 'EMAIL__C', 'Concat ID' : 'CONCATID__C' , 'Street Address' : 'ADDRESS_LINE_1__C' , 'Street Address 2' : 'ADDRESS_LINE_2__C', 'City' : 'CITY__C', 'State' : 'STATE__C', 'Zipcode' : 'ZIP_CODE__C', 'Phone': 'MOBILE__C', 'Enrollment Year': 'HS_GRADUATION_YEAR__C', 'High School CEEB Code': 'HS_CEEB_CODE__C', 'Country' : 'COUNTRY__C', 'Title' : 'STUDENT_TYPE__C'})

    return final_data

def data_clean(load_data):
    load_data = load_data[['Visitor Id']+['First Name']+['Last Name'] +['Email'] + ['Phone'] + ['Street Address'] +['Street Address 2'] + ['City'] + ['State'] + ['Zipcode'] + ['Country'] + ['Enrollment Year'] + ['Academic Interests'] + ['High School CEEB Code'] + ['Title']]
    load_data['Concat ID'] = load_data['First Name'] + load_data['Last Name'] + load_data['Street Address'].str[:10]
    load_data['Concat ID'] = load_data['Concat ID'].str.lower()
    load_data['First Name'] = load_data['First Name'].str.title()
    load_data['Last Name'] = load_data['Last Name'].str.title()
    load_data['Street Address'] = load_data['Street Address'].str.title()
    load_data['Street Address 2'] = load_data['Street Address 2'].str.title()
    load_data['City'] = load_data['City'].str.title()
    load_data['Country'] = load_data['Country'].map({'United States': 'US'})
    load_data["SOURCE__C"] = ""
    load_data["LOAD_DATE__C"] = ""
    load_data["YEAR__C"] = ""
    load_data["TERM__C"] = ""
    load_data["STUDENT_STATUS__C"] = ""
    today = date.today()
    full_date = str(today.strftime("%d/%m/%Y"))

    for index, row in load_data.iterrows():
        if(row['Visitor Id'] == "Alumni" or row['Visitor Id'] == "Parent of High School Student" or row['Visitor Id'] == "College Graduate" or row['Visitor Id'] == "Faculty / Staff" or row['Visitor Id'] == "Faculty / Staff" ):
            load_data = load_data.drop([index])
        load_data.at[index,"SOURCE__C"] = "VisitDays"
        load_data.at[index,"TERM__C"] = "Fall"
        load_data.at[index,"STUDENT_STATUS__C"] = "Inquiry"
        load_data.at[index, "LOAD_DATE__C"] = full_date

    return load_data

def imports():
    file = Path("200611_VisitDays_original.csv")
    if file.exists ():
        data = pd.read_csv("200611_VisitDays_original.csv", encoding = "ISO-8859-1")
    else:
        print("VisitDays file not found")

    major_file = Path("major_decoder.xlsx")
    if major_file.exists():
        major_data = pd.read_excel("major_decoder.xlsx")
        major_data = major_data[['Major / Program'] + ['UK Major']]
    else:
        print("Major Decoder file not found")

    return data, major_data

def main():
    new_import = imports()
    VisitDays_data, major_data = new_import[0], new_import[1]
    VisitDays_data = data_clean(VisitDays_data)
    VisitDays_data = data_rename(VisitDays_data)
    VisitDays_data = data_reorder(VisitDays_data)
    VisitDays_data.to_csv('VisitDays_upgrade.csv', index=False)

main()
