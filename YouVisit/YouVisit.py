import pandas as pd
from pathlib import *

def data_reorder(final_data):
    cols = list(final_data.columns.values)
    final_data = final_data.reindex(columns=['SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'CONCATID__C', 'EMAIL__C', 'BIRTHDATE__C', 'ADDRESS_LINE_1__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C', 'MOBILE__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'MAJOR_OF_INTEREST__C', 'COUNTRY__C'])
    return final_data

def data_rename(final_data):
    final_data = final_data.rename(columns={'First Name' : 'FIRST_NAME__C', 'Last Name' : 'LAST_NAME__C' , 'Email' : 'EMAIL__C', 'Date of Birth': 'BIRTHDATE__C' , 'Concat ID' : 'CONCATID__C', 'Street' : 'ADDRESS_LINE_1__C', 'City':'CITY__C', 'State / Region':'STATE__C', 'Postal Code':'ZIP_CODE__C', 'Phone':'MOBILE__C', 'Enrollment Year':'HS_GRADUATION_YEAR__C', 'Ceeb Code':'HS_CEEB_CODE__C', 'UK Major':'MAJOR_OF_INTEREST__C', 'Country':'COUNTRY__C', 'Visitor Type':'STUDENT_TYPE__C'})
    final_data = final_data.drop(columns = ['Major / Program'])
    final_data["SOURCE__C"] = ""
    final_data["LOAD_DATE__C"] = ""
    final_data["YEAR__C"] = ""
    final_data["TERM__C"] = ""
    final_data["STUDENT_STATUS__C"] = ""

    return final_data

def data_compare(load_data, major_data):
    major_data['Major / Program'] = major_data['Major / Program'].str.lower()
    major_data = major_data.groupby(['Major / Program']).first()
    load_data = pd.merge(load_data, major_data, on = 'Major / Program', how = 'left')
    return load_data

def data_clean(load_data):
    load_data = load_data[['Visitor Type'] + ['First Name'] + ['Last Name'] + ['Email'] + ['Enrollment Year'] + ['Phone'] + ['Date of Birth'] + ['Major / Program']  + ['Street'] + ['City'] + ['State / Region'] + ['Postal Code']+ ['Country']  + ['Ceeb Code']]
    load_data['Concat ID'] = load_data['First Name'] + load_data['Last Name'] + load_data['Street'].str[:10]
    load_data['Concat ID'] = load_data['Concat ID'].str.lower()
    load_data['First Name'] = load_data['First Name'].str.title()
    load_data['Last Name'] = load_data['Last Name'].str.title()
    load_data['Street'] = load_data['Street'].str.title()
    load_data['City'] = load_data['City'].str.title()
    load_data['Country'] = load_data['Country'].map({'United States': 'US'})
    load_data['Major / Program'] = load_data['Major / Program'].str.strip()
    load_data['Major / Program'] = load_data['Major / Program'].str.lower()

    for index, row in load_data.iterrows():
        if(row['Visitor Type'] == "Alumni" or row['Visitor Type'] == "Parent of High School Student" or row['Visitor Type'] == "College Graduate" or row['Visitor Type'] == "Faculty / Staff" or row['Visitor Type'] == "Faculty / Staff"  or row['Visitor Type'] == 'Parent of Transfer Student' or row['Visitor Type'] == 'Parent of High School Graduate' or row['Visitor Type'] == 'School Counselor' or row['Visitor Type'] == 'Parent of College Graduate'):
            load_data = load_data.drop([index])
        if(load_data['Contact ID'] == ''):
            load_data.at[index,"Contact ID"] = load_data['First Name'] + load_data['Last Name']

    return load_data

def imports():
    file = Path("200608_YouVisit_original.csv")
    if file.exists ():
        data = pd.read_csv("200608_YouVisit_original.csv", encoding = "ISO-8859-1")
    else:
        print("YouVisit file not found")

    major_file = Path("major_decoder.xlsx")
    if major_file.exists():
        major_data = pd.read_excel("major_decoder.xlsx")
        major_data = major_data[['Major / Program'] + ['UK Major']]
    else:
        print("Major Decoder file not found")

    return data, major_data

def main():
    new_import = imports()
    YouVisit_data, major_data = new_import[0], new_import[1]
    YouVisit_data = data_clean(YouVisit_data)
    YouVisit_data = data_compare(YouVisit_data, major_data)
    YouVisit_data = data_rename(YouVisit_data)
    YouVisit_data = data_reorder(YouVisit_data)
    YouVisit_data.to_csv('YouVisit_upgrade.csv', index=False)

main()

# SOURCE__C	LOAD_DATE__C	FIRST_NAME__C	LAST_NAME__C	EMAIL__C	BIRTHDATE__C	GENDER__C	CONCATID__C	ADDRESS_LINE_1__C	ADDRESS_LINE_2__C	CITY__C	STATE__C	ZIP_CODE__C	MOBILE__C HS_GRADUATION_YEAR__C	HS_CEEB_CODE__C	YEAR__C	TERM__C	STUDENT_STATUS__C	STUDENT_TYPE__C	MAJOR_OF_INTEREST__C COUNTRY__C
