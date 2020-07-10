import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from pathlib import *
import datetime as dt

def data_dedup(load_data, dedup_data, SAP_data):
    dedup_concat =  dedup_data[['Concat ID'] + ['Contact ID']]
    dedup_email = dedup_data[['Email'] + ['Contact ID']]

    load_data = pd.merge(load_data, dedup_concat, on = 'Concat ID', how = 'left')
    load_data = pd.merge(load_data, dedup_email, on = 'Email', how = 'left')
    load_data.loc[load_data['Contact ID_x'].isnull(),'Contact ID_x'] = load_data['Contact ID_y']

    load_data.drop_duplicates(subset = 'Visitor Id', keep=False,inplace=True)
    load_data.drop_duplicates(subset = 'Email', keep=False,inplace=True)
    load_data.drop_duplicates(subset = 'Concat ID', keep=False,inplace=True)

    SAP_concat = SAP_data[['Concat ID'] + ['STUDENTSHORT']]
    SAP_email1 = SAP_data[['SMTP_ADDR'] + ['STUDENTSHORT']]
    SAP_email2 = SAP_data[['SMTP_ADDR1'] + ['STUDENTSHORT']]

    SAP_email1 = SAP_email1.rename(columns={'SMTP_ADDR': 'Email', 'STUDENTSHORT': 'Student Number 1'})
    SAP_email2 = SAP_data.rename(columns={'SMTP_ADDR1': 'Email', 'STUDENTSHORT': 'Student Number 2'})

    load_data = pd.merge(load_data, SAP_concat, on = 'Concat ID', how = 'left')
    load_data = pd.merge(load_data, SAP_email1, on = 'Email', how = 'left')
    load_data = pd.merge(load_data, SAP_email2, on = 'Email', how = 'left')
    load_data.loc[load_data['STUDENTSHORT'].isnull(),'STUDENTSHORT'] = load_data['Student Number 1']
    load_data.loc[load_data['STUDENTSHORT'].isnull(),'STUDENTSHORT'] = load_data['Student Number 2']

    return load_data

#Changes the order of the columns, not required/necessary but it makes it easier for me to read
def data_reorder(load_data):
    load_data = load_data.reindex(columns=['Purchase ID', 'SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'CONCATID__C', 'EMAIL__C', 'ADDRESS_LINE_1__C', 'ADDRESS_LINE_2__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C', 'MOBILE__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'Academic Interests', 'MAJOR_OF_INTEREST__C', 'SECONDARY_MAJOR_OF_INTEREST__C', 'COUNTRY__C', 'Contact ID', 'STUDENTSHORT'])
    return load_data

#Renames all of the columns to their proper name that will be mapped in SF CRM
def data_rename(load_data):
    load_data = load_data.rename(columns={'Visitor Id' : 'Purchase ID', 'First Name' : 'FIRST_NAME__C', 'Last Name' : 'LAST_NAME__C', 'Email' : 'EMAIL__C', 'Concat ID' : 'CONCATID__C' , 'Street Address' : 'ADDRESS_LINE_1__C' , 'Street Address 2' : 'ADDRESS_LINE_2__C', 'City' : 'CITY__C', 'State' : 'STATE__C', 'Zipcode' : 'ZIP_CODE__C', 'Phone': 'MOBILE__C', 'Enrollment Year': 'HS_GRADUATION_YEAR__C', 'High School CEEB Code': 'HS_CEEB_CODE__C', 'Country' : 'COUNTRY__C', 'Title' : 'STUDENT_TYPE__C', 'Contact ID_x': 'Contact ID'})
    return load_data

#Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
def major_compare(load_data, major_data):

    #Delimits the original files' current column for holding majors by the semicolon
    major_holder = load_data['Academic Interests'].str.split(pat =";", expand = True)
    major_holder.columns = major_holder.columns.astype(str)
    selected_column_0 = major_holder[["0"]]
    selected_column_1 = major_holder[["1"]]
    load_data['Academic Interests 1'] = selected_column_0.copy()
    load_data['Academic Interests 2'] = selected_column_1.copy()

    #Makes sure that none of the capitilization impeeds the progress of matching for the majors since it's case sensitive
    major_data['Major / Program'] = major_data['Major / Program'].str.lower()
    load_data['Academic Interests 1'] = load_data['Academic Interests 1'].str.lower()
    load_data['Academic Interests 2'] = load_data['Academic Interests 2'].str.lower()
    load_data['Academic Interests 2'] = load_data['Academic Interests 2'].str.strip()

    #Prepares the major data for the merge and then does the vlookup function with the "merge" method
    major1_data =  major_data.rename(columns={'Major / Program' : 'Academic Interests 1', 'UK Major' : 'MAJOR_OF_INTEREST__C'})
    major1_data = major1_data.dropna()
    load_data = pd.merge(load_data, major1_data, on = 'Academic Interests 1', how = 'left')

    #Does the same thing but for the other major value
    major2_data = major_data.rename(columns={'Major / Program' : 'Academic Interests 2', 'UK Major' : 'SECONDARY_MAJOR_OF_INTEREST__C'})
    major2_data = major2_data.dropna()
    load_data = pd.merge(load_data, major2_data, on = 'Academic Interests 2', how = 'left')

    #Checks to see if the majors are the same
    load_data.loc[load_data['MAJOR_OF_INTEREST__C'] == load_data['SECONDARY_MAJOR_OF_INTEREST__C'],'SECONDARY_MAJOR_OF_INTEREST__C'] = ""

    return load_data

#Takes care of the majority of the copy, pasting, deleting, and editing done manually
def data_clean(load_data):
    #Shows the program which columns from the original file will be useful for the rest of the process
    load_data = load_data[['Visitor Id']+['First Name']+['Last Name'] +['Email'] + ['Phone'] + ['Street Address'] +['Street Address 2'] + ['City'] + ['State'] + ['Zipcode'] + ['Country'] + ['Enrollment Year'] + ['Academic Interests'] + ['High School CEEB Code'] + ['Title']]

    #Edits various columns
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

    load_data.loc[load_data["SOURCE__C"] == "",'SOURCE__C'] = "VisitDays"
    load_data.loc[load_data["STUDENT_STATUS__C"] == "","STUDENT_STATUS__C"] = "Inquiry"
    load_data.loc[load_data["TERM__C"] == "","TERM__C"] = "Fall"
    load_data.loc[load_data["YEAR__C"] == "","YEAR__C"] = load_data['Enrollment Year'] + 1
    load_data.loc[load_data["LOAD_DATE__C"] == "","LOAD_DATE__C"] = ""

    #Establishes the time on the day that the script was run and places it into the correct column
    today = dt.datetime.today().strftime("%m/%d/%Y")
    load_data['LOAD_DATE__C'] = load_data['LOAD_DATE__C'].map({'' : today})

    load_data['Title'] = load_data['Title'].fillna("")
    load_data.loc[load_data['Title'].str.contains('Transfer'), 'Title'] = 'Transfer'
    load_data.loc[load_data['Title'].str.contains('Transfer') == False, 'Title'] = 'Freshman'

    return load_data

def imports():
    #Check to see if the original file exists
    file = Path("200611_VisitDays_original.csv")
    if file.exists ():
        #If the original file exists, then read it into the dataframe
        load_data = pd.read_csv("200611_VisitDays_original.csv", encoding = "ISO-8859-1")
    else:
        #If it doesn't exist, warn the user
        print("VisitDays file not found")

    #Check to see if the file holding all of the major translations exists
    major_file = Path("major_decoder.xlsx")
    if major_file.exists():
        #Read in the majors
        major_data = pd.read_excel("major_decoder.xlsx")
        major_data = major_data[['Major / Program'] + ['UK Major']]
    else:
        #Warn the user that the major file doesn't exist
        print("Major Decoder file not found")

    dedup_file = Path("ConcatLoad.csv")
    if dedup_file.exists():
        dedup_data = pd.read_csv("ConcatLoad.csv")
    else:
        print("Dedup file is missing")

    SAP_file = Path("modified_SAP.csv")
    if SAP_file.exists():
        SAP_data = pd.read_csv("modified_SAP.csv")
    else:
        print("SAP File is missing boi")

    return load_data, major_data, dedup_data, SAP_data

def main():

    #importing the data from the original file into dataframes
    VisitDays_data, major_data, dedup_data, SAP_data = imports()

    #Takes care of the majority of the work in terms of copy and pasting, capitalizing properly, filling in details automatically like date/type of prospect
    VisitDays_data = data_clean(VisitDays_data)

    #Deduping this data
    VisitDays_data = data_dedup(VisitDays_data, dedup_data, SAP_data)

    #Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
    VisitDays_data = major_compare(VisitDays_data, major_data)

    #Renames all of the columns to their proper name that will be mapped in SF CRM
    VisitDays_data = data_rename(VisitDays_data)

    #Changes the order of the columns, not required/necessary but it makes it easier for me to read
    VisitDays_data = data_reorder(VisitDays_data)

    #Output the final file in CSV format
    VisitDays_data.to_csv('VisitDays_upgrade.csv', index=False)

    #Lets the user know that everything is done
    print("The transformation of the file is OVERRRR")

main()
