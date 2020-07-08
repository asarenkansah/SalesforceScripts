#Gets rid of all the warnings that come with it, they will probably need to be dealt with eventually
import warnings
warnings.filterwarnings("ignore")

#Rest of the libraries that are needed to complete this program. Pandas is the main driver for all the dataframes and everything, the rest are just accessories
import pandas as pd
from pathlib import *
import numpy as np
import datetime as dt

def data_dedup(final_data, dedup_data):
    dedup_concat =  dedup_data[['Concat ID'] + ['Contact ID']]
    dedup_email = dedup_data[['Email'] + ['Contact ID']]

    concat_data = pd.merge(final_data, dedup_concat, on = 'Concat ID', how = 'left')
    final_data = pd.merge(final_data, dedup_concat, on = 'Concat ID', how = 'left')
    email_data = pd.merge(final_data, dedup_email, on = 'Email', how = 'left')
#    final_data = pd.merge(final_data, dedup_email, on = 'Email', how = 'left')

    concat_data = concat_data[["Contact ID"]]
    email_data = concat_data[["Contact ID"]]
    final_contact_data = pd.concat([concat_data, email_data], axis=0)

#    final_contact_data = final_contact_data.dropna()
#    final_data['Visitor Type'] = final_data['Visitor Type'].fillna("")
#    final_data.drop(final_data[final_data["Contact ID"] == ""].index, inplace=True)
#    print(final_data.head())
#    final_contact_data.to_csv("tracker_data.csv")

    return final_data

#Changes the order of the columns, not required/necessary but it makes it easier for me to read
def data_reorder(final_data):
    final_data = final_data.reindex(columns=['SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'CONCATID__C', 'EMAIL__C', 'BIRTHDATE__C', 'ADDRESS_LINE_1__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C', 'MOBILE__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'MAJOR_OF_INTEREST__C', 'COUNTRY__C'])
    return final_data

#Renames all of the columns to their proper name that will be mapped in SF CRM
def data_rename(final_data):
    final_data = final_data.rename(columns={'First Name' : 'FIRST_NAME__C', 'Last Name' : 'LAST_NAME__C' , 'Email' : 'EMAIL__C', 'Date of Birth': 'BIRTHDATE__C' , 'Concat ID' : 'CONCATID__C', 'Street' : 'ADDRESS_LINE_1__C', 'City':'CITY__C', 'State / Region':'STATE__C', 'Postal Code':'ZIP_CODE__C', 'Phone':'MOBILE__C', 'Enrollment Year':'HS_GRADUATION_YEAR__C', 'Ceeb Code':'HS_CEEB_CODE__C', 'UK Major':'MAJOR_OF_INTEREST__C', 'Country':'COUNTRY__C', 'Visitor Type':'STUDENT_TYPE__C'})
    return final_data

#Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
def major_compare(load_data, major_data):
    #Makes sure that none of the capitilization impeeds the progress of matching for the majors since it's case sensitive
    major_data['Major / Program'] = major_data['Major / Program'].str.lower()

    #Essentially acts as a vlookup function
    major_data = major_data.groupby(['Major / Program']).first()
    load_data = pd.merge(load_data, major_data, on = 'Major / Program', how = 'left')

    return load_data

#Takes care of the majority of the copy, pasting, deleting, and editing done manually
def data_clean(load_data):
    #Shows the program which columns from the original file will be useful for the rest of the process
    load_data = load_data[['Visitor Type'] + ['First Name'] + ['Last Name'] + ['Email'] + ['Enrollment Year'] + ['Phone'] + ['Date of Birth'] + ['Major / Program']  + ['Street'] + ['City'] + ['State / Region'] + ['Postal Code']+ ['Country']  + ['Ceeb Code']]

    #Edits various columns
    load_data['Concat ID'] = load_data['First Name'] + load_data['Last Name'] + load_data['Street'].str[:10]
    load_data.loc[load_data["Concat ID"].isnull(),'Concat ID'] = load_data["First Name"] + load_data["Last Name"]
    load_data['Concat ID'] = load_data['Concat ID'].str.lower()
    load_data['First Name'] = load_data['First Name'].str.title()
    load_data['Last Name'] = load_data['Last Name'].str.title()
    load_data['Street'] = load_data['Street'].str.title()
    load_data['City'] = load_data['City'].str.title()
    load_data['Country'] = load_data['Country'].map({'United States': 'US'})
    load_data['Major / Program'] = load_data['Major / Program'].str.strip()
    load_data['Major / Program'] = load_data['Major / Program'].str.lower()
    load_data['Major Tester'] = ""
    load_data["SOURCE__C"] = ""
    load_data["LOAD_DATE__C"] = ""
    load_data["YEAR__C"] = ""
    load_data.loc[load_data["YEAR__C"] == "","YEAR__C"] = load_data['Enrollment Year'] + 1
    load_data["TERM__C"] = ""
    load_data["STUDENT_STATUS__C"] = ""

    #Establishes the time on the day that the script was run and places it into the correct column
    today = dt.datetime.today().strftime("%d/%m/%Y")
    load_data['LOAD_DATE__C'] = load_data['LOAD_DATE__C'].map({'' : today})

    #This is to take out unnecessary rows that are in the Visitor Type column, we only want high school students or transfers
    #Hoping to find another way to do this somehow, but it works
    for index, row in load_data.iterrows():
        if(row['Visitor Type'] == "Alumni" or row['Visitor Type'] == "Parent of High School Student" or row['Visitor Type'] == "College Graduate" or row['Visitor Type'] == "Faculty / Staff" or row['Visitor Type'] == "Faculty / Staff"  or row['Visitor Type'] == 'Parent of Transfer Student' or row['Visitor Type'] == 'Parent of High School Graduate' or row['Visitor Type'] == 'School Counselor' or row['Visitor Type'] == 'Parent of College Graduate'):
            load_data = load_data.drop([index])

    #Used the 'loc' function to fill in some of the columns that are empty and need to be filled with a certain string
    load_data.loc[load_data["SOURCE__C"] == "",'SOURCE__C'] = "YouVisit"
    load_data.loc[load_data["STUDENT_STATUS__C"]== "","STUDENT_STATUS__C"] = "Inquiry"
    load_data.loc[load_data["TERM__C"]== "","TERM__C"] = "Fall"
    load_data['Visitor Type'] = load_data['Visitor Type'].fillna("")
    load_data.loc[load_data['Visitor Type'].str.contains('Transfer'), 'Visitor Type'] = 'Transfer'
    load_data.loc[load_data['Visitor Type'].str.contains('Transfer') == False, 'Visitor Type'] = 'Freshman'

    return load_data

#importing the data from the original file into dataframes
def imports():
    #Check to see if the original file exists
    file = Path("200601_YouVisit_original.csv")
    if file.exists ():
        #If the original file exists, then read it into the dataframe
        data = pd.read_csv("200601_YouVisit_original.csv", encoding = "ISO-8859-1")
    else:
        #If it doesn't exist, warn the user
        print("YouVisit file not found")

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

    return data, major_data, dedup_data

def main():
    #importing the data from the original file into dataframes
    YouVisit_data, major_data, dedup_data = imports()

    #Takes care of the majority of the work in terms of copy and pasting, capitalizing properly, filling in details automatically like date/type of prospect
    YouVisit_data = data_clean(YouVisit_data)

    #Deduping this data
    YouVisit_data = data_dedup(YouVisit_data, dedup_data)
#    YouVisit_data.to_csv("dedup_test.csv")

    #Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
    YouVisit_data = major_compare(YouVisit_data, major_data)

    #Renames all of the columns to their proper name that will be mapped in SF CRM
    YouVisit_data = data_rename(YouVisit_data)

    #Changes the order of the columns, not required/necessary but it makes it easier for me to read
    YouVisit_data = data_reorder(YouVisit_data)

    #Output the final file in CSV format
    YouVisit_data.to_csv('YouVisit_upgrade.csv', index=False)

    #Lets the user know that everything is done
    print("File transformation complete")

main()
