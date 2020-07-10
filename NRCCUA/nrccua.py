#Gets rid of all the warnings that come with it, they will probably need to be dealt with eventually
import warnings
warnings.filterwarnings("ignore")

#Rest of the libraries that are needed to complete this program. Pandas is the main driver for all the dataframes and everything, the rest are just accessories
import pandas as pd
from pathlib import *
import datetime as dt

def data_dedup(load_data, dedup_data, SAP_data):
    dedup_concat =  dedup_data[['Concat ID'] + ['Contact ID']]
    dedup_email = dedup_data[['Email'] + ['Contact ID']]

    load_data = pd.merge(load_data, dedup_concat, on = 'Concat ID', how = 'left')
    load_data = pd.merge(load_data, dedup_email, on = 'Email', how = 'left')
    load_data.loc[load_data['Contact ID_x'].isnull(),'Contact ID_x'] = load_data['Contact ID_y']

    load_data.drop_duplicates(subset = 'Sequence', keep=False,inplace=True)
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
    load_data = load_data.reindex(columns=['Purchase ID', 'SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'EMAIL__C', 'BIRTHDATE__C', 'GENDER__C', 'MOBILE__C', 'CONCATID__C', 'ADDRESS_LINE_1__C', 'ADDRESS_LINE_2__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C', 'COUNTRY__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'MAJOR_OF_INTEREST__C', 'SECONDARY_MAJOR_OF_INTEREST__C', 'AMERICAN_INDIAN_ALASKAN_NATIVE__C', 'ASIAN__C', 'BLACK_AFRICAN_AMERICAN__C', 'WHITE_CAUCASIAN__C', 'Hispanic/Latino', 'Race/Ethnicity Unknown', 'Contact ID', 'STUDENTSHORT'])
    return load_data

#Renames all of the columns to their proper name that will be mapped in SF CRM
def data_rename(load_data):
    load_data = load_data.rename(columns={'Sequence':'Purchase ID', 'FirstName':'FIRST_NAME__C', 'LastName':'LAST_NAME__C', 'Email':'EMAIL__C', 'BirthDate':'BIRTHDATE__C', 'Gender':'GENDER__C', 'Concat ID':'CONCATID__C', 'Address':'ADDRESS_LINE_1__C', 'Address2':'ADDRESS_LINE_2__C', 'City':'CITY__C','State':'STATE__C','Zipcode':'ZIP_CODE__C','GraduationYear':'HS_GRADUATION_YEAR__C','Ceeb':'HS_CEEB_CODE__C', 'Contact ID_x': 'Contact ID'})
    return load_data

#Figures out which races are listed out in the original datafile and assigns them a True or False value for SF CRM to house
def ethnicity_compare(load_data):
    #Checking to see if the data has the correct race values, replaces the if(countif()) function"
    load_data.loc[(load_data['Race01']== "BLACKRACE") | (load_data['Race02']== "BLACKRACE") | (load_data['Race03']== "BLACKRACE") | (load_data['Race04']== "BLACKRACE") , 'BLACK_AFRICAN_AMERICAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "BLACKRACE") & (load_data['Race02'] != "BLACKRACE") & (load_data['Race03'] != "BLACKRACE") & (load_data['Race04'] != "BLACKRACE"), 'BLACK_AFRICAN_AMERICAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "WHITERACE") | (load_data['Race02']== "WHITERACE") | (load_data['Race03']== "WHITERACE") | (load_data['Race04'] == "WHITERACE") , 'WHITE_CAUCASIAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "WHITERACE") & (load_data['Race02']!= "WHITERACE") & (load_data['Race03']!= "WHITERACE") & (load_data['Race04']!= "WHITERACE"), 'WHITE_CAUCASIAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "NATIVEAM") | (load_data['Race02']== "NATIVEAM") | (load_data['Race03']== "NATIVEAM") | (load_data['Race04'] == "NATIVEAM") , 'AMERICAN_INDIAN_ALASKAN_NATIVE__C'] = "True"
    load_data.loc[(load_data['Race01']!= "NATIVEAM") & (load_data['Race02']!= "NATIVEAM") & (load_data['Race03']!= "NATIVEAM") & (load_data['Race04']!= "NATIVEAM"), 'AMERICAN_INDIAN_ALASKAN_NATIVE__C'] = "False"

    load_data.loc[(load_data['Race01']== "ASIANRACE") | (load_data['Race02']== "ASIANRACE") | (load_data['Race03']== "ASIANRACE") | (load_data['Race04'] == "ASIANRACE") , 'ASIAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "ASIANRACE") & (load_data['Race02']!= "ASIANRACE") & (load_data['Race03']!= "ASIANRACE") & (load_data['Race04']!= "ASIANRACE"), 'ASIAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "LATINORACE") | (load_data['Race02']== "LATINORACE") | (load_data['Race03']== "LATINORACE") | (load_data['Race04'] == "LATINORACE") , 'Hispanic/Latino'] = "True"
    load_data.loc[(load_data['Race01']!= "LATINORACE") & (load_data['Race02']!= "LATINORACE") & (load_data['Race03']!= "LATINORACE") & (load_data['Race04']!= "LATINORACE"), 'Hispanic/Latino'] = "False"

    load_data.loc[(load_data['BLACK_AFRICAN_AMERICAN__C'] == "False") & (load_data['WHITE_CAUCASIAN__C']== 'False') & (load_data['AMERICAN_INDIAN_ALASKAN_NATIVE__C']== 'False' ) & (load_data['ASIAN__C'] == 'False') & (load_data['Hispanic/Latino'] == 'False') , 'Race/Ethnicity Unknown'] = "True"
    load_data.loc[(load_data['BLACK_AFRICAN_AMERICAN__C'] == "True") | (load_data['WHITE_CAUCASIAN__C']== 'True') | (load_data['AMERICAN_INDIAN_ALASKAN_NATIVE__C']== 'True' ) | (load_data['ASIAN__C'] == 'True') | (load_data['Hispanic/Latino'] == 'True') , 'Race/Ethnicity Unknown'] = "False"

    return load_data

#Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
def major_compare(load_data, major_data):
    #Makes sure that none of the capitilization impeeds the progress of matching for the majors since it's case sensitive
    major_data['Major / Program'] = major_data['Major / Program'].str.lower()
    load_data['Major01'] = load_data['Major01'].str.lower()
    load_data['Major02'] = load_data['Major02'].str.lower()

    #Prepares the major data for the merge and then does the vlookup function with the "merge" method
    major1_data =  major_data.rename(columns={'Major / Program' : 'Major01', 'UK Major' : 'MAJOR_OF_INTEREST__C'})
    major1_data = major1_data.dropna()
    load_data = pd.merge(load_data, major1_data, on = 'Major01', how = 'left')

    #Does the same thing but for the other major value
    major2_data = major_data.rename(columns={'Major / Program' : 'Major02', 'UK Major' : 'SECONDARY_MAJOR_OF_INTEREST__C'})
    major2_data = major2_data.dropna()
    load_data = pd.merge(load_data, major2_data, on = 'Major02', how = 'left')

    #Checks to see if the majors are the same
    load_data.loc[load_data['MAJOR_OF_INTEREST__C'] == load_data['SECONDARY_MAJOR_OF_INTEREST__C'],'SECONDARY_MAJOR_OF_INTEREST__C'] = ""

    return load_data

#Takes care of the majority of the copy, pasting, deleting, and editing done manually
def data_clean(load_data):
    #Shows the program which columns from the original file will be useful for the rest of the process
    load_data = load_data[['Sequence']+['FirstName']+['LastName']+['Address']+['Address2']+['City']+['State']+['Zipcode']+['Email']+['Phone']+['CellPhone']+['Ceeb']+['BirthDate']+['Gender']+['GraduationYear']+['Major01']+['Major02']+['Race01']+['Race02']+['Race03']+['Race04']]

    #Edits various columns
    load_data['Concat ID'] = load_data['FirstName'] + load_data['LastName'] + load_data['Address'].str[:10]
    load_data.loc[load_data["Concat ID"].isnull(),'Concat ID'] = load_data["FirstName"] + load_data["LastName"]
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
    load_data['SOURCE__C'] = ""
    load_data['LOAD_DATE__C'] = ""
    load_data['YEAR__C'] = ""
    load_data['TERM__C'] = ""
    load_data['STUDENT_STATUS__C'] = ""
    load_data['STUDENT_TYPE__C'] = ""
    load_data['MOBILE__C'] = ""
    load_data['COUNTRY__C'] = ""
    load_data['Gender'] = load_data['Gender'].map({'M' : 'Male', 'F': 'Female'})

    #Establishes the time on the day that the script was run and places it into the correct column
    today = dt.datetime.today().strftime("%m/%d/%Y")
    load_data['LOAD_DATE__C'] = load_data['LOAD_DATE__C'].map({'' : today})

    #Used the 'loc' function to fill in some of the columns that are empty and need to be filled with a certain string
    load_data.loc[load_data['MOBILE__C']== "",'MOBILE__C'] = load_data["CellPhone"]
    load_data.loc[load_data['MOBILE__C'].isnull(),'MOBILE__C'] = load_data["Phone"]
    load_data.loc[load_data["SOURCE__C"] == "",'SOURCE__C'] = "NRCCUA DSC"
    load_data.loc[load_data["STUDENT_STATUS__C"]== "","STUDENT_STATUS__C"] = "Inquiry"
    load_data.loc[load_data["TERM__C"]== "","TERM__C"] = "Fall"
    load_data.loc[load_data["STUDENT_TYPE__C"]== "","STUDENT_TYPE__C"] = "Freshman"
    load_data.loc[load_data['COUNTRY__C']== "",'COUNTRY__C'] = "US"
    load_data.loc[load_data["YEAR__C"] == "","YEAR__C"] = load_data['GraduationYear'] + 1

    return load_data

def imports():
    #Check to see if the original file exists
    file = Path("200515_NRCCUA_original.csv")
    if file.exists ():
        #If the original file exists, then read it into the dataframe
        load_data = pd.read_csv("200515_NRCCUA_original.csv", encoding = "ISO-8859-1")
    else:
        #If it doesn't exist, warn the user
        print("NRCCUA file not found")

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
    nrccua_data, major_data, dedup_data, SAP_data = imports()

    #Takes care of the majority of the work in terms of copy and pasting, capitalizing properly, filling in details automatically like date/type of prospect
    nrccua_data = data_clean(nrccua_data)

    #Deduping this data
    nrccua_data = data_dedup(nrccua_data, dedup_data, SAP_data)

    #Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
    nrccua_data = major_compare(nrccua_data, major_data)

    #Figures out which races are listed out in the original datafile and assigns them a True or False value for SF CRM to house
    nrccua_data = ethnicity_compare(nrccua_data)

    #Renames all of the columns to their proper name that will be mapped in SF CRM
    nrccua_data = data_rename(nrccua_data)

    #Changes the order of the columns, not required/necessary but it makes it easier for me to read
    nrccua_data = data_reorder(nrccua_data)

    #Output the final file in CSV format
    nrccua_data.to_csv('NRCCUA_upgrade.csv', index=False)

    #Lets the user know that everything is done
    print("NRCCUA data transformation complete!")

main()
