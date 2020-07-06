#Gets rid of all the warnings that come with it, they will probably need to be dealt with eventually
import warnings
warnings.filterwarnings("ignore")

#Rest of the libraries that are needed to complete this program. Pandas is the main driver for all the dataframes and everything, the rest are just accessories
import pandas as pd
from pathlib import *
import datetime as dt

#Changes the order of the columns, not required/necessary but it makes it easier for me to read
def data_reorder(final_data):
    final_data = final_data.reindex(columns=['PURCHASE_ID__C', 'SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'CONCATID__C', 'EMAIL__C', 'BIRTHDATE__C', 'GENDER__C', 'ADDRESS_LINE_1__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C','COUNTRY__C', 'MOBILE__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'MAJOR_OF_INTEREST__C', 'SECONDARY_MAJOR_OF_INTEREST__C','Race/Ethnicity','AMERICAN_INDIAN_ALASKAN_NATIVE__C', 'ASIAN__C', 'BLACK_AFRICAN_AMERICAN__C', 'WHITE_CAUCASIAN__C', 'Hispanic/Latino'])
    return final_data

#Renames all of the columns to their proper name that will be mapped in SF CRM
def data_rename(final_data):
    final_data = final_data.rename(columns={'Gender':'GENDER__C', 'DOB':'BIRTHDATE__C', 'Raise.me Code' : 'PURCHASE_ID__C', 'First Name' : 'FIRST_NAME__C', 'Last Name' : 'LAST_NAME__C', 'Email' : 'EMAIL__C', 'Concat ID' : 'CONCATID__C' , 'Street Address' : 'ADDRESS_LINE_1__C' , 'City' : 'CITY__C', 'Subdivision' : 'STATE__C', 'Postal Code' : 'ZIP_CODE__C', 'Mobile': 'MOBILE__C', 'Projected Graduation Year': 'HS_GRADUATION_YEAR__C', 'CEEB Code': 'HS_CEEB_CODE__C', 'Country' : 'COUNTRY__C', })
    return final_data

#Figures out which races are listed out in the original datafile and assigns them a True or False value for SF CRM to house
def ethnicity_compare(load_data):
    #Delimits the current column that holds each records races and splits it up. Creating a new dataframe to hold those values
    race_holder = load_data['Race/Ethnicity'].str.split(pat =",", expand = True)
    #Changing the column values to a string
    race_holder.columns = race_holder.columns.astype(str)
    #Copying and pasting the races data values into the working dataframe
    selected_column_0 = race_holder[["0"]]
    selected_column_1 = race_holder[["1"]]
    load_data['Race01'] = selected_column_0.copy()
    load_data['Race02'] = selected_column_1.copy()
    #Taking off any extra white space
    load_data['Race02'] = load_data['Race02'].str.strip()

    #Checking to see if the data has the correct race values, replaces the if(countif()) function"
    load_data.loc[(load_data['Race01']== "Black or African American") | (load_data['Race02']== "Black or African American"), 'BLACK_AFRICAN_AMERICAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "Black or African American") & (load_data['Race02'] != "Black or African American") , 'BLACK_AFRICAN_AMERICAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "White") | (load_data['Race02']== "White") , 'WHITE_CAUCASIAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "White") & (load_data['Race02']!= "White"), 'WHITE_CAUCASIAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "American Indian or Alaska Native") | (load_data['Race02']== "American Indian or Alaska Native") , 'AMERICAN_INDIAN_ALASKAN_NATIVE__C'] = "True"
    load_data.loc[(load_data['Race01']!= "American Indian or Alaska Native") & (load_data['Race02']!= "American Indian or Alaska Native"), 'AMERICAN_INDIAN_ALASKAN_NATIVE__C'] = "False"

    load_data.loc[(load_data['Race01']== "Asian") | (load_data['Race02']== "Asian") , 'ASIAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "Asian") & (load_data['Race02']!= "Asian"), 'ASIAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "Hispanic or Latino") | (load_data['Race02']== "Hispanic or Latino"), 'Hispanic/Latino'] = "True"
    load_data.loc[(load_data['Race01']!= "Hispanic or Latino") & (load_data['Race02']!= "Hispanic or Latino"), 'Hispanic/Latino'] = "False"

    load_data.loc[(load_data['BLACK_AFRICAN_AMERICAN__C'] == "False") & (load_data['WHITE_CAUCASIAN__C']== 'False') & (load_data['AMERICAN_INDIAN_ALASKAN_NATIVE__C']== 'False' ) & (load_data['ASIAN__C'] == 'False') & (load_data['Hispanic/Latino'] == 'False') , 'Race/Ethnicity Unknown'] = "True"
    load_data.loc[(load_data['BLACK_AFRICAN_AMERICAN__C'] == "True") | (load_data['WHITE_CAUCASIAN__C']== 'True') | (load_data['AMERICAN_INDIAN_ALASKAN_NATIVE__C']== 'True' ) | (load_data['ASIAN__C'] == 'True') | (load_data['Hispanic/Latino'] == 'True') , 'Race/Ethnicity Unknown'] = "False"

    return load_data

#Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
def major_compare(load_data, major_data):

    #Makes sure that none of the capitilization impeeds the progress of matching for the majors since it's case sensitive
    major_data['Major / Program'] = major_data['Major / Program'].str.lower()
    load_data['Academic Interest Name 1'] = load_data['Academic Interest Name 1'].str.lower()
    load_data['Academic Interest Name 2'] = load_data['Academic Interest Name 2'].str.lower()

    #Prepares the major data for the merge and then does the vlookup function with the "merge" method
    major1_data =  major_data.rename(columns={'Major / Program' : 'Academic Interest Name 1', 'UK Major' : 'MAJOR_OF_INTEREST__C'})
    major1_data = major1_data.dropna()
    load_data = pd.merge(load_data, major1_data, on = 'Academic Interest Name 1', how = 'left')

    #Does the same thing but for the other major value
    major2_data = major_data.rename(columns={'Major / Program' : 'Academic Interest Name 2', 'UK Major' : 'SECONDARY_MAJOR_OF_INTEREST__C'})
    major2_data = major2_data.dropna()
    load_data = pd.merge(load_data, major2_data, on = 'Academic Interest Name 2', how = 'left')

    #Checks to see if the majors are the same
    load_data.loc[load_data['MAJOR_OF_INTEREST__C'] == load_data['SECONDARY_MAJOR_OF_INTEREST__C'],'SECONDARY_MAJOR_OF_INTEREST__C'] = ""

    return load_data

#Takes care of the majority of the copy, pasting, deleting, and editing done manually
def data_clean(load_data):
    #Shows the program which columns from the original file will be useful for the rest of the process
    load_data = load_data[['Raise.me Code'] + ['First Name'] + ['Last Name'] + ['Email'] + ['Mobile'] + ['Projected Graduation Year'] + ['DOB'] + ['Academic Interest Name 1'] + ['Academic Interest Name 2'] + ['Street Address'] + ['City'] + ['Subdivision'] + ['Postal Code']+ ['Country'] + ['Gender']  + ['CEEB Code'] + ['Race/Ethnicity']]

    #Edits various columns
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
    load_data["SOURCE__C"] = ""
    load_data['LOAD_DATE__C'] = ""
    load_data['YEAR__C'] = ""
    load_data['TERM__C'] = ""
    load_data['STUDENT_STATUS__C'] = ""
    load_data['STUDENT_TYPE__C'] = ""
    load_data['Gender'] = load_data['Gender'].map({'M' : 'Male', 'F': 'Female'})
    load_data.loc[load_data["YEAR__C"] == "","YEAR__C"] = load_data['Projected Graduation Year'] + 1

    #Establishes the time on the day that the script was run and places it into the correct column
    today = dt.datetime.today().strftime("%d/%m/%Y")
    load_data['LOAD_DATE__C'] = load_data['LOAD_DATE__C'].map({'' : today})

    #Used the 'loc' function to fill in some of the columns that are empty and need to be filled with a certain string
    load_data.loc[load_data["SOURCE__C"] == "",'SOURCE__C'] = "RaiseMe"
    load_data.loc[load_data["STUDENT_STATUS__C"] == "","STUDENT_STATUS__C"] = "Inquiry"
    load_data.loc[load_data["TERM__C"] == "","TERM__C"] = "Fall"
    load_data.loc[load_data["STUDENT_TYPE__C"] == "","STUDENT_TYPE__C"] = "Freshman"

    return load_data

def imports():
    #Check to see if the original file exists
    file = Path("200616_RaiseMe_original.csv")
    if file.exists ():
        #If the original file exists, then read it into the dataframe
        data = pd.read_csv("200616_RaiseMe_original.csv")
    else:
        #If it doesn't exist, warn the user
        print("RaiseMe file not found")

    #Check to see if the file holding all of the major translations exists
    major_file = Path("major_decoder.xlsx")
    if major_file.exists():
        #Read in the majors
        major_data = pd.read_excel("major_decoder.xlsx")
        major_data = major_data[['Major / Program'] + ['UK Major']]
    else:
        #Warn the user that the major file doesn't exist
        print("Major Decoder file not found")

    return data, major_data

def main():
    #importing the data from the original file into dataframes
    RaiseMe_data, major_data = imports()

    #Takes care of the majority of the work in terms of copy and pasting, capitalizing properly, filling in details automatically like date/type of prospect
    RaiseMe_data = data_clean(RaiseMe_data)

    #Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
    RaiseMe_data = major_compare(RaiseMe_data, major_data)

    #Figures out which races are listed out in the original datafile and assigns them a True or False value for SF CRM to house
    RaiseMe_data = ethnicity_compare(RaiseMe_data)

    #Renames all of the columns to their proper name that will be mapped in SF CRM
    RaiseMe_data = data_rename(RaiseMe_data)

    #Changes the order of the columns, not required/necessary but it makes it easier for me to read
    RaiseMe_data = data_reorder(RaiseMe_data)

    #Output the final file in CSV format
    RaiseMe_data.to_csv('RaiseMe_upgrade.csv', index=False)

    #Lets the user know that everything is done
    print('YOUR FILE HAS BEEN TRANSFORMED INTO A FINISHED PRODUCT')

main()
