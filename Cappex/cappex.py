#Gets rid of all the warnings that come with it, they will probably need to be dealt with eventually
import warnings
warnings.filterwarnings("ignore")

#Rest of the libraries that are needed to complete this program. Pandas is the main driver for all the dataframes and everything, the rest are just accessories
import pandas as pd
from pathlib import *
import datetime as dt

#Changes the order of the columns, not required/necessary but it makes it easier for me to read
def data_reorder(final_data):
    final_data = final_data.reindex(columns=['SOURCE__C', 'LOAD_DATE__C', 'FIRST_NAME__C', 'LAST_NAME__C', 'CONCATID__C', 'EMAIL__C', 'BIRTHDATE__C', 'GENDER__C', 'ADDRESS_LINE_1__C', 'ADDRESS_LINE_2__C', 'CITY__C', 'STATE__C', 'ZIP_CODE__C', 'COUNTRY__C', 'MOBILE__C', 'HS_GRADUATION_YEAR__C', 'HS_CEEB_CODE__C', 'YEAR__C', 'TERM__C', 'STUDENT_STATUS__C', 'STUDENT_TYPE__C', 'ACT/SAT Max Cumulative', 'MAJOR_OF_INTEREST__C', 'SECONDARY_MAJOR_OF_INTEREST__C', 'AMERICAN_INDIAN_ALASKAN_NATIVE__C', 'ASIAN__C', 'BLACK_AFRICAN_AMERICAN__C', 'WHITE_CAUCASIAN__C', 'Hispanic/Latino', 'Race/Ethnicity Unknown'])
    return final_data

#Renames all of the columns to their proper name that will be mapped in SF CRM
def data_rename(final_data):
    final_data = final_data.rename(columns={'Inquiry Product' : 'SOURCE__C', 'First Name': 'FIRST_NAME__C', 'Last Name':'LAST_NAME__C', 'Concat ID':'CONCATID__C', 'Email Address':'EMAIL__C', 'Birth Date':'BIRTHDATE__C', 'Gender':'GENDER__C', 'Address1':'ADDRESS_LINE_1__C', 'Address2':'ADDRESS_LINE_2__C', 'City':'CITY__C', 'State':'STATE__C', 'Zip Code':'ZIP_CODE__C', 'Country':'COUNTRY__C', 'Primary Phone':'MOBILE__C', 'Expected HS Graduation Date':'HS_GRADUATION_YEAR__C', 'ACT Composite':'ACT/SAT Max Cumulative', 'CEEB Code': 'HS_CEEB_CODE__C'})
    return final_data

#Figures out which races are listed out in the original datafile and assigns them a True or False value for SF CRM to house
def ethnicity_compare(load_data):
    #Delimits the current column that holds each records races and splits it up. Creating a new dataframe to hold those values
    race_holder = load_data['Ethnicity - Fixed List'].str.split(pat =",", expand = True)
    #Changing the column values to a string
    race_holder.columns = race_holder.columns.astype(str)
    #Copying and pasting the race data values into the working dataframe
    selected_column_0 = race_holder[["0"]]
    selected_column_1 = race_holder[["1"]]
    selected_column_2 = race_holder[["2"]]
    load_data['Race01'] = selected_column_0.copy()
    load_data['Race02'] = selected_column_1.copy()
    load_data['Race03'] = selected_column_2.copy()
    #Taking off any extra white space
    load_data['Race02'] = load_data['Race02'].str.strip()
    load_data['Race03'] = load_data['Race03'].str.strip()

    #Checking to see if the data has the correct race values, replaces the if(countif()) function"
    load_data.loc[(load_data['Race01']== "African American") | (load_data['Race02']== "African American") | (load_data['Race03']== "African American") , 'BLACK_AFRICAN_AMERICAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "African American") & (load_data['Race02'] != "African American") & (load_data['Race03'] != "African American") , 'BLACK_AFRICAN_AMERICAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "White") | (load_data['Race02']== "White") | (load_data['Race03']== "White") , 'WHITE_CAUCASIAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "White") & (load_data['Race02']!= "White") & (load_data['Race03']!= "White"), 'WHITE_CAUCASIAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "American Indian or Native Alaskan") | (load_data['Race02']== "American Indian or Native Alaskan") | (load_data['Race03']== "American Indian or Native Alaskan") , 'AMERICAN_INDIAN_ALASKAN_NATIVE__C'] = "True"
    load_data.loc[(load_data['Race01']!= "American Indian or Native Alaskan") & (load_data['Race02']!= "American Indian or Native Alaskan") & (load_data['Race03']!= "American Indian or Native Alaskan"), 'AMERICAN_INDIAN_ALASKAN_NATIVE__C'] = "False"

    load_data.loc[(load_data['Race01']== "Asian or Pacific Islander") | (load_data['Race02']== "Asian or Pacific Islander") | (load_data['Race03']== "Asian or Pacific Islander") , 'ASIAN__C'] = "True"
    load_data.loc[(load_data['Race01']!= "Asian or Pacific Islander") & (load_data['Race02']!= "Asian or Pacific Islander") & (load_data['Race03']!= "Asian or Pacific Islander"), 'ASIAN__C'] = "False"

    load_data.loc[(load_data['Race01']== "Hispanic/Latino") | (load_data['Race02']== "Hispanic/Latino") | (load_data['Race03']== "Hispanic/Latino"), 'Hispanic/Latino'] = "True"
    load_data.loc[(load_data['Race01']!= "Hispanic/Latino") & (load_data['Race02']!= "Hispanic/Latino") & (load_data['Race03']!= "Hispanic/Latino"), 'Hispanic/Latino'] = "False"

    load_data.loc[(load_data['BLACK_AFRICAN_AMERICAN__C'] == "False") & (load_data['WHITE_CAUCASIAN__C']== 'False') & (load_data['AMERICAN_INDIAN_ALASKAN_NATIVE__C']== 'False' ) & (load_data['ASIAN__C'] == 'False') & (load_data['Hispanic/Latino'] == 'False') , 'Race/Ethnicity Unknown'] = "True"
    load_data.loc[(load_data['BLACK_AFRICAN_AMERICAN__C'] == "True") | (load_data['WHITE_CAUCASIAN__C']== 'True') | (load_data['AMERICAN_INDIAN_ALASKAN_NATIVE__C']== 'True' ) | (load_data['ASIAN__C'] == 'True') | (load_data['Hispanic/Latino'] == 'True') , 'Race/Ethnicity Unknown'] = "False"

    return load_data

#Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
def major_compare(load_data, major_data):

    #Makes sure that none of the capitilization impeeds the progress of matching for the majors since it's case sensitive
    major_data['Major / Program'] = major_data['Major / Program'].str.lower()
    load_data['Major 1'] = load_data['Major 1'].str.lower()
    load_data['Major 2'] = load_data['Major 2'].str.lower()

    #Prepares the major data for the merge and then does the vlookup function with the "merge" method
    major1_data =  major_data.rename(columns={'Major / Program' : 'Major 1', 'UK Major' : 'MAJOR_OF_INTEREST__C'})
    major1_data = major1_data.dropna()
    load_data = pd.merge(load_data, major1_data, on = 'Major 1', how = 'left')

    #Does the same thing but for the other major value
    major2_data = major_data.rename(columns={'Major / Program' : 'Major 2', 'UK Major' : 'SECONDARY_MAJOR_OF_INTEREST__C'})
    major2_data = major2_data.dropna()
    load_data = pd.merge(load_data, major2_data, on = 'Major 2', how = 'left')

    #Checks to see if the majors are the same
    load_data.loc[load_data['MAJOR_OF_INTEREST__C'] == load_data['SECONDARY_MAJOR_OF_INTEREST__C'],'SECONDARY_MAJOR_OF_INTEREST__C'] = ""

    return load_data

#Takes care of the majority of the copy, pasting, deleting, and editing done manually
def data_clean(load_data):
    #Shows the program which columns from the original file will be useful for the rest of the process
    load_data = load_data[['Inquiry Product'] + ['First Name'] + ['Last Name'] + ['Email Address'] + ['Birth Date'] + ['Gender'] + ['Address1'] + ['Address2'] + ['City'] + ['State'] + ['Zip Code'] + ['Country'] + ['Primary Phone'] + ['Expected HS Graduation Date'] + ['ACT Composite'] + ['CEEB Code'] + ['Major 1'] + ['Major 2'] + ['Ethnicity - Fixed List']]

    #Edits various columns
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
    load_data['Hispanic/Latino']= ""
    load_data['Race/Ethnicity Unknown']= ""
    load_data['LOAD_DATE__C'] = ""
    load_data['YEAR__C'] = ""
    load_data['TERM__C'] = ""
    load_data['STUDENT_STATUS__C'] = ""
    load_data['STUDENT_TYPE__C'] = ""

    #Establishes the time on the day that the script was run and places it into the correct column
    today = dt.datetime.today().strftime("%d/%m/%Y")
    load_data['LOAD_DATE__C'] = load_data['LOAD_DATE__C'].map({'' : today})

    #Replacing some values specifically so that they will be ready correctly by SF CRM
    load_data['Inquiry Product'].replace({'Greenlight':'Cappex Greenlight'}, inplace=True)
    load_data['Country'].replace({'United States': 'US', 'USA':'US'}, inplace=True)
    load_data['Gender'].replace({'M' : 'Male', 'F': 'Female'}, inplace=True)

    #Used the 'loc' function to fill in some of the columns that are empty and need to be filled with a certain string
    load_data.loc[load_data["STUDENT_STATUS__C"]== "","STUDENT_STATUS__C"] = "Inquiry"
    load_data.loc[load_data["TERM__C"]== "","TERM__C"] = "Fall"
    load_data.loc[load_data["STUDENT_TYPE__C"]== "","STUDENT_TYPE__C"] = "Freshman"

    return load_data

def imports():
    #Check to see if the original file exists
    file = Path("200619_Cappex_original.csv")
    if file.exists ():
        #If the original file exists, then read it into the dataframe
        data = pd.read_csv("200619_Cappex_original.csv", encoding = "ISO-8859-1")
    else:
        #If it doesn't exist, warn the user
        print("Cappex file not found")

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
    cappex_data, major_data = imports()

    #Takes care of the majority of the work in terms of copy and pasting, capitalizing properly, filling in details automatically like date/type of prospect
    cappex_data = data_clean(cappex_data)

    #Major Translater: Converts the original majors into a a format that is easily read by SF CRM database
    cappex_data = major_compare(cappex_data, major_data)

    #Figures out which races are listed out in the original datafile and assigns them a True or False value for SF CRM to house
    cappex_data = ethnicity_compare(cappex_data)

    #Renames all of the columns to their proper name that will be mapped in SF CRM
    cappex_data = data_rename(cappex_data)

    #Changes the order of the columns, not required/necessary but it makes it easier for me to read
    cappex_data = data_reorder(cappex_data)

    #Output the final file in CSV format
    cappex_data.to_csv('Cappex_upgrade.csv', index=False)

    #Lets the user know that everything is done
    print("Cappex data transformation complete!")

main()
