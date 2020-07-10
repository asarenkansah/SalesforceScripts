#Gets rid of all the warnings that come with it, they will probably need to be dealt with eventually
import warnings
warnings.filterwarnings("ignore")

#Rest of the libraries that are needed to complete this program. Pandas is the main driver for all the dataframes and everything, the rest are just accessories
import pandas as pd
from pathlib import *

def data_dedup(load_data, dedup_data, SAP_data):
#    dedup_concat =  dedup_data[['Concat ID'] + ['Contact ID']]
    dedup_email = dedup_data[['Email'] + ['Contact ID']]

#    load_data = pd.merge(load_data, dedup_concat, on = 'Concat ID', how = 'left')
    load_data = pd.merge(load_data, dedup_email, on = 'Email', how = 'left')
    load_data.loc[load_data['Contact ID_x'].isnull(),'Contact ID_x'] = load_data['Contact ID_y']

#    SAP_concat = SAP_data[['Concat ID'] + ['STUDENTSHORT']]
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

def imports():
    #Check to see if the original file exists
    file = Path("200515_NRCCUA_original.csv")
    if file.exists ():
        #If the original file exists, then read it into the dataframe
        load_data = pd.read_csv("200515_NRCCUA_original.csv", encoding = "ISO-8859-1")
    else:
        #If it doesn't exist, warn the user
        print("Load file not found")

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
    load_data = imports()
    load_data = data_dedup(load_data, dedup_data, SAP_data)

    load_data.to_csv("DedupUpgrade.csv")
