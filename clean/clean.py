# Notes from Reading the Data
#     col = list(data.columns.values)
#     concat = data['First Name'].map(str) + data['Last Name'].map(str)
#    print(data.sort_values(['First Name']))
#    print(data.loc[data['First Name'] == 'Chad'])
#    for index, row in data.iterrows():
#        print(index, row['Source'])
# data = data.drop(columns = ['New Column'])
#Notes from editing the Data

import pandas as pd
from pathlib import *

def SAP():
    file = Path("SAP Applicant File.xlsx")
    if file.exists ():
        data = pd.read_excel("SAP Applicant File.xlsx")
        data['Concat ID'] = data['VORNA'] + data['NACHN'] + data['FMTSTREET'].str[:10]
        data['Concat ID'] = data['Concat ID'].str.lower()
        SAP_data = data[['Concat ID'] + ['STUDENTSHORT'] + ['SMTP_ADDR'] + ['STUDENTSHORT'] + ['SMTP_ADDR1'] + ['STUDENTSHORT']]

#        print(SAP_data.head())
        SAP_data.to_csv('modified_SAP.csv')
        print('SAP Applicant File Complete')

    else:
        print ("SAP Applicant File not exist")

def eab():
    file = Path("eab.csv")
    if file.exists ():
        data = pd.read_csv("eab.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        data['Concat ID'] = data['Concat ID'].str.lower()
        eab_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]

#        print(eab_data.head())
        eab_data.to_csv('modified_eab.csv')
        print('EAB Marketing Pop Complete')
    else:
        print ("EAB Marketing Pop File not exist")

def marketing():
    file = Path("marketing.csv")
    if file.exists ():
        data = pd.read_csv("marketing.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        data['Concat ID'] = data['Concat ID'].str.lower()
        marketing_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]

#        print(marketing_data.head())
        marketing_data.to_csv('modified_marketing.csv')
        print('Marketing Pop Complete')
        

    else:
        print ("Marketing Pop File not exist")

def prospect():
    file = Path("prospects.csv")
    if file.exists ():
        data = pd.read_csv("prospects.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['Contact: First Name'] + data['Contact: Last Name'] + data['Contact: Mailing Address Line 1'].str[:10]
        data['Concat ID'] = data['Concat ID'].str.lower()        
        prospect_data = data[ ['Concat ID'] + ['Contact: Contact ID'] + ['Contact: Email'] + ['Contact: Contact ID']]

#        print(prospect_data.head())
        prospect_data.to_csv('modified_prospect.csv')
        print('20-21 Prospects Complete')

    else:
        print ("Prospect Pop File not exist")

def transfer():
    file = Path("transfer.csv")
    if file.exists ():
        data = pd.read_csv("transfer.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        data['Concat ID'] = data['Concat ID'].str.lower()
        transfer_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]

#        print(transfer_data.head())
        transfer_data.to_csv('modified_transfer.csv')
        print('Transfer Pop Complete')

    else:
        print ("Transfer Pop File not exist")


def main():

    transfer()
    prospect()
    marketing()
    eab()
    SAP()

main()
