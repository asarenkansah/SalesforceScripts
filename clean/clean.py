
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
    file = Path("SAP.xlsx")
    if file.exists ():
        data = pd.read_excel("SAP.xlsx")
        data['Concat ID'] = data['VORNA'] + data['NACHN'] + data['FMTSTREET'].str[:10]
        SAP_data = data[['Concat ID'] + ['STUDENTSHORT'] + ['SMTP_ADDR'] + ['STUDENTSHORT'] + ['SMTP_ADDR1'] + ['STUDENTSHORT']]

        print(SAP_data.head())
#        SAP_data.to_csv('modified_SAP.csv')

    else:
        print ("SAP Applicant File not exist")

def eab():
    file = Path("eab.csv")
    if file.exists ():
        data = pd.read_csv("eab.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        eab_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]

        print(eab_data.head())
#        eab_data.to_csv('modified_eab.csv')

    else:
        print ("EAB Marketing Pop File not exist")

def marketing():
    file = Path("marketing.csv")
    if file.exists ():
        data = pd.read_csv("marketing.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        marketing_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]

        print(marketing_data.head())
#        tranfer_data.to_csv('modified_marketing.csv')

    else:
        print ("Marketing Pop File not exist")

def prospect():
    file = Path("prospect.csv")
    if file.exists ():
        data = pd.read_csv("prospect.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['Contact: First Name'] + data['Contact: Last Name'] + data['Contact: Mailing Address Line 1'].str[:10]
        prospect_data = data[ ['Concat ID'] + ['Contact: Contact ID'] + ['Contact: Email'] + ['Contact: Contact ID']]

        print(prospect_data.head())
#        prospect_data.to_csv('modified_prospect.csv')

    else:
        print ("Prospect Pop File not exist")

def transfer():
    file = Path("transfer.csv")
    if file.exists ():
        data = pd.read_csv("transfer.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        transfer_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]

        print(transfer_data.head())
#        tranfer_data.to_csv('modified_transfer.csv')

    else:
        print ("Transfer Pop File not exist")


def main():

    transfer()
    prospect()
    marketing()
    eab()
    SAP()


main()
