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
        print("Working on SAP Applicant File")
        data = pd.read_excel("SAP Applicant File.xlsx")
        
        data['Concat ID'] = data['VORNA'] + data['NACHN'] + data['FMTSTREET'].str[:10]
        for index, row in data.iterrows():
            if (row['FMTSTREET'] == ''):
                data.at[index,"Concat ID"] = row['VORNA'] + row['NACHN']               

        data['Concat ID'] = data['Concat ID'].str.lower()
        
        SAP_data = data[['Concat ID'] + ['STUDENTSHORT'] + ['SMTP_ADDR'] + ['STUDENTSHORT'] + ['SMTP_ADDR1'] + ['STUDENTSHORT']]
        SAP_data.reset_index(drop=True, inplace=True)
        SAP_data.to_csv('modified_SAP.csv')

    else:
        print ("SAP Applicant File not exist")

def eab():
    file = Path("eab.csv")
    if file.exists ():
        data = pd.read_csv("eab.csv", encoding = "ISO-8859-1")
        print("Working on EAB Pop")
        
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]

        for index, row in data.iterrows():
            if(row['Mailing Street'] == ''):
                data.at[index,"Concat ID"] = row['First Name'] + row['Last Name']

        data['Concat ID'] = data['Concat ID'].str.lower()
       
        eab_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]
        eab_data.reset_index(drop=True, inplace=True)
        eab_data.to_csv('modified_eab.csv')

    else:
        print ("EAB Marketing Pop File not exist")

def marketing():
    file = Path("marketing.csv")
    if file.exists ():
        print("Working on Marketing Pop")
        data = pd.read_csv("marketing.csv", encoding = "ISO-8859-1")
        
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]

        for index, row in data.iterrows():
            if(row['Mailing Street'] == ''):
                data.at[index,"Concat ID"] = row['First Name'] + row['Last Name']

        data['Concat ID'] = data['Concat ID'].str.lower()
        
        marketing_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]
        marketing_data.reset_index(drop=True, inplace=True)
        marketing_data.to_csv('modified_marketing.csv')

    else:
        print ("Marketing Pop File not exist")

def prospect():
    file = Path("prospect.csv")
    if file.exists ():
        print("Working on Prospect Pop")
        data = pd.read_csv("prospect.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['Contact: First Name'] + data['Contact: Last Name'] + data['Contact: Mailing Address Line 1'].str[:10]
        
        for index, row in data.iterrows():
            if(row['Contact: Mailing Address Line 1'] == ''):
                data.at[index,"Concat ID"] = row['Contact: First Name'] + row['Contact: Last Name']

        data['Concat ID'] = data['Concat ID'].str.lower()
            
        prospect_data = data[['Concat ID'] + ['Contact: Contact ID'] + ['Contact: Email'] + ['Contact: Contact ID']]
        prospect_data.reset_index(drop=True, inplace=True)
        prospect_data.to_csv('modified_prospect.csv')

    else:
        print ("Prospect Pop File not exist")

def transfer():
    file = Path("transfer.csv")
    if file.exists ():
        data = pd.read_csv("transfer.csv", encoding = "ISO-8859-1")
        print("Working on Transfer Pop") 
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        for index, row in data.iterrows():
            if(row['Mailing Street'] == ''):
                data.at[index,"Concat ID"] = row['First Name'] + row['Last Name']

        data['Concat ID'] = data['Concat ID'].str.lower()
        
        transfer_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]
        transfer_data.reset_index(drop=True, inplace=True)
        transfer_data.to_csv('modified_transfer.csv')

    else:
        print ("Transfer Pop File not exist")


def main():
    transfer()
    prospect()
    marketing()
    eab()
    SAP()
    print("DONE!")

main()
