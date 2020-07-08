import pandas as pd
from pathlib import *

def SAP():
    file = Path("SAP Applicant File.xlsx")
    if file.exists ():
        print("Working on SAP Applicant File")
        data = pd.read_excel("SAP Applicant File.xlsx")

        data['Concat ID'] = data['VORNA'] + data['NACHN'] + data['FMTSTREET'].str[:10]
        data.loc[data["Concat ID"].isnull(),'Concat ID'] = data["VORNA"] + data["NACHN"]

        data['Concat ID'] = data['Concat ID'].str.lower()

        SAP_data = data[['Concat ID'] + ['STUDENTSHORT'] + ['SMTP_ADDR'] + ['STUDENTSHORT'] + ['SMTP_ADDR1'] + ['STUDENTSHORT']]

        SAP_data.to_csv('modified_SAP.csv', index=False)

    else:
        print ("SAP Applicant File not exist")

    return SAP_data

def eab():
    file = Path("eab.csv")
    if file.exists ():
        data = pd.read_csv("eab.csv", encoding = "ISO-8859-1")
        print("Working on EAB Pop")

        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        data.loc[data["Concat ID"].isnull(),'Concat ID'] = data["First Name"] + data["Last Name"]

        data['Concat ID'] = data['Concat ID'].str.lower()

        eab_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]
#        eab_data.to_csv('modified_eab.csv', index=False)

    else:
        print ("EAB Marketing Pop File not exist")

    return eab_data

def marketing():
    file = Path("marketing.csv")
    if file.exists ():
        print("Working on Marketing Pop")
        data = pd.read_csv("marketing.csv", encoding = "ISO-8859-1")

        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        data.loc[data["Concat ID"].isnull(),'Concat ID'] = data["First Name"] + data["Last Name"]

        data['Concat ID'] = data['Concat ID'].str.lower()

        marketing_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]
#        marketing_data.to_csv('modified_marketing.csv', index=False)

    else:
        print ("Marketing Pop File not exist")

    return marketing_data

def prospect():
    file = Path("prospect.csv")
    if file.exists ():
        print("Working on Prospect Pop")
        data = pd.read_csv("prospect.csv", encoding = "ISO-8859-1")
        data['Concat ID'] = data['Contact: First Name'] + data['Contact: Last Name'] + data['Contact: Mailing Address Line 1'].str[:10]
        data.loc[data["Concat ID"].isnull(),'Concat ID'] = data['Contact: First Name'] + data['Contact: Last Name']

        data['Concat ID'] = data['Concat ID'].str.lower()
        data = data.rename(columns={'Contact: Contact ID': 'Contact ID', 'Contact: Email': 'Email'})

        prospect_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]
#        prospect_data.to_csv('modified_prospect.csv', index=False)

    else:
        print ("Prospect Pop File not exist")

    return prospect_data

def transfer():
    file = Path("transfer.csv")
    if file.exists ():
        data = pd.read_csv("transfer.csv", encoding = "ISO-8859-1")
        print("Working on Transfer Pop")
        data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Mailing Street'].str[:10]
        data.loc[data["Concat ID"].isnull(),'Concat ID'] = data["First Name"] + data["Last Name"]


        data['Concat ID'] = data['Concat ID'].str.lower()

        transfer_data = data[['Concat ID'] + ['Contact ID'] + ['Email'] + ['Contact ID']]
#        transfer_data.to_csv('modified_transfer.csv', index=False)

    else:
        print ("Transfer Pop File not exist")

    return transfer_data


def main():
    transfer_data = transfer()
    prospect_data = prospect()
    marketing_data = marketing()
    eab_data = eab()
    SAP_data = SAP()

    final_data = pd.concat([transfer_data, prospect_data], axis=0)
    final_data = pd.concat([final_data, marketing_data], axis=0)
    final_data = pd.concat([final_data, eab_data], axis=0)

    final_data.to_csv('ConcatLoad.csv')

    print("DONE!")

main()
