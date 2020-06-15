import pandas as pd
from pathlib import *

def OPI_data(data):
    data['Concat ID'] = data['First Name'] + data['Last Name'] + data['Address Line 1'].str[:10]
    data = data[['Entry Term'] + ['Submitted Date'] + ['Admission Decision'] + ['Concat ID']]
    data['Admission Decision'] = data['Admission Decision'].fillna('App Started')
    data = data.rename(columns={'Entry Term' : 'AY_Entry_Term' , 'Submitted Date' : 'Application Date' , 'Admission Decision' : 'Application Status', 'Address Line 2' : 'Other Street', 'Concat ID':'Concat Short'})
    return data


def rename(data):
    data = data.rename(columns={'E-mail': "EMAIL", 'Address Line 1' : "MAILING STREET", 'City' : 'MAILING CITY', 'State/Province': 'MAILINGSTATE', 'Zip' : 'MAILINGPOSTALCODE', 'DOB': 'BIRTHDATE'})
    return data

def data_clean(load_data):
    load_data = load_data[['First Name'] + ['Last Name'] + ['E-mail'] + ['Address Line 1'] + ['Address Line 2'] + ['City'] + ['State/Province'] + ['Country'] + ['Zip'] + ['DOB'] + ['Gender'] + ['Phone']]
    load_data['Concat ID'] = load_data['First Name'] + load_data['Last Name'] + load_data['Address Line 1'].str[:10]
    load_data['Concat ID'] = load_data['Concat ID'].str.lower()
    load_data['First Name'] = load_data['First Name'].str.title()
    load_data['Last Name'] = load_data['Last Name'].str.title()
    load_data['Address Line 1'] = load_data['Address Line 1'].str.title()
    load_data['City'] = load_data['City'].str.title()
    load_data['Gender'] = load_data['Gender'].map({'1': 'Male', '2': 'Female'})

    return load_data

def imports():
    file = Path("Online_programs_for_SF_06_11_2020_12.00.22_PM2.csv")
    if file.exists ():
        data = pd.read_csv("Online_programs_for_SF_06_11_2020_12.00.22_PM2.csv", encoding = "ISO-8859-1")
    else:
        print("UK Online file not found")

    return data

def main():
    online_data = imports()
    clean_contact_data = data_clean(online_data)
    contact_data_final = rename(clean_contact_data)
    OPI_data_final = OPI_data(online_data)
#    print(contact_data_final.head())
#    print(OPI_data_final.head())

    OPI_data_final.to_csv('UKOnline_OPI_final.csv')
    contact_data_final.to_csv('UKOnline_Contact_final.csv')

main()
