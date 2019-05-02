print('importing libraries...')
import os
import re
import time
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta, date
from matplotlib import pyplot as plt

print('defining functions...')

def get_file_paths (folder, full_path=True):
    file_paths = []
    for (dirpath, dirnames, filenames) in os.walk(folder):
        for file_ in filenames:
            if not re.match('\.', file_):
                if full_path:
                    file_paths.append(os.path.join(dirpath, file_))
                else:
                    file_paths.append((dirpath, file_))                     
    return file_paths

def get_all_paths(subfolder):
    folder = os.path.join(subfolder)
    files = get_file_paths(folder)
    return files

def get_df_from_file(filepath):
    df = pd.read_csv(filepath, low_memory=False) 
    return df

def set_type_str(dataframe, column):
    dataframe[column] = dataframe[column].astype(str)
    
def reset_index_drop(dataframe):
    dataframe.reset_index(drop= True, inplace=True)
    
def set_index(dataframe, column):
    dataframe.set_index(column, inplace=True)
        
def check_if_business_operated_year(dataframe, start, end, year):
    test_year = (dataframe[start].dt.year <= year) & ((dataframe[end].dt.year >= year) | pd.isnull(dataframe[end]))
    return test_year

file_paths = get_all_paths('relocation_SF')

print('Opening first dataframe...')
business_locations = get_df_from_file(file_paths[0])

print('Opening second dataframe...')
reports_311 = get_df_from_file(file_paths[1])
reports_311['CaseID'] = reports_311['CaseID'].astype(str)
reports_311.drop(['Closed', 'Updated', 'Status', 'Status Notes', 'Responsible Agency', 'Request Type', 'Request Details', 'Address', 'Street', 'Supervisor District', 'Police District', 'Latitude', 'Longitude', 'Point', 'Source', 'Media URL'], axis=1, inplace=True)

#############
print('preparing business locations dataset...')
business_locations = business_locations[business_locations.City == 'San Francisco']
business_locations.drop(['Ownership Name', 'State', 'Business Location', 'Mail Address', 'Mail City', 'Mail Zipcode', 'Mail State', 'NAICS Code', 'Parking Tax', 'Transient Occupancy Tax', 'LIC Code', 'LIC Code Description', 'Supervisor District', 'Business Corridor', 'Source Zipcode'], axis=1, inplace=True)
business_locations.rename(columns={'Neighborhoods - Analysis Boundaries':'Neighborhood', 'DBA Name':'Business_Name'}, inplace=True)

set_type_str(business_locations, 'Location Id')
set_type_str(business_locations, 'Business Account Number')

business_locations.dropna(subset=['Neighborhood'], inplace=True)

reset_index_drop (business_locations)
set_index(business_locations, 'Location Id')

print('Converting start dates with datetime....')
business_locations['Location Start Date'] = pd.to_datetime(business_locations['Location Start Date'])

print('Converting end dates with datetime....')
business_locations['Location End Date'] = pd.to_datetime(business_locations['Location End Date'])

for year in range(2008,2020):
    business_locations[year] = check_if_business_operated_year(business_locations, 'Location Start Date', 'Location End Date', year)



operating_2013 = len(business_locations[business_locations[2013]==True])
closed_2013 = len(business_locations[(business_locations[2013]==False) & (business_locations[2012]==True)])
percentage_2013 = closed_2013 / operating_2013
print('{:.2%}'.format(percentage_2013))

percentage_closures = {}
for year in range(2009,2019):
    operating = len(business_locations[business_locations[year]==True])
    closed = len(business_locations[(business_locations[year]==False) & (business_locations[year-1]==True)])
    percentage = closed / operating
    percentage_closures[year]=round(percentage*100, 2)
percentage_closures

percentage_closures_df = pd.DataFrame.from_dict(percentage_closures, orient='index', columns=['percentage'])

percentage_closures_df.plot(kind='bar')
