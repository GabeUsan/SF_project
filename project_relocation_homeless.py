
import os
import re
import time
import pandas as pd
import numpy as np
from datetime import datetime as dt
from matplotlib import pyplot as plt


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

file_paths = get_all_paths('relocation_SF')
print(file_paths)

filename = 'relocation_SF/Registered_Business_Locations_San_Francisco.csv'.split('/')
print(filename)

ticker = filename[1].split('.')[0]
print(ticker)

def get_ticker_from_filepath(filepath):
    filename = filepath.split('/')[-1]
    ticker = filename.split('.')[0]
    return ticker

get_ticker_from_filepath(file_paths[0])
get_ticker_from_filepath(file_paths[1])

def get_df_from_file(filepath):
    df = pd.read_csv(filepath, low_memory=False) 
    ticker = get_ticker_from_filepath(filepath)
    df['ticker'] = ticker
    return df

#opening dataframe related to locations
business_locations = get_df_from_file(file_paths[0])
print('Business locations dataframe shape: ', business_locations.shape)
print('Business locations dataframe columns: ', business_locations.columns)

#opening dataframe related to reports
reports_311 = get_df_from_file(file_paths[1])
print('Reports 311 cases dataframe shape: ', reports_311.shape)
print('Reports 311 cases dataframe collumns: ', reports_311.columns)

#############
#WORKING ON BUSINESS LOCATIONS
business_locations = business_locations[business_locations.City == 'San Francisco']
business_locations.drop(['Ownership Name', 'Business Start Date', 'Business End Date', 'Mail Address', 'Mail City', 'Mail Zipcode', 'Mail State', 'NAICS Code', 'Parking Tax', 'Transient Occupancy Tax', 'LIC Code', 'LIC Code Description', 'Supervisor District', 'Business Corridor'], axis=1, inplace=True)
business_locations.rename(columns={'Neighborhoods - Analysis Boundaries':'Neighborhood', 'DBA Name':'Name'}, inplace=True)

print('Converting start dates with datetime....')
business_locations['Location Start Date'] = pd.to_datetime(business_locations['Location Start Date'])

print('Converting end dates with datetime....')
business_locations['Location End Date'] = pd.to_datetime(business_locations['Location End Date'])

business_locations['Days of business'] = business_locations['Location End Date'] - business_locations['Location Start Date']

def reset_index_drop (dataframe):
    dataframe.reset_index(inplace=True)
    dataframe.drop(['index'], axis=1, inplace=True)

reset_index_drop (business_locations)

business_locations['Days'] = business_locations['Days of business'] / np.timedelta64(1, 'D')
business_locations['Years'] = business_locations['Days'] / 365

business_locations.dropna(subset=['Neighborhood'], inplace=True)

reset_index_drop (business_locations)

columns_to_convert = ['Location Id', 'Business Account Number']
for c in columns_to_convert:
    business_locations[c] = business_locations[c].astype(str)

business_locations.drop_duplicates(subset='Name', keep='first', inplace=True)
business_locations.set_index('Name', inplace=True)    
    
def get_business_info(dataframe, index):
    start_date = dataframe.loc[index]['Location Start Date']
    end_date = dataframe.loc[index]['Location End Date']
    hood = dataframe.loc[index]['Neighborhood']
    single_business_dict = {'Location Start Date': start_date, 'Location End Date': end_date, 'Neighborhood': hood}
    return single_business_dict

businesses_dict = {}
start = time.time()
for business in business_locations.index:
    single_dict = get_business_info(business_locations, business)
    businesses_dict[business] = single_dict
    print(business,' done!')
end = time.time()
print(end - start, 'seconds')

SF_companies = pd.DataFrame(businesses_dict)
SF_companies = SF_companies.T




















































