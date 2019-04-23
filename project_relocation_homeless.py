import os
import re
import time
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta, date
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
reports_311['CaseID'] = reports_311['CaseID'].astype(str)
reports_311.drop(['Closed', 'Updated', 'Status', 'Status Notes', 'Responsible Agency', 'Request Type', 'Request Details', 'Address', 'Street', 'Supervisor District', 'Police District', 'Latitude', 'Longitude', 'Point', 'Source', 'Media URL', 'ticker'], axis=1, inplace=True)

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

for business in business_locations.index:
    single_dict = get_business_info(business_locations, business)
    businesses_dict[business] = single_dict
    print(business,' added!')

SF_companies = pd.DataFrame(businesses_dict)
SF_companies = SF_companies.T
SF_companies['Location End Date'].fillna(0, inplace=True)


def get_end_date (dataframe, business_name):
    date_ = dataframe['Location End Date'].loc[business_name]
    return date_

for company in SF_companies.index:
    end_date = get_end_date(SF_companies, company)
    if end_date == 0:
        SF_companies['Location End Date'].loc[company] = date.today()
        print('Date converted for {} ...'.format(company))

for company in SF_companies.index:
    end_date = get_end_date(SF_companies, company) 
    SF_companies['Location End Date'].loc[company] = dt.combine(end_date, dt.min.time())
    print('Date converted for {} ...'.format(company))


#def daterange(date1, date2):
#    for n in range(int ((date2 - date1).days)+1):
#        yield date1 + timedelta(n)
#
#list_years=[]
#for company in SF_companies.index:
#    start_date = SF_companies['Location Start Date'].loc[company]
#    end_date = SF_companies['Location End Date'].loc[company]
#    for date_ in daterange(start_date, end_date):
#        list_years.append(dt.strftime("%Y"))



#############
#WORKING ON REPORTS 311
homeless_concerns_df = reports_311[reports_311.Category == 'Homeless Concerns']
homeless_concerns_df.set_index('CaseID', inplace=True)
homeless_concerns_df['Opened'] = pd.to_datetime(homeless_concerns_df['Opened'])
print('The least recent case was opened on this date: {}'.format(homeless_concerns_df['Opened'].min()))

encampents_df = reports_311[reports_311.Category == 'Encampments']
encampents_df.set_index('CaseID', inplace=True)
encampents_df['Opened'] = pd.to_datetime(encampents_df['Opened'])
print('The least recent case was opened on this date: {}'.format(encampents_df['Opened'].min()))

least_recent_case = encampents_df['Opened'].min()

dataframes = [homeless_concerns_df, encampents_df]

homeless_reports = pd.concat(dataframes)
    
#############
#BACK TO FIRST DATATSET
# I need to drop all business observations with end date prior to Julie 7th 2009
SF_companies = SF_companies[SF_companies['Location End Date'] > least_recent_case]

date_for_df = '2019-01-01'
date_filter = dt.strptime(date_for_df, '%Y-%m-%d')
SF_companies = SF_companies[SF_companies['Location End Date'] < date_filter]


#############
#Quickly on homeless_reports
homeless_reports = homeless_reports[homeless_reports['Opened'] < date_filter]


############
#Working on BOTH

neighborhoods_SF_companies = []
for hood in SF_companies['Neighborhood']:
    neighborhoods_SF_companies.append(hood)
neighborhoods_SF_companies = set(neighborhoods_SF_companies)

neighborhoods_homeless_reports = []
for hood in homeless_reports['Neighborhood']:
    neighborhoods_homeless_reports.append(hood)
neighborhoods_homeless_reports = set(neighborhoods_homeless_reports)

common_hoods = set.intersection(neighborhoods_SF_companies, neighborhoods_homeless_reports)

