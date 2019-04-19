#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 22:52:17 2019

@author: gabrieleusan
"""

import os
import re
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
file_paths

filename = 'relocation_SF/Registered_Business_Locations_San_Francisco.csv'.split('/')
print(filename)

ticker = filename[1].split('.')[0]
ticker

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
business_locations.shape
business_locations.columns

#opening dataframe related to reports
reports_SF = get_df_from_file(file_paths[1])
reports_SF.shape
reports_SF.columns


business_locations = business_locations[business_locations.City == 'San Francisco']
business_locations.shape

business_locations.drop(['Ownership Name', 'Business Start Date', 'Business End Date', 'Mail Address', 'Mail City', 'Mail Zipcode', 'Mail State', 'NAICS Code', 'Parking Tax', 'Transient Occupancy Tax', 'LIC Code', 'LIC Code Description', 'Supervisor District', 'Business Corridor'], axis=1, inplace=True)
business_locations.shape

business_locations = business_locations.rename(columns={'Neighborhoods - Analysis Boundaries':'Neighborhood'})

business_locations['Location Start Date'] = pd.to_datetime(business_locations['Location Start Date'])
business_locations['Location End Date'] = pd.to_datetime(business_locations['Location End Date'])

#calculating days of business in a location. NaT occurs when there is no end date so I need to understand if those businesses are still functioning or the information is simply missing
business_locations['Days of business'] = business_locations['Location End Date'] - business_locations['Location Start Date']

business_locations.reset_index(inplace=True)

business_locations.drop(['index'], axis=1, inplace=True) #dropping the column

#converting days of businesses to floats as a numbpy series
business_locations['Days'] = business_locations['Days of business'] / np.timedelta64(1, 'D')
business_locations['Years'] = business_locations['Days'] / 365

business_ended = business_locations[business_locations.Days > 0]
business_ended.dropna(subset=['Neighborhood'], inplace=True)

list_neighborhoods = []
for hood in set(business_ended['Neighborhood']):
    list_neighborhoods.append(hood)
    
business_end_group = business_ended.groupby('Neighborhood')['Years'].mean()

def convert_series_to_df (series):
    new_df = series.to_frame()
    return new_df

neighborhood_avg_years = convert_series_to_df (business_end_group)
neighborhood_avg_years = neighborhood_avg_years.round(2)
#neighborhood_avg_years.reset_index(level=0, inplace=True) #to reset index and have neighborhood as column

avg_years_chart = neighborhood_avg_years.plot(figsize=(20,10),kind='bar')
plt.suptitle('Average years in each Neighborhood', fontsize=24)

count_businesses = business_ended.groupby('Neighborhood').size()
count_businesses_df = convert_series_to_df (count_businesses)
count_businesses_df.columns = ['Total number of businesses']

count_business_chart = count_businesses_df.plot(figsize=(20,10),kind='bar')
plt.suptitle('Total number of businesses', fontsize=24)


main_relocation_df = neighborhood_avg_years
main_relocation_df['Total number of businesses'] = count_businesses_df['Total number of businesses']


std_years = business_ended.groupby('Neighborhood')['Years'].std()
std_years_df = convert_series_to_df(std_years)
std_years_df.columns = ['Standard deviation (years)']
main_relocation_df['Standard deviation (years)'] = std_years_df['Standard deviation (years)']


main_relocation_df['Years'].plot(figsize=(12,8), kind='barh')
plt.suptitle('Average Years in each Neighborhood', fontsize=20)

main_relocation_df['Total number of businesses'].plot(figsize=(12,8), kind='barh')
plt.suptitle('Total number of businesses', fontsize=20)

main_relocation_df['Standard deviation (years)'].plot(figsize=(12,8), kind='barh')
plt.suptitle('Years Standard Deviation', fontsize=20)


#CLEANING SECOND DATASET
homeless_concerns_df = reports_SF[reports_SF.Category == 'Homeless Concerns']
encampents_df = reports_SF[reports_SF.Category == 'Encampments']



#homeless_report_dict = {}
#
#for hood in list_neighborhoods:
#    homeless_report_dict[hood] = []
#    
#def get_reports(dataframe):
#    for hood in 




















































