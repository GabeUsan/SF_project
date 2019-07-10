print('importing libraries...')
import os
import re
import time
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta, date
from matplotlib import pyplot as plt
import time
import statsmodels.formula.api as sm


print('defining functions...')
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

def check_if_business_opened_year(dataframe, start, year):
    test_year = dataframe[start].dt.year == year
    return test_year

def merge_datasets(df1, df2):
    merge = df1.merge(df2, how='left', left_index=True, right_index=True)
    return merge


print('Opening business locations dataframe...')
business_locations = get_df_from_file('/Users/gabrieleusan/Documents/Python class/datasets_for_project/Registered_Business_Locations_San_Francisco.csv')

print('working on business locations data...')
business_locations = business_locations[business_locations.City == 'San Francisco']
business_locations.drop(['Business Account Number','Street Address','Business Start Date','Neighborhoods - Analysis Boundaries','Business End Date','Ownership Name', 'State', 'Business Location', 'Mail Address', 'Mail City', 'Mail Zipcode', 'Mail State', 'NAICS Code', 'Parking Tax', 'Transient Occupancy Tax', 'LIC Code', 'LIC Code Description', 'Supervisor District', 'Business Corridor', 'Source Zipcode'], axis=1, inplace=True)
business_locations.rename(columns={'DBA Name':'Business_Name', 'NAICS Code Description':'Industry'}, inplace=True)
set_type_str(business_locations, 'Location Id')
reset_index_drop (business_locations)
set_index(business_locations, 'Location Id')

print('Converting start dates with datetime....')
business_locations['Location Start Date'] = pd.to_datetime(business_locations['Location Start Date'])
print('Converting end dates with datetime....')
business_locations['Location End Date'] = pd.to_datetime(business_locations['Location End Date'])

print('Checking businesses operatiting in SF between 2009 and 2018')
for year in range(2008,2019):
    business_locations[year] = check_if_business_operated_year(business_locations, 'Location Start Date', 'Location End Date', year)

print('Calculating closure percentages between 2009 and 2018')
percentage_closures_SF = {}
for year in range(2009,2019):
    operating = len(business_locations[business_locations[year]==True])
    closed = len(business_locations[(business_locations[year]==False) & (business_locations[year-1]==True)])
    percentage = closed / operating
    percentage_closures_SF[year]=round(percentage*100, 2)

closures = pd.DataFrame.from_dict(percentage_closures_SF, orient='index', columns=['closure_ptg'])
closures.plot(kind='bar')


print('Working on evictions data...')
evictions = get_df_from_file('/Users/gabrieleusan/Documents/Python class/datasets_for_project/Count_of_Eviction_Notices_By_Analysis_Neighborhood_and_Year.csv')
evictions.rename(columns={'File Year':'Year', 'Count of Eviction Notices':'Evictions'}, inplace=True)
evictions['Year'] = pd.to_datetime(evictions['Year'])
evictions['Year'] = evictions['Year'].dt.year
evictions_df = evictions.groupby(['Year']).sum()
evictions_df.drop([1997,1998,1999,2000,2001,2002, 2003, 2004, 2005, 2006, 2007, 2008,2019], axis=0, inplace=True)
evictions_df.plot(kind='bar')

#################################################
print('Opening third dataframe...')
reports_311 = get_df_from_file('/Users/gabrieleusan/Documents/Python class/datasets_for_project/311_Cases.csv')
reports_311['CaseID'] = reports_311['CaseID'].astype(str)
reports_311.drop(['Closed', 'Updated', 'Status', 'Status Notes', 'Responsible Agency', 'Request Details', 'Street', 'Supervisor District', 'Police District', 'Latitude', 'Longitude', 'Point', 'Source', 'Media URL'], axis=1, inplace=True)
reports_311.rename(columns={'Request Type':'Type'}, inplace=True)

#################################################
print('Working on human waste data...')
human_waste = reports_311[reports_311.Type == 'Human Waste']
human_waste.rename(columns={'Opened':'Year'}, inplace=True)
human_waste['Year'] = pd.to_datetime(human_waste['Year']).dt.date
human_waste['Year'] = pd.to_datetime(human_waste['Year'])
human_waste['Year'] = human_waste['Year'].dt.year
human_waste.drop_duplicates(subset=['Address'], keep='first', inplace=True)
group_human_waste = human_waste.groupby('Year').size()
human_waste_df = pd.DataFrame([group_human_waste])
human_waste_df = human_waste_df.T
human_waste_df.drop([2008,2019], axis=0, inplace=True)
human_waste_df.rename(columns={0:'human_waste'}, inplace=True)
human_waste_df.plot(kind='bar')


#################################################
print('Working on medical waste data...')
medical_waste = reports_311[reports_311.Type == 'Medical Waste']
medical_waste.rename(columns={'Opened':'Year'}, inplace=True)
medical_waste['Year'] = pd.to_datetime(medical_waste['Year']).dt.date
medical_waste['Year'] = pd.to_datetime(medical_waste['Year'])
medical_waste['Year'] = medical_waste['Year'].dt.year
medical_waste.drop_duplicates(subset=['Address'], keep='first', inplace=True)
group_medical_waste = medical_waste.groupby('Year').size()
medical_waste_df = pd.DataFrame([group_medical_waste])
medical_waste_df = medical_waste_df.T
medical_waste_df.drop([2008,2019], axis=0, inplace=True)
medical_waste_df.rename(columns={0:'medical_waste'}, inplace=True)
medical_waste_df.plot(kind='bar')

list_of_dfs = [evictions_df, human_waste_df, medical_waste_df]
for df in list_of_dfs:
    closures = merge_datasets(closures, df)
    
    
for col in closures.columns:
    closures['log'+col] = np.log(closures[col])

model = sm.ols(formula='closure_ptg ~ Evictions + medical_waste', data=closures).fit()
print(model.summary())

model = sm.ols(formula='closure_ptg ~ Evictions + human_waste + medical_waste', data=closures).fit()
print(model.summary())

model = sm.ols(formula='closure_ptg ~ logEvictions + loghuman_waste + logmedical_waste', data=closures).fit()
print(model.summary())

regression = sm.ols(formula='closure_ptg ~ logEvictions + logmedical_waste', data=closures).fit()
print(regression.summary())

closures.to_csv(path_or_buf='/Users/gabrieleusan/Documents/Python class/datasets_for_project/data_presentation.csv')
