# -*- coding: utf-8 -*-
"""
Created on Sat Jul 13 18:15:48 2024

@author: Lyndsay Heeter
"""
import pandas as pd
import geopandas as gpd
import glob 
import os


def neighbAnalyze(ny_gdf,taxis_rides,joined,nycShp):
    ny_gdf['Trip Counts'] = 0
    ny_gdf['Average Dist'] = 0
    ny_gdf['Average Fare'] = 0
    
    for i,row in ny_gdf.iterrows():
        neighb = row['NTAName']
        #Number of trips per neighborhood.
        city_counts = (taxis_rides['NTAName'] == neighb).sum()
        ny_gdf.at[i,'Trip Counts'] = city_counts
        
        if city_counts > 0:
            #Average trip distance
            av_dist = taxis_rides[taxis_rides['NTAName'] == neighb]['trip_distance'].mean()
            ny_gdf.at[i,'Average Dist'] = av_dist
        
            #Fare by neighborhood
            av_fare = joined[joined['NTAName'] == neighb]['fare_amount'].mean()
            ny_gdf.at[i,'Average Fare'] = av_fare
    
        else:
            ny_gdf.at[i,'Average Dist'] = None
            ny_gdf.at[i,'Average Fare'] = None

    #export results to shapefile and csv
    new_NTA_shp = os.path.join(os.path.dirname(nycShp),'adjusted_NTA.shp')
    NTA_csv = os.path.join(os.path.dirname(nycShp),'adjusted_NTA.csv')
    ny_gdf.to_file(new_NTA_shp,index=False)
    ny_gdf.to_csv(NTA_csv,index=False)
    print('Adjusted shapefile saved to: {}.'.format(new_NTA_shp))
    
def findPeak(taxis_rides):
    taxis_rides['pickup_datetime'] = pd.to_datetime(taxis_rides['pickup_datetime'])

    #Extract hour from timestamp
    taxis_rides['hour'] = taxis_rides['pickup_datetime'].dt.hour
    
    #Count occurrences of each hour
    hour_counts = taxis_rides['hour'].value_counts()
    
    #Find the peak hours
    peak_hours = hour_counts[hour_counts == hour_counts.max()].index.tolist()
    
    for hour in peak_hours:
        print('Peak hours were from {}:00 to {}:00.'.format(hour, hour+1))

def main():

    #get current directory
    dir_name = os.getcwd()
    
    #find NTA shp and taxi zone shp
    nycShp = os.path.join(dir_name, 'Data', 'nynta2020.shp')
    taxiShp = os.path.join(dir_name, 'Data', 'taxi_zones.shp')

    #merge all parquet files to one dataframe
    parquet_files = glob.glob(os.path.join(dir_name, 'Data','*.parquet'))

    allTaxis = pd.DataFrame()
    for file in parquet_files:
        parquet_df = pd.read_parquet(file, engine='fastparquet')
        allTaxis = pd.concat([allTaxis,parquet_df], ignore_index=True)
    allTaxis['pickup_datetime'] = allTaxis['lpep_pickup_datetime'].combine_first(allTaxis['tpep_pickup_datetime'])
    allTaxis['dropoff_datetime'] = allTaxis['lpep_dropoff_datetime'].combine_first(allTaxis['tpep_dropoff_datetime'])

    allTaxis_clean = allTaxis.fillna(0)
    
    #read in NTA and taxi zone shps
    ny_gdf = gpd.read_file(nycShp, crs ='2263')
    taxiZone_gdf = gpd.read_file(taxiShp, crs ='2263')

    #spatial join all taxi zones with the NTAs to find which taxi zone is in which NTA
    joined = gpd.sjoin(taxiZone_gdf, ny_gdf, how='left', predicate='intersects')
    taxis_zone_clean = joined.drop_duplicates(subset=['OBJECTID'],keep='first')

    #Join the taxi df with the taxi zone df to append NTANames
    merged_taxis = pd.merge(allTaxis_clean, taxis_zone_clean[['LocationID', 'NTAName']], left_on='PULocationID', right_on='LocationID', how='inner')
    # merged_taxis = gpd.GeoDataFrame(merged_df, geometry='geometry')
    
    #filter for negative fares - disputed or no charge rides
    taxis_noNegs = merged_taxis[merged_taxis['fare_amount'] > 0]
    taxis_cleaned = taxis_noNegs.drop_duplicates(subset=['VendorID','pickup_datetime'],keep='first')
    
    neighbAnalyze(ny_gdf,taxis_cleaned,merged_taxis,nycShp)
    findPeak(taxis_cleaned)
    
if __name__ == '__main__':
    main()