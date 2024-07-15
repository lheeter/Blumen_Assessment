# Blumen_TakeHome NYC Taxi Trip Data Pipeline

## Prerequisites
- Python 3.x
- Required packages: pandas, geopandas, os, glob, fastparquet

## Installation

1. Clone the repository:

   `git clone https://github.com/lheeter/Blumen_Assessment.git`  
   `cd Blumen_Assessment`
   
2. Create and activate new python environment.
   
   `python -m venv myenv`

3. Install required packages:
   
   `pip install -r requirements.txt`

4. Download the data from NYC.gov (Taxi records- yellow_tripdata_2024-01.parquet, green_tripdata_2024-01.parquet)
   
   `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`  
   Place these in the Data directory with the other shapefiles.

   An image of the specific files I downloaded can be seen in the Workflow Documentation.
   
6. Run the script:  

   `python NYCTaxiTrip_Pipeline.py`

7. The script will merge the taxi trip data, clean it up, spatially join the trip records with the Taxi Zone and NTA data, compute the number of trips per neighborhood, average trip distance, and fare by neighborhood. These calculations will be output to a shapefile and CSV in the same directory as the NTA shapefile. The results can be viewed in QGIS or as a CSV with the corresponding neighborhoods.
The script will also calculate the peak hours of operation and print a statement to show these results. 

For the data listed above, I found the peak hours of operation were between 18:00 and 19:00.
