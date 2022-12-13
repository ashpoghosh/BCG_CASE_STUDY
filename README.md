# BCG_CASE_STUDY

## Dataset:


Data Set folder has 6 csv files. Please use the data dictionary (attached in the mail) to understand the dataset and then develop your approach to perform below analytics. 

## Analytics: 


Application should perform below analysis and store the results for each analysis. 

Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male? 

Analysis 2: How many two wheelers are booked for crashes?  

Analysis 3: Which state has highest number of accidents in which females are involved?  

Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death 

Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style   

Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code) 

Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance 

Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data) 

## Project Structure

```bash
BCG_CASE_STUDY/
 |-- configs/
 |   |-- config.json
 |-- Data/
 |   |-- Primary_Person_use.csv
 |   |-- Restrict_use.csv
 |   |-- Units_use.csv
 |   |-- Charges_use.csv
 |   |-- Damages_use.csv
 |   |-- Endorse_use.csv
 |-- etl_job/
 |   |-- main_spark.py
 |-- output_files/
 |   |-- analysis_1/
 |   |-- analysis_2/
 |   |-- analysis_3/
 |   |-- analysis_4/
 |   |-- analysis_5/
 |   |-- analysis_6/
 |   |-- analysis_7/
 |   |-- analysis_8/
 |-- Data.zip
 |-- start_script.sh

```

## Running the ETL job

1. Get into Project Directory: ``` cd BCG_CASE_STUDY ```
2. Run the start_script ``` sh start_script.sh ``` which consists unzip of data files and creation of output_file directory and sub directories.
```bash
unzip Data.zip

mkdir -p output_files/analysis_1
mkdir -p output_files/analysis_2
mkdir -p output_files/analysis_3
mkdir -p output_files/analysis_4
mkdir -p output_files/analysis_5
mkdir -p output_files/analysis_6
mkdir -p output_files/analysis_7
mkdir -p output_files/analysis_8

spark-submit \
--master "local[*]" \
--files configs/config.json \
etl_job/main_spark.py

```
