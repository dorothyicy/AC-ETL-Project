#import the required library
import pandas as pd
import pyodbc


#-----EXTRACT the data-----#
#improt the dataset from three CSV file
dfVaccineAB = pd.read_csv('VaccineDate_Alberta.csv')
dfCanada = pd.read_csv('covid19_canada.csv')
dfVaccineON = pd.read_csv('VaccineData_ON.csv')

print('Datasets are extracted!')

#-----TRANSFORM the data-----#
#---Create key to combine add three dataset (granularity: year week + province, merge by this two columns)---#
#Create yearweek and province for VaccineAB and VaccineON as key 
dfVaccineAB['date'] = pd.to_datetime(dfVaccineAB['date'])
dfVaccineAB['yearweek'] = dfVaccineAB['date'].apply(lambda x: f"{x.isocalendar()[0]}{x.isocalendar()[1]+1 if x.weekday() >= 6 else x.isocalendar()[1]:02d}")
dfVaccineAB['province'] = 'Alberta'

dfVaccineON['report_date'] = pd.to_datetime(dfVaccineON['report_date'])
dfVaccineON['yearweek'] = dfVaccineON['report_date'].apply(lambda x: f"{x.isocalendar()[0]}{x.isocalendar()[1]+1 if x.weekday() >= 6 else x.isocalendar()[1]:02d}")
dfVaccineON['province'] = 'Ontario'

#Create yearweek for dfCanada as key, province = 'prname' column 
dfCanada['date'] = pd.to_datetime(dfCanada['date'])
dfCanada['yearweek'] = dfCanada['date'].dt.strftime('%G%V')


#---Drop the column that not used for our data analysis/data visualization---#
#remove the unnecessary columns on Canada data
columnsRemove = ['pruid','prnameFR','reporting_week','reporting_year','update']
dfCanada = dfCanada.drop(columnsRemove, axis=1)
dfCanada = dfCanada[dfCanada['prname'] != 'Canada']

#remove the unnecessary columns on Alberta Vaccine data
columnsRemove = ['percent_pop_1_dose','percent_pop_2_doses','percent_pop_3_doses','population']
dfVaccineAB = dfVaccineAB.drop(columnsRemove, axis=1)

#remove the unnecessary columns on Ontario Vaccine data
columnsRemove = ['_id']
dfVaccineON = dfVaccineON.drop(columnsRemove, axis=1)


#---Group two Vaccine datasets by yearweek in order to merge with Canada Covid data (yearweek = week number of the year)---#
#---because the covid dataset only contains weekly cases---#
#Group the VaccineAB by date, remove the location level
dfVaccineAB = dfVaccineAB.groupby('date').agg({'yearweek': 'max', 'province': 'max',
                                                'dose_1':'sum', 'dose_2':'sum', 
                                                'dose_3':'sum','total_doses_administered':'sum'})
dfVaccineAB = dfVaccineAB.reset_index(drop=False)

#Group the VaccineAB by yearweek, remove the date level
dfVaccineAB = dfVaccineAB.groupby('yearweek').agg({'province': 'max',
                                                'dose_1':'max', 'dose_2':'max', 
                                                'dose_3':'max','total_doses_administered':'max'})
dfVaccineAB = dfVaccineAB.reset_index(drop=False)

#Group the VaccineON by yearweek, remove the date level
dfVaccineON = dfVaccineON.groupby('yearweek').agg({'province': 'max', 
                                                'total_individuals_at_least_one':'max', 
                                                'total_individuals_fully_vaccinated':'max', 
                                                'total_individuals_3doses':'max', 
                                                'total_doses_administered':'max',
                                                'previous_day_total_doses_administered':'sum', 
                                                'previous_day_at_least_one':'sum', 
                                                'previous_day_fully_vaccinated':'sum', 
                                                'previous_day_3doses':'sum'})
dfVaccineON = dfVaccineON.reset_index(drop=False)

#---Calculated the data of each week by using the total value---#
#---becuase VaccineAB only contains the total vaccine but VaccineON has both total and weekly---#
dfVaccineAB['previous_week_total_doses_administered'] = dfVaccineAB['total_doses_administered'].diff()
dfVaccineAB['previous_week_dose_1'] = dfVaccineAB['dose_1'].diff()
dfVaccineAB['previous_week_dose_2'] = dfVaccineAB['dose_2'].diff()
dfVaccineAB['previous_week_dose_3'] = dfVaccineAB['dose_3'].diff()


#---Rename the column in the dataframes for preparing the union AB+ON and merging ---#
#Rename columns in the dataframe VaccineON
dfVaccineON = dfVaccineON.rename(columns={'total_individuals_at_least_one': 'dose_1', 
                                          'total_individuals_fully_vaccinated': 'dose_2',
                                          'total_individuals_3doses':'dose_3',
                                          'previous_day_total_doses_administered':'previous_week_total_doses_administered',
                                          'previous_day_at_least_one':'previous_week_dose_1',
                                          'previous_day_fully_vaccinated':'previous_week_dose_2',
                                          'previous_day_3doses':'previous_week_dose_3'})
#Rename columns in the dataframe dfCanada
dfCanada = dfCanada.rename(columns={'prname':'province'})

#---Union and Merge all dataframes together---#
#Union the dfVaccineON and dfVaccineAB
dfVaccine_ABON = pd.concat([dfVaccineON, dfVaccineAB], axis=0)

#Merge the Vaccine dataframe and dfCanada together by using left join (this merged dataframe = Fact table)
dfMerge = pd.merge(dfCanada, dfVaccine_ABON, on=['yearweek','province'], how='left')

#---Handle the missing value of the merged dataframe---#
dfMerge.fillna(0, inplace=True)

#---Create dimension tables---#
#create date dataframe for date information (i.e. Dim_date)
dfDate = dfMerge.groupby('date').agg({'yearweek': 'max'}).reset_index()
dfDate['year'] = pd.to_datetime(dfDate['date']).dt.year
dfDate['month'] = pd.to_datetime(dfDate['date']).dt.month
dfDate['day'] = dfDate['date'].dt.day
dfDate['week'] = pd.to_datetime(dfDate['date']).dt.isocalendar().week
dfDate = dfDate[['date', 'year', 'month','day','week', 'yearweek']]

#create location data for region information (i.e. Dim_region)
provinces = dfMerge['province'].unique()

#Remove the additional date information in the fact table
dfMerge.drop('yearweek', axis=1,inplace=True)

print('Data is transformed!')

#-----LOAD to SQL Server Database-----#
#tables tbl_Date/tbl_fact_CanadaCovid/tbl_Region are already created in the SQL Server
server = 'GoombaFish\SQLEXPRESS' 
database = 'ETLProject' 
username = 'Login' 
password = '123' 
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

cursor = cnxn.cursor()

cursor.execute("DELETE FROM tbl_date")
cursor.execute("DELETE FROM tbl_region")
cursor.execute("DELETE FROM tbl_fact_CanadaCovid")

#insert the data into dim_date (aka tbl_date)
for index,row in dfDate.iterrows():
    cursor.execute("INSERT INTO tbl_date (fulldate, year, month, day, week, yearweek) VALUES (?, ?, ?, ?, ?, ?)",
                   row['date'], row['year'], row['month'], row['day'], row['week'], row['yearweek'])
    cursor.commit()

#insert the region into dim_region (aka tbl_region)
for province in provinces:
    cursor.execute(f"INSERT INTO tbl_region (region, country) VALUES ('{province}', 'Canada')")

#insert the numeric values into fact table (aka tbl_fact_CanadaCovid)
for i, row in dfMerge.iterrows():
    query = f"INSERT INTO tbl_fact_CanadaCovid (province, date, totalcases, numtotal_last7, ratecases_total, numdeaths, numdeaths_last7, ratedeaths, ratecases_last7, ratedeaths_last7, numtotal_last14, numdeaths_last14, ratetotal_last14, ratedeaths_last14, avgcases_last7, avgincidence_last7, avgdeaths_last7, avgratedeaths_last7, dose_1, dose_2, dose_3, total_doses_administered, previous_week_total_doses_administered, previous_week_dose_1, previous_week_dose_2, previous_week_dose_3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    values = (row['province'], row['date'], row['totalcases'], row['numtotal_last7'], row['ratecases_total'], row['numdeaths'], row['numdeaths_last7'], row['ratedeaths'], row['ratecases_last7'], row['ratedeaths_last7'], row['numtotal_last14'], row['numdeaths_last14'], row['ratetotal_last14'], row['ratedeaths_last14'], row['avgcases_last7'], row['avgincidence_last7'], row['avgdeaths_last7'], row['avgratedeaths_last7'], row['dose_1'], row['dose_2'], row['dose_3'], row['total_doses_administered'], row['previous_week_total_doses_administered'], row['previous_week_dose_1'], row['previous_week_dose_2'], row['previous_week_dose_3'])
    cursor.execute(query, values)

cursor.commit()
cursor.close()

# Close the database connection
cnxn.close()

print('Data is loaded into SQL Server!')
print('End of the ETL process')
