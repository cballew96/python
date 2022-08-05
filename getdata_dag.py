from datetime import date, timedelta
import pandas as pd
import numpy as np
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


pd.set_option('display.max_columns', None)
'''
    Downloads the latest CSV file from fangraphs on hitters splits
'''

# to get recent data need to subtract 30 days from todays date
date1 = date.today()
thirty_days_ago = date1 - timedelta(days=30)
#date1 = str(date1)

# Downloading the csv from fangraphs
# Setting the default directory location
print('THIS IS OUTSIDE THE SCRAPE METHOD')
def scrape():
    
    print('THIS IS INSIDE THE SCRAPE METHOD')
            # in order - advanced for whole season vs lefties, batted ball for whole season vs lefties
            #            advanced for last 30 days vs lefties, batted ball for last 30 days vs lefties
            #            advanced for whole season vs righties, batted ball for whole season vs righties
            #            advanced for whole season vs righties, batted ball for whole season vs righties
    links = [r'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=1&splitArrPitch=&position=B&autoPt=true&'
             r'splitTeams=false&statType=player&statgroup=2&startDate=2022-3-1&endDate=2022-11-1&players=&filter=&groupBy=season'
             r'&wxTemperature=&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1',
             
             r'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=1&splitArrPitch=&position=B&autoPt=true&splitTeams=false'
             r'&statType=player&statgroup=3&startDate=2022-3-1&endDate=2022-11-1&players=&filter=&groupBy=season&wxTemperature='
             r'&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1',
            
             fr'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=1&splitArrPitch=&position=B&autoPt=true&splitTeams=false'
             fr'&statType=player&statgroup=2&startDate={thirty_days_ago}&endDate={date1}&players=&filter=&groupBy=season&wxTemperature='
             fr'&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1',
            
             fr'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=1&splitArrPitch=&position=B&autoPt=true&splitTeams=false'
             fr'&statType=player&statgroup=3&startDate={thirty_days_ago}&endDate={date1}&players=&filter=&groupBy=season&wxTemperature='
             fr'&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1',
            
             fr'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=2&splitArrPitch=&position=B&autoPt=true&splitTeams=false'
             fr'&statType=player&statgroup=2&startDate=2022-3-1&endDate=2022-11-1&players=&filter=&groupBy=season&wxTemperature='
             fr'&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1',
             
             fr'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=2&splitArrPitch=&position=B&autoPt=true&splitTeams=false'
             fr'&statType=player&statgroup=3&startDate=2022-3-1&endDate=2022-11-1&players=&filter=&groupBy=season&wxTemperature='
             fr'&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1',
    
             fr'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=2&splitArrPitch=&position=B&autoPt=true&splitTeams=false'
             fr'&statType=player&statgroup=2&startDate={thirty_days_ago}&endDate={date1}&players=&filter=&groupBy=season&wxTemperature='
             fr'&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1',
   
             fr'https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=2&splitArrPitch=&position=B&autoPt=true&splitTeams=false'
             fr'&statType=player&statgroup=3&startDate={thirty_days_ago}&endDate={date1}&players=&filter=&groupBy=season&wxTemperature='
             fr'&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1'
            ]

    options = webdriver.ChromeOptions()

    # adding this for a possible fix to DevToolsActivePort file doesnt exist
    options.add_argument("--remote-debugging-port=9222")

    #added this to fix the chrome cant read or write to its data directory - so far it hasnt worked
    options.add_argument('user_data-dir=/mnt/c/Users/c_bal/AppData/Local/Google/Chrome Beta/User Data')


    #options.binary_location = (r"/mnt/c/Program Files/Google/Chrome Beta/Application/chrome.exe")
    options.binary_location = (r"/usr/bin/google-chrome-beta")
    prefs = {"download.default_directory": r"/home/coleballew/baseball-data-sets//"}
    options.add_experimental_option("prefs",prefs)
    driver = webdriver.Chrome('/home/coleballew/chromedriver/chromedriver_linux64/chromedriver', options=options)
     
    index = 0
    for link in links:
        
        driver.get(link)
        driver.implicitly_wait(20)
        downloadcsv = driver.find_element(By.CSS_SELECTOR, '.data-export')
        downloadcsv.click()
        time.sleep(5)
    
        path = r'/home/coleballew/baseball-data-sets//'
        filenames = os.listdir(path)

        for filename in filenames:
            os.rename(os.path.join(path, filename), os.path.join(path, filename.replace(' ', '')))
        
        # if csv is vs LHP the name, oldname should have vsLHP if the csv is vs RHP, the name should have vsRHP in it
        if index in [0,1,2,3]:
            old_name = r'/home/coleballew/baseball-data-sets/SplitsLeaderboardDatavsLHP.csv'
        else:
            old_name = r'/home/coleballew/baseball-data-sets/SplitsLeaderboardDatavsRHP.csv'
            
        if index == 0:
            new_name = fr'/home/coleballew/baseball-data-sets/season_splits_lefties_advanced{date.today()}.csv'
            os.rename(old_name, new_name)
        elif index == 1:
            new_name = fr'/home/coleballew/baseball-data-sets/season_splits_lefties_battedball{date.today()}.csv'
            os.rename(old_name, new_name)
        elif index == 2:
            new_name = fr'/home/coleballew/baseball-data-sets/thirtydays_splits_lefties_advanced{date.today()}.csv'
            os.rename(old_name, new_name)
        elif index == 3:
            new_name = fr'/home/coleballew/baseball-data-sets/thirtydays_splits_lefties_battedball{date.today()}.csv'
            os.rename(old_name, new_name)
        elif index == 4:
            new_name = fr'/home/coleballew/baseball-data-sets/season_splits_righties_advanced{date.today()}.csv'
            os.rename(old_name, new_name)
        elif index == 5:
            new_name = fr'/home/coleballew/baseball-data-sets/season_splits_righties_battedball{date.today()}.csv'
            os.rename(old_name, new_name)
        elif index == 6:
            new_name = fr'/home/coleballew/baseball-data-sets/thirtydays_splits_righties_advanced{date.today()}.csv'
            os.rename(old_name, new_name)
        elif index == 7:
            new_name = fr'/home/coleballew/baseball-data-sets/thirtydays_splits_righties_battedball{date.today()}.csv'
            os.rename(old_name, new_name)
        index += 1

    driver.close()

def clean():
    lefties_df_season_advanced = pd.read_csv(fr'/home/coleballew/baseball-data-sets/season_splits_lefties_advanced{date.today()}.csv')
    lefties_df_season_battedball = pd.read_csv(fr'/home/coleballew/baseball-data-sets/season_splits_lefties_battedball{date.today()}.csv')
    lefties_df_30days_advanced = pd.read_csv(fr'/home/coleballew/baseball-data-sets/thirtydays_splits_lefties_advanced{date.today()}.csv')
    lefties_df_30days_battedball = pd.read_csv(fr'/home/coleballew/baseball-data-sets/thirtydays_splits_lefties_battedball{date.today()}.csv')
    righties_df_season_advanced = pd.read_csv(fr'/home/coleballew/baseball-data-sets/season_splits_righties_advanced{date.today()}.csv')
    righties_df_season_battedball = pd.read_csv(fr'/home/coleballew/baseball-data-sets/season_splits_righties_battedball{date.today()}.csv')
    righties_df_30days_advanced = pd.read_csv(fr'/home/coleballew/baseball-data-sets/thirtydays_splits_righties_advanced{date.today()}.csv')
    righties_df_30days_battedball = pd.read_csv(fr'/home/coleballew/baseball-data-sets/thirtydays_splits_righties_battedball{date.today()}.csv')

	# need to join advanced and batted ball data sets together here

    lefties_season_merged = (lefties_df_season_advanced.merge(lefties_df_season_battedball, left_on='Name', right_on='Name')
                     .rename(columns={'BB%': 'BB_percent', 'K%': 'K_percent'})
                     .drop(columns=['Tm_y', 'playerId_y', 'wRAA', 'BB/K', 'Season_y', 'PA_y', 'BUH%', 'IFFB%', 'IFH%', 'Oppo%', 'Pull%', 'Cent%', 'wRC'], axis=1)
                     .set_index(['playerId_x'])
                        )

    lefties_30days_merged = (lefties_df_30days_advanced.merge(lefties_df_30days_battedball, left_on='Name', right_on='Name')
                     .rename(columns={'BB%': 'BB_percent', 'K%': 'K_percent'})
                     .drop(columns=['Tm_y', 'playerId_y', 'wRAA', 'BB/K', 'Season_y', 'PA_y', 'BUH%', 'IFFB%', 'IFH%', 'Oppo%', 'Pull%', 'Cent%', 'wRC'], axis=1)
                     .set_index(['playerId_x'])
                        )

    righties_season_merged = (righties_df_season_advanced.merge(righties_df_season_battedball, left_on='Name', right_on='Name')
                     .rename(columns={'BB%': 'BB_percent', 'K%': 'K_percent'})
                     .drop(columns=['Tm_y', 'playerId_y', 'wRAA', 'BB/K', 'Season_y', 'PA_y', 'BUH%', 'IFFB%', 'IFH%', 'Oppo%', 'Pull%', 'Cent%', 'wRC'], axis=1)
                     .set_index(['playerId_x'])
                        )

    righties_30days_merged = (righties_df_30days_advanced.merge(righties_df_30days_battedball, left_on='Name', right_on='Name')
                      .rename(columns={'BB%': 'BB_percent', 'K%': 'K_percent'})
                      .drop(columns=['Tm_y', 'playerId_y', 'wRAA', 'BB/K', 'Season_y', 'PA_y', 'BUH%', 'IFFB%', 'IFH%', 'Oppo%', 'Pull%', 'Cent%', 'wRC'], axis=1)
                      .set_index(['playerId_x'])
                        )

    lefties_season_merged.to_csv(r'/home/coleballew/baseball-data-sets/merged-data-sets/lefties_season_merged.csv')
    lefties_30days_merged.to_csv(r'/home/coleballew/baseball-data-sets/merged-data-sets/lefties_30days_merged.csv')
    righties_season_merged.to_csv(r'/home/coleballew/baseball-data-sets/merged-data-sets/righties_season_merged.csv')
    righties_30days_merged.to_csv(r'/home/coleballew/baseball-data-sets/merged-data-sets/righties_30days_merged.csv')
        
        

default_args = {
        'owner' : 'coleballew',
        'depends_on_past': False,
        'start_date': days_ago(2),
        'retries' : 1,
        'retry_delay' : timedelta(minutes=5)
        }

dag = DAG(
        'baseball-scrape',
        default_args=default_args,
        description='baseball data scape from fangraphs',
        schedule_interval=timedelta(days=1)
        )
data_scrape = PythonOperator(
        task_id = 'data-scrape',
        python_callable=scrape,
        dag=dag
        )

data_clean = PythonOperator(
	task_id = 'clean_data',
	python_callable=clean,
	dag=dag
	)

data_scrape >> data_clean
