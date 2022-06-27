from selenium import webdriver
#from selenium.webdriver.common.by import By
import time

'''
    Downloads the latest CSV file from fangraphs on hitters splits
'''



# Downloading the csv from fangraphs
# Setting the default directory location

def scrape():
    options = webdriver.ChromeOptions()
    download_location = {"download.default_directory": "C:\data sets"}
    options.add_experimental_option("prefs",download_location)


    driver = webdriver.Chrome('C:\chromedriver\chromedriver.exe', chrome_options=options)
    driver.get("https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=&splitArrPitch=&position=B&autoPt=true&splitTeams=false"
               "&statType=player&statgroup=2&startDate=2022-03-01&endDate=2022-11-01&players=&filter=&groupBy=season&wxTemperature=&wx"
               "Pressure=&wxAirDensity=&wxElevation=&wxWindSpeed=&sort=22,1&pageitems=10000000000000&pg=0")

    downloadcsv = driver.find_element_by_css_selector('.data-export')
    downloadcsv.click()
    time.sleep(5)

    driver.close()

