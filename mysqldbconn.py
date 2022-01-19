import mysql.connector as msql
from mysql.connector import Error
import pandas as pd


df = pd.read_csv(r'C:\MLB_Stats\2021_batting_stats.csv')
first_column = df.iloc[:,0]


try:
    conn = msql.connect(host='',
                        database='',
                        user='',
                        password='')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to the database!")

except Error as e:
            print("Error while connecting to MySQL", e)
