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
        cursor.execute('DROP TABLE IF EXISTS players')

        print("Creating Table...")
        cursor.execute("CREATE TABLE players(name CHAR(255),team CHAR(4),g INT,pa INT,hr INT,"
                       "r INT,rbi INT,sb INT,bb_percentage DOUBLE,k_percentage DOUBLE,iso DOUBLE,"
                       "babip DOUBLE,avg DOUBLE,obp DOUBLE,slg DOUBLE,woba DOUBLE,xwoba DOUBLE,"
                       "wrc_plus INT,bsr DOUBLE,off DOUBLE,def DOUBLE,war DOUBLE,playerid INT NOT NULL,"
                       "PRIMARY KEY (playerid))")
        print("Table is created...")
        # filling in the table created with data from df -- 2021_batting_stats.csv
        for i,row in df.iterrows():
            sql = "INSERT INTO baseball.players VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")

            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)
