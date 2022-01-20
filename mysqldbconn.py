import mysql.connector as msql
from mysql.connector import Error
import pandas as pd

#df_players = pd.read_csv(r'C:\MLB_Stats\')
df_lefties = pd.read_csv(r'C:\MLB_Stats\2021_batting_stats_vs_lefties.csv')
df_righties = pd.read_csv(r'C:\MLB_Stats\2021_batting_stats_vs_righties.csv')

# in vslefties and vsrights df the col SEASON, NAME, AND TM need to be removed before the rest of the code works


# creating the database tables and filling the tables with data from the pandas dataframes
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
        cursor.execute('DROP TABLE IF EXISTS vslefties')
        cursor.execute('DROP TABLE IF EXISTS vsrighties')

        print("Creating Table...")
        cursor.execute("CREATE TABLE players(season INT, name CHAR(255),team CHAR(3),playerid INT NOT NULL,"
                       "PRIMARY KEY (playerid))")
        print("Table players is created...")
        print("Creating Table...")
        cursor.execute("CREATE TABLE vslefties(g INT,pa INT,hr INT,"
                       "r INT,rbi INT,sb INT,bb_percentage DOUBLE,k_percentage DOUBLE,iso DOUBLE,"
                       "babip DOUBLE,avg DOUBLE,obp DOUBLE,slg DOUBLE,woba DOUBLE,xwoba DOUBLE,"
                       "wrc_plus INT,bsr DOUBLE,off DOUBLE,def DOUBLE,war DOUBLE,playerid INT NOT NULL,"
                       "PRIMARY KEY (playerid))")
        print("Table vslefties is created...")
        print("Creating Table")
        cursor.execute("CREATE TABLE vsrighties(g INT,pa INT,hr INT,"
                       "r INT,rbi INT,sb INT,bb_percentage DOUBLE,k_percentage DOUBLE,iso DOUBLE,"
                       "babip DOUBLE,avg DOUBLE,obp DOUBLE,slg DOUBLE,woba DOUBLE,xwoba DOUBLE,"
                       "wrc_plus INT,bsr DOUBLE,off DOUBLE,def DOUBLE,war DOUBLE,playerid INT NOT NULL,"
                       "PRIMARY KEY (playerid))")
        print("Table vsrighties is created...")
        '''
        for i, row in df_players.iterrows():
            sql = "INSERT INTO baseball.vsrighties VALUES (%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
        '''
        for i,row in df_lefties.iterrows():
            sql = "INSERT INTO baseball.vslefties VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
        for i, row in df_righties.iterrows():
            sql = "INSERT INTO baseball.vsrighties VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")

            conn.commit()
except Error as e:
            print("Error: ", e)
