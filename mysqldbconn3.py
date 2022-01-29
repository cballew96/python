import mysql.connector as msql
from mysql.connector import Error
import pandas as pd

# df_players = pd.read_csv(r'C:\MLB_Stats\')
df_lefties = pd.read_csv(r'C:\MLB_Stats\2021_batting_stats_vs_lefties.csv')
df_righties = pd.read_csv(r'C:\MLB_Stats\2021_batting_stats_vs_righties.csv')
df_lefties['pitcherHandedness'] = 'L'
df_righties['pitcherHandedness'] = 'R'

frames = [df_lefties, df_righties]

players = pd.concat(frames, axis=0, join="outer")


# creating the database tables and filling the tables with data from the pandas dataframes

conn = msql.connect(host=,
                    database=,
                    user=,
                    password=)
if conn.is_connected():
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to the database!")

def create_db_tables():
    try:
            cursor.execute('DROP TABLE IF EXISTS vslefties')
            cursor.execute('DROP TABLE IF EXISTS vsrighties')
            cursor.execute('DROP TABLE IF EXISTS players')

            print("Creating Table...")
            cursor.execute("CREATE TABLE players(season INT, name CHAR(255),team CHAR(3),pa INT,"
                           "bb_percentage DOUBLE, k_percentage DOUBLE, bb_by_k DOUBLE, avg DOUBLE,"
                           "obp DOUBLE, slg DOUBLE, ops DOUBLE, iso DOUBLE, babip DOUBLE, wrc DOUBLE,"
                           "wraa DOUBLE, woba DOUBLE, wrc_plus DOUBLE, playerid INT NOT NULL, pitcherHandedness CHAR(2),"
                           "PRIMARY KEY (playerid,pitcherHandedness))")
            print("Table players is created...")


            for i, row in players.iterrows():
                sql = "INSERT INTO baseball.players VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                print("Record inserted")

                conn.commit()

    except Error as e:
        print("Error: ", e)

def data_collecting():

    df1 = pd.read_sql("SELECT name, playerid, season FROM  players", conn)
    #print(df1.head(5))
    visualize_data(df1)

def visualize_data(data):
    print(data.head(5))
#create_db_tables()
data_collecting()
