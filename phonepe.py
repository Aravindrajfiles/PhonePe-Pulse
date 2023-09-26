import csv
import subprocess
import pandas as pd
import requests
import git
import json
import os
from git import Repo
import os
import pandas as pd
import numpy as np

repo_url = 'https://github.com/PhonePe/pulse.git'

# Replace with the local directory where you want to clone the repository
local_dir = '/path/to/local/directory'

try:
    # Clone the repository
    git.Repo.clone_from(repo_url, local_dir)
    print(f"Cloned into {local_dir}")
except git.GitCommandError as e:
    print(f"Error: {e}")
os.system("git clone https://github.com/phonepe/pulse.git")

##Aggregated transcations

root_dir = (r'/content/pulse/data')

# Initialize empty list to hold dictionaries of data for each JSON file
data_list = []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, '/content/pulse/data/aggregated/transaction/country/india/state')):
    state_path = os.path.join(root_dir, '/content/pulse/data/aggregated/transaction/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                            # Extract the data we're interested in
                            for transaction_data in data['data']['transactionData']:
                                # print(transaction_data)
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'Transaction_Type': transaction_data['name'],
                                    'Transaction_Count': transaction_data['paymentInstruments'][0]['count'],
                                    'Transaction_Amount': transaction_data['paymentInstruments'][0]['amount']
                                }
                                data_list.append(row_dict)
df1 = pd.DataFrame(data_list)                               

# Save the DataFrame to a CSV file named 'data.csv' in the current working directory
df1.to_csv('Aggregated transcations .csv', index=False)


##Aggregated users


root_dir = (r'/content/pulse/data')

# Initialize empty list to hold dictionaries of data for each JSON file
data_list2= []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, '/content/pulse/data/aggregated/user/country/india/state')):
    state_path = os.path.join(root_dir, '/content/pulse/data/aggregated/user/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            # data_list2.append(data)
                            try:
                              for g in data['data']['usersByDevice']:
                                # print(g)
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'brand': g['brand'],
                                    'Transaction_Count':g['count'],
                                    'percentage': g['percentage']
                                    }
                                data_list2.append(row_dict)

                            except:
                              pass                               
df2=pd.DataFrame(data_list2)

df2.to_csv('Aggregate users .csv', index=False)

##Map transcation

root_dir = (r'/content/pulse/data')

# Initialize empty list to hold dictionaries of data for each JSON file
data_list3= []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, '/content/pulse/data/map/transaction/hover/country/india/state')):
    state_path = os.path.join(root_dir, '/content/pulse/data/map/transaction/hover/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            # data_list3.append(data)
                            try:
                              for g in data['data']['hoverDataList']:
                                # print(g)
                                row_dict = {
                                    'States': state_dir,
                                    'district':g['name'],
                                    'count':g['metric'][0]['count'],
                                    'amount':g['metric'][0]['amount'],
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                            #         'brand': g['brand'],
                            #         'Transaction_Count':g['count'],
                            #         'percentage': g['percentage']
                                    }
                                data_list3.append(row_dict)

                            except:
                              pass

df3=pd.DataFrame(data_list3)

df3.to_csv('Map transcations .csv', index=False)

##Map user

root_dir = (r'/content/pulse/data')

# Initialize empty list to hold dictionaries of data for each JSON file
data_list4= []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, '/content/pulse/data/map/user/hover/country/india/state')):
    state_path = os.path.join(root_dir, '/content/pulse/data/map/user/hover/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            # data_list4.append(data)
                            try:
                              for g in data['data']['hoverData']:
                                # print(g)
                                row_dict = {
                                    'States': state_dir,
                                    'district':g,
                                    'registered_users':data['data']['hoverData'][g]['registeredUsers'],
                                    'app_open':data['data']['hoverData'][g]['appOpens'],
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                   }
                                data_list4.append(row_dict)

                            except:
                              pass


df4=pd.DataFrame(data_list4)

df4.to_csv('Map users .csv', index=False)

##Top transaction data district wise

root_dir = (r'/content/pulse/data')

# Initialize empty list to hold dictionaries of data for each JSON file
data_list5= []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, '/content/pulse/data/top/transaction/country/india/state')):
    state_path = os.path.join(root_dir, '/content/pulse/data/top/transaction/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            # data_list5.append(data)
                            try:
                              for g in data['data']['districts']:
                                # print(g)
                                row_dict = {
                                    'States': state_dir,
                                    'district':g['entityName'],
                                    'count':g['metric']['count'],
                                    'amount':g['metric']['amount'],
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                   }
                                data_list5.append(row_dict)

                            except:
                              pass


df5=pd.DataFrame(data_list5)

df5.to_csv('Top transaction data district wise .csv', index=False)

##Top transaction pincode wise

root_dir = (r'/content/pulse/data')

# Initialize empty list to hold dictionaries of data for each JSON file
data_list6= []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, '/content/pulse/data/top/transaction/country/india/state')):
    state_path = os.path.join(root_dir, '/content/pulse/data/top/transaction/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            # data_list5.append(data)
                            try:
                              for g in data['data']['pincodes']:
                                # print(g)
                                row_dict = {
                                    'States': state_dir,
                                    'pincode':g['entityName'],
                                    'count':g['metric']['count'],
                                    'amount':g['metric']['amount'],
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                   }
                                data_list6.append(row_dict)

                            except:
                              pass


df6=pd.DataFrame(data_list6)

df6.to_csv('Top transaction data pincode wise .csv', index=False)

##Top user data district wise

root_dir = (r'/content/pulse/data')

# Initialize empty list to hold dictionaries of data for each JSON file
data_list7= []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, '/content/pulse/data/top/user/country/india/state')):
    state_path = os.path.join(root_dir, '/content/pulse/data/top/user/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            # data_list5.append(data)
                            try:
                              for g in data['data']['districts']:
                                # print(g)
                                row_dict = {
                                    'States': state_dir,
                                    'Registered_users':g['registeredUsers'],
                                    'district':g['name'],

                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                   }
                                data_list7.append(row_dict)

                            except:
                              pass


df7=pd.DataFrame(data_list7)

df7.to_csv('top user data district wise .csv', index=False)

##Top user data pincode wise

root_dir = (r'/content/pulse/data')

# Initialize empty list to hold dictionaries of data for each JSON file
data_list8= []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, '/content/pulse/data/top/user/country/india/state')):
    state_path = os.path.join(root_dir, '/content/pulse/data/top/user/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            # data_list5.append(data)
                            try:
                              for g in data['data']['pincodes']:
                                # print(g)
                                row_dict = {
                                    'States': state_dir,
                                    'pincode':g['name'],
                                    'Registered_users':g['registeredUsers'],
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                   }
                                data_list8.append(row_dict)

                            except:
                              pass


df8=pd.DataFrame(data_list8)

df8.to_csv('top user data pincode wise .csv', index=False)

##To find NULL values

import mysql.connector
import pymysql

mydb = pymysql.connect(
        host= "localhost",
        user="root",
        password="root",
        autocommit=True)

mycursor=mydb.cursor()
mycursor.execute("CREATE DATABASE if not exists phonepe")
mycursor.execute("SHOW DATABASES")
mycursor.execute("USE phonepe")

def create_tables():

    #import MySQLdb as mysql
    import pandas as pd
    import pymysql
    from sqlalchemy import create_engine



    mydb = mysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="root",
        database='phonepe',
        autocommit=True)

    mycursor=mydb.cursor()
    mycursor.execute("CREATE DATABASE if not exists phonepe")
    mycursor.execute("SHOW DATABASES")
    mycursor.execute("USE phonepe")

    #created table for aggregate transcation
    Agg_trans='''CREATE TABLE if not exists Agg_trans
                (States VARCHAR(255),
                Transaction_Year int,
                Quarters int,
                Transaction_Type TEXT,
                Transaction_Count int,
                Transaction_Amount TEXT);'''

    mycursor.execute(Agg_trans)


    #created table for aggregate users
    Agg_user=("CREATE TABLE if not exists agg_users ("
                 "States VARCHAR(255),"
                 "Transaction_Year int,"
                 "Quarters int,"
                 "brand VARCHAR(255),"
                 "Transaction_Count int",
                 "percentage int"
                 ")")
    mycursor.execute(Agg_user)

    #created table for Map transcation
    Map_trans = ("CREATE TABLE IF NOT EXISTS Map_trans ("
               "States VARCHAR(255),"
               "district VARCHAR(255),"
               "count INT,"
               "amount VARCHAR(255),"
               "Transaction_Year INT,"
               "Quarters INT,"
               ")")
    mycursor.execute(Map_trans)

    #created table for Map_users
    Map_users = ("CREATE TABLE IF NOT EXISTS Map_users ("
               "States VARCHAR(255),"
               "district VARCHAR(255),"
               "registered_users	INT,"
               "app_open INT,"
               "Transaction_Year INT,"
               "Quarters INT,"
               ")")
    mycursor.execute( Map_users)

    #created table for Top transaction data district wise
    Top_trans_ds = ("CREATE TABLE IF NOT EXISTS Top_trans_ds ("
               "States VARCHAR(255),"
               "district VARCHAR(255),"
               "count	 INT,"
               "amount VARCHAR(255),"
               "Transaction_Year INT,"
               "Quarters INT,"
               ")")
    mycursor.execute( Map_users)

    #created table for Top transaction data pincode wise
    Top_trans_pn = ("CREATE TABLE IF NOT EXISTS Top_trans_pn ("
               "States VARCHAR(255),"
               "users INT,"
               "name VARCHAR(255),"
               "Transaction_Year INT,"
               "Quarters INT,"
               ")")
    mycursor.execute(Top_trans_pn)


    #created table for Top transaction data users district wise
    Top_users_ds = ("CREATE TABLE IF NOT EXISTS Top_users_ds ("
               "States VARCHAR(255),"
               "users INT,"
               "name VARCHAR(255),"
               "Transaction_Year INT,"
               "Quarters INT,"
               ")")
    mycursor.execute(Top_users_ds)

    #created table for top users data pincode wise
    Top_users_pn = ("CREATE TABLE IF NOT EXISTS Top_users_pn ("
               "States VARCHAR(255),"
               "pincode INT",
               "users INT,"
               "name VARCHAR(255),"
               "Transaction_Year INT,"
               "Quarters INT,"
               ")")
    mycursor.execute(Top_users_pn)

    