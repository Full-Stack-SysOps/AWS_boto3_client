#! /usr/bin/python

import boto3
import pandas
import mysql.connector
from mysql.connector import errorcode
import itertools
import datetime
import os

# variables declared
date_list = []
files = {}
data_file = {}

# boto object created
client = boto3.client('s3')
response_bucket = client.list_buckets()

# db details
config = {"user":os.environ.get(DB_USER), "password":os.environ.get(DB_SECRET), "host":os.environ.get(DB_HOST), "database":os.environ.get(DB_NAME), "raise_on_warnings":True}

# db connection
try:
        cnx = mysql.connector.connect(**config)
        print("Connected")
except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
        else:
                print(err)

def mypbx_backup():
    print("Extracting data from S3 ...")

    # query into db
    cursor = cnx.cursor()
    query = ("SELECT prefix FROM all_data")
    cursor.execute(query)
    service_list = list(itertools.chain(*cursor))

    # fetching data from s3
    for service in service_list:
        dir_name = service+"/"
        response_object = client.list_objects(Bucket="all-mysql-backup", Prefix=dir_name, Delimiter="/")
        
        for filename in response_object["Contents"]:
            print(filename["LastModified"].date())

def main():
    mypbx_backup()

if __name__== "__main__":
    main()
