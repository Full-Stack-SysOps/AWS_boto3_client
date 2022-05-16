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


def dialer_backup():
    print("Extracting data from S3 ...")

    # query into db
    cursor = cnx.cursor()
    query = ("SELECT prefix FROM dialer_data")
    cursor.execute(query)
    dialer_list = list(itertools.chain(*cursor))

    # fetching data from s3
    for dialer in dialer_list:
        dir_name = dialer + "/mysqlbackup/"
        response_object = client.list_objects(Bucket="dialer-backup", Prefix=dir_name, Delimiter="/")
        
        for filename in response_object["Contents"]:
            if filename["LastModified"].date() not in date_list:
                date_list.append(filename["LastModified"].date())
            files[dialer] = [{"FileName": filename["Key"].split('/')[2].encode("ascii"), "TimeStamp": filename["LastModified"].date()}
                            for filename in response_object["Contents"]]

    print("Processing data into file ...")

    date_list.sort()
    for dt in date_list:
        data_file[dt] = {server_name: [val["FileName"] for val in file_list if dt == val["TimeStamp"]]
                        for server_name, file_list in files.items()}

    print("Backup Report Exported... File Location - /home/users/ldap_username")

    print("Connection Closed")
    cnx.close()

    df = pandas.DataFrame(data_file)
    df.to_csv("/home/users/aveekmukherjee/Backup_Report.csv")

def main():
    dialer_backup()

if __name__ == "__main__":
    main()
