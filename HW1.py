import pandas as pd
import numpy as np
import random
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

mydata=pd.DataFrame() #Create empty DataFrame
len_mydata=1000000
mydata['fruit']=pd.Series(random.choices(['Orange', 'Grape', 'Apple', 'Banana', 'Pineapple', 'Avocado'], k=len_mydata))
mydata['colors']=pd.Series(random.choices(['Red', 'Green', 'Yellow', 'Blue'], k=len_mydata))
mydata['price']=np.random.randint(10, 100, len_mydata)
mydata.index=np.arange(1,len_mydata+1) #Index starting from 1
mydata_path='/Users/drado/GitCode/BigDataCourse/mydata.csv'
mydata=mydata.to_csv(mydata_path) #DataFrame to CSV

conn=sqlite3.connect('mydb')
c=conn.cursor()

print('hi')