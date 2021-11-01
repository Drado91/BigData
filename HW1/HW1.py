import pandas as pd
import numpy as np
import random
import sqlite3
from sqlite3 import Error
import pathlib
import pyarrow.parquet as pq
from pyarrow import csv
import dask.dataframe as dd
import math
import csv


#General Task:
def createCSV(len_mydata):
    #Create DF by instructions:
    mydata=pd.DataFrame() #Create empty DataFrame
    mydata['fruit']=pd.Series(random.choices(['Orange', 'Grape', 'Apple', 'Banana', 'Pineapple', 'Avocado'], k=len_mydata))
    mydata['colors']=pd.Series(random.choices(['Red', 'Green', 'Yellow', 'Blue'], k=len_mydata))
    mydata['price']=np.random.randint(10, 100, len_mydata)
    mydata.index=np.arange(1,len_mydata+1) #Index starting from 1
    currentDir = pathlib.Path(__file__).parent.resolve()
    mydata_path = str(currentDir) + '/mydata.csv'
    mydata.to_csv(mydata_path, index_label='index')  # DataFrame to CSV
    return mydata_path


#Task 1: CSV and SQL
def loadCSVtoSQL(dbPath,dbName,tableName):
    mydata = pd.read_csv(dbPath,index_col=False)
    conn=sqlite3.connect(dbName)
    c=conn.cursor()
    mydata.to_sql(tableName, conn, if_exists='replace', index=False)
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(c.fetchall())
    return c

def queryExecute(query,c):
    c.execute(query)
    for row in c.fetchall():
        print(row)

dbName = 'mydb'
tableName = 'mydata'
query1 = "SELECT * FROM mydata WHERE fruit = 'Orange' AND colors = 'Red' AND price < 13"
query2 = "SELECT * FROM mydata WHERE fruit = 'Apple' AND colors = 'Green' AND price > 20 AND price < 25"
len_mydata = 1000000


dbPath=createCSV(len_mydata)
#T1.1,2
dbCursor=loadCSVtoSQL(dbPath,dbName,tableName)
#T1.3
queryExecute(query1,dbCursor)

#Task 2: CSV and Parquet

#T2.1
from pyarrow import csv
f=pd.read_csv(dbPath)
print("number of lines in mydata.csv is : " +str(f.shape[0]))

def csv2ParqutePyarrow(dbPath):
    table = csv.read_csv(dbPath)
    pq.write_table(table,'mydatapyarrow.parquet')

def csv2ParquetDask(dbPath):
    df = dd.read_csv(dbPath)
    df.to_parquet('mydatadask.parquet', write_index=False)

def csv2ParquetPandas(dbPath):
    df = pd.read_csv(dbPath)
    df.to_parquet('mydatapandas.parquet')

import time
#T2.2
start_time=time.time()
csv2ParqutePyarrow(dbPath)
print("--- %s seconds ---" % (time.time() - start_time))

#T2.3
start_time=time.time()
csv2ParquetDask(dbPath)
print("--- %s seconds ---" % (time.time() - start_time))

#T2.4
start_time=time.time()
csv2ParquetPandas(dbPath)
print("--- %s seconds ---" % (time.time() - start_time))

#Task 3: Split CSV files
def calcCharsFromCSV(dbPath):
    char_cnt=0
    with open(dbPath, 'r') as fl:
        for word in fl:
            for ch in word:
                char_cnt+=1
    return char_cnt

#T3.1
CSV_Char_Size=calcCharsFromCSV(dbPath)
middle=int(CSV_Char_Size/2)

#T3.2
def first_chunk(dbPath,middle):
    f1 = open(dbPath, "rb")
    f1.seek(0)
    fn = f1.read(middle).decode(encoding='utf-8')
    num_lines=fn.count('\n')
    print('number of rows in first chunk is: ' + str(num_lines))
    return fn,num_lines

def last_chunk(dbPath,middle):
    f2 = open(dbPath, "rb")
    f2.seek(middle+1,0)
    f2n =f2.read().decode(encoding='utf-8')
    num_lines = f2n.count('\n')
    print('number of rows in last chunk is: ' + str(num_lines))
    return f2n,num_lines

f_chunk,f_number_line=first_chunk(dbPath,middle)
l_chunk,l_number_line=last_chunk(dbPath,middle)
print('Total num of lines is: ' + str(f_number_line+l_number_line))

#T3.3
"""
When splitting CSV and splitting point size defined as a byte,
probably splitting point location will be somewhere in line and probably not at the end of line.
in this case splitting line appears in both chunks. this line counts twice.

When counting lines without splitting, each line counts once as in T2.1
"""

#T3.4
"""
The solution is to set splitting point at the end of line:
"""
def calcCharsFromCSV_2(dbPath):
    char_cnt=0
    with open(dbPath, 'r') as fl:
        for word in fl:
            for ch in word:
                char_cnt+=1
    x = math.floor(char_cnt / 2)
    with open(dbPath,'r') as fl:
        fl.seek(x, 0)
        for word in fl:
            for ch in word:
                if ch == '\n':
                    break
                x += 1
            break
    return x

new_middle=calcCharsFromCSV_2(dbPath)
f_chunk_new,f_number_line_new=first_chunk(dbPath,new_middle)
l_chunk_new,l_number_line_new=last_chunk(dbPath,new_middle)
print('Total num of lines is: ' + str(f_number_line_new+l_number_line_new))

#Task 3.5
chunk_size=math.ceil(16e6)
def chunks_16MB(dbPath,chunk_size):
    char_cnt=0
    with open(dbPath, 'r') as fl:
        for word in fl:
            for ch in word:
                char_cnt+=1
    num_of_chunks=math.ceil(char_cnt/chunk_size)
    seeker1=chunk_size
    seeker0=0
    ls=[]
    for i in range(num_of_chunks):
        f = open(dbPath, "rb")
        f.seek(seeker0+seeker1,0)
        for line in f:
            for char in line:
                if char == '\n':
                    break
                seeker1+=1
            break
        f.seek(seeker0,0)
        ls.append(f.read(seeker1).decode(encoding='utf-8'))
        seeker0+=seeker1
        seeker1=chunk_size
    f.seek(seeker0,0)
    ls.append(f.read().decode(encoding='utf-8'))
    print('Data has been divided to ' +str(num_of_chunks) + ' chunks, each size is ' + str(chunk_size) + ' MB')
    return ls
mydata_chunks=chunks_16MB(dbPath,chunk_size)

print('finish')