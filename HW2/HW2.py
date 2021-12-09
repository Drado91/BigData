import csv
import os
import random
import warnings
import numpy as np
import scipy as sp
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3
import threading
import time
import csv

warnings.filterwarnings('ignore')
random.seed(123)

#Q1
firstname = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
secondname = ['Mayfield', 'Redding', 'Charles', 'Brown', 'Gaye', 'Simone', 'Wonder', 'Heron', 'Franklin']
city = ['New York', 'Haifa', 'München', 'London', 'Palo Alto',  'Tel Aviv', 'Kiel', 'Hamburg']
csv_number=20
records_number=10
os.makedirs('CSVs_Folder',exist_ok=True)
os.makedirs('mapreducetemp',exist_ok=True)
os.makedirs('mapreducefinal',exist_ok=True)
for i in range(csv_number):
    temp_df=pd.DataFrame()
    temp_df['firstname']=pd.Series(random.choices(firstname, k=records_number))
    temp_df['secondname'] = pd.Series(random.choices(secondname, k=records_number))
    temp_df['city'] = pd.Series(random.choices(city, k=records_number))
    temp_df.to_csv('CSVs_Folder/myCSV{0}.csv'.format(i),index=False)

#Q2:
con = sqlite3.connect('mydb_hw2.db')
cur = con.cursor()
#cur.execute('''DROP table IF EXISTS temp_results''')
cur.execute('''CREATE TABLE IF NOT EXISTS  temp_results (key text, value text)''')

# implement all of the class here

class MapReduceEngine:
    def execute(self, input_data, map_function, reduce_function):
        """
        :param input_data: is an array of elements
        :param map_function: is a pointer to the Python function that returns a list where each entry of the form (key,value)
        :param reduce_function: is pointer to the Python function that returns a list where each entry of the form (key,value)
        :return:
        """
        elements_num=len(input_data)
        thread_arr=[]
        for i in range(elements_num):
            thread_arr.append(threading.Thread(target=map_function,args=[input_data[i]]))
            thread_arr[i].start()
        self.isThreadAlive(thread_arr)
        self.loadCsvToDB()
        group_list=self.GroupQueries()
        thread_arr = []
        for i in range(len(group_list)):
            thread_arr.append(threading.Thread(target=reduce_function,args=[group_list[i]]))
            thread_arr[i].start()
        self.isThreadAlive(thread_arr)
    def CsvPerThread(self, key):
        records_number = 10
        x=[]
        for i in range(records_number): x.append((random.choice(firstname),random.choice(secondname)))
        thread_num=threading.current_thread().name.split('-')[1]
        temp_df=pd.DataFrame(x)
        temp_df.to_csv('mapreducetemp/part-tmp-{0}.csv'.format(thread_num), index=False, header=None)
        return
    def CsvPerThreadFinal(self, key):
        records_number = 10
        x=[]
        for i in range(records_number): x.append((random.choice(firstname),random.choice(secondname)))
        thread_num=threading.current_thread().name.split('-')[1]
        temp_df=pd.DataFrame(x)
        temp_df.to_csv('mapreducefinal/part-{0}-final.csv'.format(thread_num), index=False, header=None)
        return
    def isThreadAlive(self, threadList): #Join in C - flow continues once threads completed.
        time.sleep(1)
        isAlive=True
        for thread in threadList:
            if thread.is_alive():
                isAlive=False
                print('Thread #{0} has not finished'.format(thread))
        if isAlive:
            print('All threads completed')
    def loadCsvToDB(self):
        csvList=os.listdir('mapreducetemp')
        for i in range(len(csvList)):
            data = pd.read_csv('mapreducetemp/{0}'.format(csvList[i]), names=['key','value'], header=None)
            data.to_sql('temp_results', con,schema='online', if_exists='append', index = False)
    def GroupQueries(self):
        cur.execute('SELECT key,group_concat(value) FROM temp_results group by key ORDER BY key;')
        df = pd.read_sql_query("SELECT key,group_concat(value) FROM temp_results group by key ORDER BY key;", con)
        ls = list(cur.fetchall())
        print(cur.fetchall())
        return ls

def printTempResult():
    cur.execute("SELECT * FROM temp_results")
    print(cur.fetchall())

#Check type of return variable in function.
a=MapReduceEngine()
a.execute([0,1,2,3,4,5,6],a.CsvPerThread,a.CsvPerThreadFinal)
# df = pd.read_sql_query("SELECT * from temp_results", con)

def inverted_map(document_name,column_index):
    with open(document_name, 'r') as csvfile:
        f=csv.DictReader(csvfile)
        headers=f.fieldnames
        ls=[]
        for row in f:
            ls.append((row[headers[column_index]],document_name))
            #ls.append((row[0].split(',')[0]+'_'+row[0].split(',')[1],' '+document_name))
    return ls

def inverted_reduce(key,documents):
    """
    with open(document_name, 'r') as csvfile:
        f=csv.DictReader(csvfile)
        headers=f.fieldnames
        ls=[]
        for row in f:
            ls.append((row[headers[column_index]],document_name))
            #ls.append((row[0].split(',')[0]+'_'+row[0].split(',')[1],' '+document_name))
    return ls
    """
    return True

x=inverted_map('CSVs_Folder/myCSV0.csv',1)
csvList=os.listdir('CSVs_Folder')
inverted_reduce(key=1,csvList)
print('hi')