import csv
import random
import pandas as pd

random.seed(42)

index = range(1, 1000001)
price = range(10, 101)

fruit = [random.choice(['Orange', 'Grape', 'Apple', 'Banana', 'Pineapple', 'Avocado']) for i in index]
color = [random.choice(['Red', 'Green', 'Yellow', 'Blue']) for i in index]
price = [random.choice(price) for i in index]

df = pd.DataFrame({'id' : index, 'fruit' : fruit, 'price' : price, 'color' : color})
df.to_csv('mydata.csv', index=False)

print('Done')

"""
import sqlite3
conn = sqlite3.connect(r'mydb.db') #Connection to DB stores locally on project folder
cur = conn.cursor()
cur.execute('CREATE TABLE mydata (id integer, fruit text, color text, price integer);') #Cr
conn.close()

print('Success')
"""

"""
import csv, sqlite3

conn = sqlite3.connect(r"mydb.db")
cur = conn.cursor()

with open('mydata.csv','r') as f:
    reader = csv.reader(f)
    for row in reader:
        cur.execute('INSERT INTO mydata (id, fruit, price, color) VALUES (?, ?, ?, ?);', row)

conn.commit()
conn.close()
"""

import csv, sqlite3

conn = sqlite3.connect(r'mydb.db')
cur = conn.cursor()

# 'SELECT AVG(price)' is the projection and 'WHERE fruit = "Grape"' is the predicate
average_price_of_grapes_query = ''' SELECT AVG(price) 
                                    FROM mydata 
                                    WHERE fruit = "Grape"; '''

cur.execute(average_price_of_grapes_query)
result = cur.fetchall()
print('average_price_of_grapes =', result)

# 'SELECT COUNT(id)' is the projection and 'WHERE fruit = "Orange" AND color = "Blue"' is the predicate
count_of_blue_oranges_query = ''' SELECT COUNT(id) 
                                FROM mydata
                                WHERE fruit = "Orange" AND color = "Blue"; '''

cur.execute(count_of_blue_oranges_query)
result = cur.fetchall()
print('count_of_blue_oranges =', result)

conn.close()
print('Success')


import math
import os

def first_chunk(middle):
    with open('mydata.csv','rb') as f:
        byte_stream = f.read(math.floor(middle)).decode(encoding='utf-8')
        return byte_stream.count('\n') if byte_stream[math.floor(middle) - 1] == '\n' else byte_stream.count('\n') + 1

def last_chunk(middle):
    with open('mydata.csv','rb') as f:
        f.seek(math.floor(middle), 0)
        byte_stream = f.read(math.ceil(middle)).decode(encoding='utf-8')
        return byte_stream.count('\n') - 1 if byte_stream[0] == '\n' else byte_stream.count('\n')

middle = os.path.getsize('mydata.csv') / 2
print('First chunk lines count:', first_chunk(middle))
print("Last chunk lines count:", last_chunk(middle))
print(first_chunk(middle) + last_chunk(middle))



import os

CHUNK_SIZE = 1024 * 1024 * 4

FILE_SIZE = os.path.getsize('mydata.csv')

def count_lines_in_chunk(seek_to, chunk_size):
    with open('mydata.csv', 'rb') as f:
        f.seek(seek_to, 0)
        byte_stream = f.read(chunk_size).decode(encoding='utf-8')
        return byte_stream.count('\n')

def get_modified_chunk_size(seek_to):
    with open('mydata.csv', 'rb') as f:
        f.seek(seek_to + CHUNK_SIZE, 0)
        byte_stream = f.read(FILE_SIZE - seek_to - CHUNK_SIZE).decode(encoding='utf-8')
        first_newline = byte_stream.index('\n')
    return CHUNK_SIZE + first_newline + 1

total_lines = 0
seek_to = 0
while seek_to <= FILE_SIZE:
    if seek_to + CHUNK_SIZE > FILE_SIZE:
        chunk_lines = count_lines_in_chunk(seek_to, FILE_SIZE - seek_to)
    else:
        modified_chunk_size = get_modified_chunk_size(seek_to)
        chunk_lines = count_lines_in_chunk(seek_to, modified_chunk_size)
    total_lines += chunk_lines
    seek_to += modified_chunk_size

print(total_lines)