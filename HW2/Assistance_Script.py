import random

firstname = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
secondname = ['Mayfield', 'Redding', 'Charles', 'Brown', 'Gaye', 'Simone', 'Wonder', 'Heron', 'Franklin']
city = ['New York', 'Haifa', 'München', 'London', 'Palo Alto',  'Tel Aviv', 'Kiel', 'Hamburg']

x=[]
n=10
for i in range(n):
    x.append((random.choice(firstname),random.choice(secondname)))

print(x)