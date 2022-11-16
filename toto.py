import json
my_list = []
urls = []

with open('message.txt') as f:
    lines = f.readline() # list containing lines of file
    for line in lines:
        my_list = line.split('"href":')

for i in my_list:
    urls = i.split(',')[0]
    print(urls)