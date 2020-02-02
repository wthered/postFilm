##################################
# Source Can be found at
# https://medium.com/hanman/data-clustering-what-type-of-movies-are-in-the-imdb-top-250-7ef59372a93b
# Incomplete and erroneous as of now
##################################
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import operator

URL = "http://www.imdb.com/chart/top"
r = requests.get(URL)
soup = BeautifulSoup(r.content, "lxml")
entries = soup.findAll('div', class_="wlb_ribbon")
movie_ids = []
for a in entries:
    movie_ids.append(a['data-tconst'])

header = 'http://www.omdbapi.com/?i='
movie_info = []
for i in movie_ids:
    url = header + i + "&apikey=fb3ced84&plot=full"
    r = requests.get(url).json()
    # print("Requesting {}".format(url))
    movie = []
    for a in r.keys():
        movie.append(r[a])
    movie_info.append(movie)
columns = r.keys()
df = pd.DataFrame(movie_info, columns=columns)
print(df)

content = []
for a in movie_ids:
    URL = "http://www.imdb.com/title/" + a
    r = requests.get(URL)
    content.append(r.content)
    print("done: " + str(a))
    a += 1
budget = []
revenue = []

for soups in contentsoup:
    entries = soups.findAll('div', class_="txt-block")
    try:
        entry = entries[9].text.split(":")[1].replace(",", "").replace("$", "").split("(")[0].replace(" ", "")
        budget.append(float(entry))
    except:
        budget.append(np.NaN)

    try:
        local_entry = float(
            entries[10].text.split(":")[1].replace(",", "").replace("$", "").split("(")[0].replace(" ", ""))
        revenue.append(local_entry)
    except:
        revenue.append(np.NaN)

plots = list(df['Plot'])

temp = ""
for p in plots:
    temp = temp + p

temp.replace(".", " ").replace(",", "")
temp = temp.split(" ")
temp = map(lambda x: x.lower(), temp)

freq = {i: temp.count(i) for i in set(temp)}
sort = sorted(freq.items(), key=operator.itemgetter(1), reverse=True)
