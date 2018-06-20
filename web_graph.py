import socket
import MySQLdb
import requests
from bs4 import BeautifulSoup
import config as conf
from urllib.parse import urlparse
import re

startingPoints = conf.STARTING_POINTS
for startingPoint in startingPoints:
    frontier = [startingPoint]
    blacklist = []
    pFrontier = []
    db = MySQLdb.connect(conf.DATABASE['server'], conf.DATABASE['username'], conf.DATABASE['password'],
                         conf.DATABASE['database'])
    cursor = db.cursor()
    while len(frontier) > 0:
        source = frontier.pop(0)
        pFrontier.append(source)
        try:
            request = requests.get(source)
        except:
            continue
        if request.status_code == 200:
            soup = BeautifulSoup(request.text, 'html.parser')
            for link in soup.find_all('a', attrs={'href': re.compile("^(http|https)://")}):
                dest = link.get('href')
                try:
                    s = socket.gethostbyname(urlparse(source).hostname)
                    d = socket.gethostbyname(urlparse(dest).hostname)
                except:
                    continue
                if dest not in pFrontier and len(frontier) < conf.CAPACITY:
                    frontier.append(dest)
                if (s + d) not in blacklist:
                    try:
                        cursor.execute('insert into graph values("%s","%s")' % (s, d))
                        db.commit()
                        print(s, '>', d)
                    except:
                        blacklist.append(s + d)
                        db.rollback()
        if len(blacklist) > conf.LIMIT:
            break
    db.close()
