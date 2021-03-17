import requests
import sqlite3
import re
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin
# Remove on release
import os

if os.path.exists('testing.db'):
    os.remove('testing.db')

# Set up database
dbConnection = sqlite3.connect('testing.db')
c = dbConnection.cursor()
c.execute('''CREATE TABLE unit
            (unitID text PRIMARY KEY, unitName text, 
            offering1 INTEGER DEFAULT 0, offering2 INTEGER DEFAULT 0, offering3 INTEGER DEFAULT 0, 
            unitDescription text)''')
c.execute('''CREATE TABLE prerequisite
            (unitID text, prereqGroup INTEGER DEFAULT 0, prereqUID text)''')

# Regex for parsing prerequisites
prereqPattern = re.compile(r"[a-zA-Z]{4}\d{4}|and")


def scrape_subject(subject_url, unit_id):
    subject_connection = sqlite3.connect('testing.db')
    sc = subject_connection.cursor()

    print(unit_id)
    subject_page = requests.get(subject_url)
    subject_soup = BeautifulSoup(subject_page.content, 'html.parser')
    info_table = subject_soup.find(class_='general-info-table')

    # update unit description
    desc = info_table.find(class_='unit-description').find(class_='general-info-value')
    desc_text = desc.find('p')
    if desc_text is None:
        desc_text = desc.text
    else:
        desc_text = desc_text.text

    desc_text = desc_text.strip()
    desc_sql = '''UPDATE unit SET unitDescription = ? WHERE unitID = ?'''
    sc.execute(desc_sql, (desc_text, unit_id))

    # parse prerequisites
    prereq = info_table.find(class_='unit-prerequisites').find(class_='general-info-value').text.strip()
    parsed_prereq = re.findall(prereqPattern, prereq)
    if parsed_prereq is not None and len(parsed_prereq) != 0:
        if parsed_prereq[0] == 'and':
            parsed_prereq.pop(0)
        prereq_group = 0
        for word in parsed_prereq:
            if word == 'and':
                prereq_group += 1
            else:
                prereq_sql = '''INSERT INTO prerequisite (unitID, prereqGroup, prereqUID) VALUES (?, ?, ?)'''
                sc.execute(prereq_sql, (unit_id, prereq_group, word))

    subject_connection.commit()


# Home page scraping
startUrl = 'https://unitguides.mq.edu.au/units/show_year/2020/Department%20of%20Computing'
url = startUrl
hasMorePages = True

unitIDSet = set()

while hasMorePages:
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    unitListTable = soup.find(class_='table-search-results')
    # remove "help" div
    unitListTable.find(class_='unit-guide-list-header').decompose()

    unitElements = unitListTable.find_all('a')

    threads = []
    for unitElement in unitElements:
        (thisUnitID, thisUnitTitle) = unitElement.find(class_='underline').text.split(maxsplit=1)
        thisOffering = int(unitElement.find(class_='unit-handbook-code').text.split(maxsplit=2)[1][0])
        if thisUnitID not in unitIDSet:
            unitLink = urljoin(startUrl, unitElement['href'])

            c.execute('INSERT INTO unit (unitID, unitName) values (?, ?)', (thisUnitID, thisUnitTitle))
            unitIDSet.add(thisUnitID)

            thread = threading.Thread(target=scrape_subject, args=(unitLink, thisUnitID))
            thread.start()
            threads.append(thread)

        offeringSQL = "UPDATE unit SET offering" + str(thisOffering) + " = 1 WHERE unitID = \"" + thisUnitID + "\""
        c.execute(offeringSQL)

    dbConnection.commit()

    for t in threads:
        t.join()

    # hasMorePages = False
    # Continue onto the next page
    nextPage = soup.find(class_='next_page')
    if 'disabled' in nextPage['class']:
        print('No more pages')
        hasMorePages = False
    else:
        relUrl = nextPage.find('a')['href']
        url = urljoin(startUrl, relUrl)

dbConnection.commit()
