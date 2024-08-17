import sqlite3
import json

try:
    connection = sqlite3.connect('assets/companies.db')
    cursor = connection.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            companyId INTEGER PRIMARY KEY,
            companyURL TEXT
        )
        ''')
    print('companies table created')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS enriched_companies_data (
        companyId INTEGER PRIMARY KEY,
        companyName TEXT,
        city TEXT,
        country TEXT,
        employeeCount INTEGER,
        croppedImageURL TEXT,
        tagline TEXT,
        followerCount INTEGER,
        originalCoverImageURL TEXT,
        industry TEXT,
        description TEXT,
        websiteUrl TEXT,
        foundedMonth TEXT,
        foundedYear INTEGER,
        foundedDate INTEGER,
        universalName TEXT,
        hashtag TEXT,
        companyURL TEXT
    )
    ''')

    
    print('enriched_companies_data table created')

    with open('assets/bootstrap.json', 'r') as file:
        resultData = json.load(file)

    dataMap = {}
    tupleList = []
    for company in resultData['data']:
        companyId = company['data']['companyId']
        companyUrl = company['entry']
        if dataMap.get(companyId) is None:
            company_tuple = (companyId, companyUrl)
            tupleList.append(company_tuple)
            dataMap[companyId] = companyUrl


    cursor.executemany('''
        INSERT INTO companies (companyId, companyURL)
        VALUES (?, ?)
    ''', tupleList)

    print("Inserted Bootstrap Data")

    connection.commit()
    cursor.close()
    connection.close()

except Exception as e:
    print('Error', e)
