import sqlite3
import http.client
import json

#fetching the data from the rapid api
def getenricheddata(link):
    httpConn = http.client.HTTPSConnection("linkedin-bulk-data-scraper.p.rapidapi.com")
    payload = f'{{"link":"{link}"}}'
    headers = {
        'x-rapidapi-key': "472abcb66dmsh3e3ff0bf66046dcp12deaajsnf56c0b0f713a",
        'x-rapidapi-host': "linkedin-bulk-data-scraper.p.rapidapi.com",
        'Content-Type': "application/json"
    }
    httpConn.request("POST", "/company", payload, headers)
    res = httpConn.getresponse()
    responseData = res.read()
    return json.loads(responseData.decode("utf-8"))


try:
    dbConn = sqlite3.connect('assets/companies.db')
    cursor = dbConn.cursor()

    cursor.execute('SELECT * FROM companies LIMIT 2 OFFSET 11') # Limiting to two rows because of rapid api limitation

   
    rows = cursor.fetchall()
    for row in rows:

        resultData = getenricheddata(row[1])

        insert_query = """
                INSERT INTO enriched_companies_data (
                    companyId, companyName, city, country, employeeCount, croppedImageURL, tagline, followerCount,
                    originalCoverImageURL, industry, description, websiteUrl, foundedMonth, foundedYear,
                    foundedDate, universalName, hashtag, companyURL
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """
        if resultData.get('data') is not None:
            company_data = resultData['data']
        else:
            print('Unexpected Response', resultData)
            exit()

        if 'locations' in company_data and len(company_data['locations']) > 0:
            location = company_data['locations'][0]
        else:
            location = {}

        data_tuple = (
            company_data['companyId'],  
            company_data['companyName'], 
            location.get('city', None),  
            location.get('country', None), 
            company_data.get('employeeCount', 0),  
            company_data.get('croppedCoverImage', None),  
            company_data.get('tagline', None),  
            company_data.get('followerCount', 0), 
            company_data.get('originalCoverImage', None),  
            company_data.get('industry', None), 
            company_data.get('description', None),  
            company_data.get('websiteUrl', None),  
            company_data.get('foundedMonth', None), 
            company_data.get('foundedYear', 0),  
            company_data.get('foundedDate', 0),  
            company_data.get('universalName', None), 
            company_data.get('hashtag', None),  
            company_data.get('companyURL', None)  
        )

        cursor.execute(insert_query, data_tuple)
        print(data_tuple)

    dbConn.commit()
    cursor.close()
    dbConn.close()

except Exception as e:
    print(e)
