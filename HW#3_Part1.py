#04/13/2024
#Alvin Liu 
#CIS 3120 HW#3 Part 1: Web Scraping Tables and Inputting it into the API

#Importing all the necessary libraires.
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import numpy as np

#The function below will take the parameter of the url of the website that we want to scrape and output all the top 100
#publicly traded companies symbols in a dataframe for our api requests later. However, we would also be getting the company name too.
#I decided not to scrap the company market cap and price from the site because we can just get them from the api later.
def top_100_publicly_traded_companies_ticker(url):
    #Lists for the company name and ticker that we wil get later.
    company_name = []
    ticker = []
    page = requests.get(url) #Making the request to the url
    if page.status_code == 200: #If the page status code is 200, we can start scraping, if not it will output an error message.
        soup = BeautifulSoup(page.content, "html.parser") #Importing the raw html into beautiful soup.
        company_tags = soup.find_all('td', class_ = 'name-td') #Finding all the td tables with the class of "name-td" into a list.
        #The for loop below would take all the td tags and find the first instance of the div with the class of either company name or
        #company code and assign them to a variable. Afterwards, it will append each of the ticker and name to the variables.
        for company in company_tags:
            companies = company.find('div', class_ = 'company-name') #Getting the company name
            tickers = company.find('div', class_ = 'company-code') #Getting the company code
            if '.' not in tickers.get_text(): #This if statement is to tell the scrapper to see if there is another period on the ticker because the API can't seem to find the information for some international companies.
                ticker.append(tickers.get_text())
                company_name.append(companies.get_text())
        
        #Making the df1 dataframe using pandas with the information that we scraped earlier
        df1 = pd.DataFrame({
                'Company Name' : company_name,
                'Company Ticker' : ticker
            })
        return df1 #Returning the dataframe
    else:
        print(f"This website is not available. The status code is {page.status_code}.")

#The function below would take the arguement of the df1['Company Ticker'] tocall the Real-Time Finance Data API from the website RAPIDAPI.
#It would return another dataframe called df2 that contain the company stock's price, previous close price, change in price, year low price,
#year high price, CDP score, PE Ratio, and Market Cap.
#WARNING: THIS API CAN ONLY TAKE 200 REQUESTS PER MONTH. USE SPARELY!!!
#As of April I have used 85% of the request.
def realtime_finance_api(tickers):

    #List of the information that we want from the company.
    price = []
    previous_close = []
    change = []
    year_low = []
    year_high = []
    cdp_score = []
    pe_ratio = []
    market_cap = []

    #A for loop so that for each of the tickers in the df1['Company Ticker'], we would get each information seperately.
    for ticker in tickers:

        #Making the calls to the API
        url = "https://real-time-finance-data.p.rapidapi.com/stock-overview"

        querystring = {f"symbol":{ticker},"language":"en"} #The Query to get the information from the API

        headers = {
            "X-RapidAPI-Key": "Enter Your Own API Key here",
            "X-RapidAPI-Host": "real-time-finance-data.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)
 
        if response.status_code == 200: #Making sure that we get a valid response from the API
            data = response.json() #Getting the API data in json format
            #We appending most of the data in float form.
            price.append(float(data['data']['price'])) #Appending the price of that company stock to the list by going to the data key in the series and getting the price key from the data key.
            previous_close.append(float(data['data']['previous_close'])) #Appending the previous close price of that company stock to the list by going to the data key in the series and getting the previous close price key from the data key.
            change.append(float(data['data']['change'])) #Appending the change in price of that company stock to the list by going to the data key in the series and getting the change in price key from the data key.
            year_high.append(float(data['data']['year_high'])) #Appending the year high of that company stock to the list by going to the data key in the series and getting the year_high key from the data key.
            year_low.append(float(data['data']['year_low'])) #Appending the year low of that company stock to the list by going to the data key in the series and getting the year_low key from the data key.
            market_cap.append(round(float(data['data']['company_market_cap']), 2)) #Appending the market_cap of that company to the list by going to the data key in the series and getting the market_cap key from the data key. We will round it to the hundredth place.

            #The code below will try to scrape the company's cdp score, if it is not available, we would say it is not available.
            try:
                cdp_score.append(data['data']['company_cdp_score']) #Appending the cdp score of that company to the list by going to the data key in the series and getting the company_cdp_score key from the data key.
            except:
                cdp_score.append('Not available')
            
            #The code below will try to scrape the company's pe, if it is not available, we would append nan to it.
            try:
                pe_ratio.append(round(float(data['data']['company_pe_ratio']), 2)) #Appending the pe ratio of that company stock to the list by going to the data key in the series and getting the company_pe_ratio key from the data key. We will round it to the hundredth place.
            except:
                pe_ratio.append(np.nan)
        else:
            print(f"API request was unsuccessful. The status code is {response.status_code}.")
    
    #Making the second dataframe on the stock of the 100 companies that we scraped.
    df2 = pd.DataFrame({
        'Last Price (USD)' : price,
        'Previous Closing Price (USD)' : previous_close,
        'Change in Price' : change,
        'Year Low' : year_low,
        'Year High' : year_high,
        'Carbon Disclosure Rating Score' : cdp_score,
        'PE Ratio' : pe_ratio,
        'Market Capitalization (USD)' : market_cap
    })

    return df2 #Returning the dataframe

#The main function will be used to insert the website that we will use to be scraped, assigning the dataframes names, and merging them horizontally
#To a new dataframe called df3. We will then output df3 as a csv file, print the dataframe, and the min, max, etc of each 
#numerical column of the dataframe.
def main():
    url = 'https://companiesmarketcap.com/' #Website that we will be using
    df1 = top_100_publicly_traded_companies_ticker(url) #Assigning the first dataframe
    df2 = realtime_finance_api(df1['Company Ticker']) #Assigning the second dataframe by passing through the ticker names of each companies into the API.
    df3 = pd.concat([df1, df2], axis = 1) #Mergeing the dataframes horizontally.

    df3.to_csv('Top Publicly Traded Companies Stocks By Market Cap.csv', index=False) #Outputting the csv file and getting rid of the indexes.
    print(df3) #Displaying the dataframe
    print(df3.describe().apply(lambda s: s.apply('{0:.2f}'.format))) #Displaying the  statistics for the combined dataframe.

main()