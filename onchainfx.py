import sys
import requests
import re
import datetime
import time
import pandas as pd
import pymongo
import urllib.parse

from pymongo.errors import ConnectionFailure, ConfigurationError
from bs4 import BeautifulSoup as BS
from bs4 import SoupStrainer
from apscheduler.schedulers.blocking import BlockingScheduler
from selenium import webdriver
#onchainfx
#martes 17 1:00 pm

#this function parse the html, find all the tags with the relevant data of the table
#then returns a pandas DataFrame with the content
def load_html_dataframe():

    #open chrome the gets the html code after the dinamic data loads
    #unzip the PhantomJS driver in the current path
    
    browser = webdriver.PhantomJS("phantomjs-2.1.1-linux-x86_64/bin/phantomjs")
    browser.get("http://onchainfx.com/v/L5KTFZ")

    time.sleep(15)

    html_page = browser.page_source
    browser.quit()

    matrix_head = SoupStrainer("thead")
    matrix_body = SoupStrainer("tbody")

    #parse the fraction of the html code the head and the body of the matrix
    soup_h = BS(html_page, "html.parser", parse_only=matrix_head)
    soup_b = BS(html_page, "html.parser", parse_only=matrix_body)


    #finds all the elements that contains the data we're looking for
    indexes = soup_h.find_all("th")
    names = soup_b.find_all("a", "table_asset_link")
    caps = soup_b.find_all("td", " col_marketcap_y2050_implied")
    cap_current = soup_b.find_all("td", " col_marketcap_current")
    ch_per = soup_b.find_all("td", " col_marketcap_24hr_percent_change")
    prc = soup_b.find_all("td", " col_price_usd")
    vol = soup_b.find_all("td"," col_vol_last24")
    sup = soup_b.find_all("td", " col_supply_y2050")
    sup_per = soup_b.find_all("td", " col_supply_y2050_percent_issued")

    #loads the data of the matrix by type
    indexlist = [item.get_text() for item in indexes[2:]]
    indexlist.append('Date')
    namelist = [item.get_text() for item in names]
    capital = [re.sub('[$,]','',item.get_text()) for item in caps]
    current = [re.sub('[$,]','',item.get_text()) for item in cap_current]
    change = [item.get_text() for item in ch_per]
    price = [re.sub('[$,]','',item.get_text()) for item in prc]
    volume = [re.sub('[$,]','',item.get_text()) for item in vol]
    supply = [re.sub('[$,]','',item.get_text()) for item in sup]
    supply_per = [item.get_text() for item in sup_per]
 
    #time stamps
    timenow = "%s" % datetime.datetime.now()
    timenow = timenow[:16]
    timestmp = [timenow for i in range(len(namelist))]

    #creates the dataframe that's going to be loaded into the database
    columns_content = [namelist, capital, current, change, price, volume,supply,supply_per,timestmp]
    dt_dict = dict(zip(indexlist, columns_content))
    df = pd.DataFrame(dt_dict)

    return df


def maintainload(database):

    dataf_cont = load_html_dataframe()

    names = dataf_cont["Name"]
    del dataf_cont["Name"]

    #keys values of the dictionary of our document for the collections
    keys = dataf_cont.columns

    for j,objs in enumerate(names):
        act_collct = database.get_collection(objs)
        act_collct.insert(dict(zip(keys, dataf_cont.iloc[j])))

def main():

    sched = BlockingScheduler()

    opc=input('Create or maintain a database?  (1 -Create | 2 -Maintain):')

    dbname=input("Database name:")
    user=input('Username:')
    password=input('Password:')
    
    if opc=="2":        
        usr = urllib.parse.quote_plus(user)
        pwd = urllib.parse.quote_plus(password)
        client = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1' % (usr, pwd))
        db = client[dbname]

    elif opc=="1":
        client = pymongo.MongoClient()
        db=client[dbname]
        db.add_user(user,password)

    try:
        client.admin.command('ismaster')
    except ConfigurationError:
        print("Server not available")

    print("Press 'CTRL + C' to exit")

    dataf = load_html_dataframe()
    
    #getting the name of each cryptocurrency
    collects = dataf["Name"]
    del dataf["Name"]

    #keys values of the dictionary of our document for the collections


    if opc=="1":
        for items in collects:
        #Create the collections if it hasnt being created to avoid conflicts
            db.create_collection(items)
            act_collect=db.get_collection(items)


    #every hour runs maintainload and insert documents into the collections
    sched.add_job(maintainload, trigger='cron', args=[db], minute=0, second=0)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass



if __name__=="__main__":
    main()
