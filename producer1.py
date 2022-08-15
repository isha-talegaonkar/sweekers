import pandas as pd
from serpapi import GoogleSearch
import sqlite3
import datetime as dt

import config

import os
import time
import threading
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

from kafka import KafkaProducer
import logging
from json import dumps, loads
import csv
logging.basicConfig(level=logging.INFO)


producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', value_serializer=lambda K:dumps(K).encode('utf-8'))

def google_job_search(job_title, city_state, post_age="week"):
    '''
    job_title(str): "Data Scientist", "Data Analyst"
    city_state(str): "Denver, CO"
    post_age,(str)(optional): "3day", "week", "month"
    '''
    params = {
            "engine": "google_jobs",
            "q": f"{job_title} {city_state}",
            "hl": "en",
            "api_key": "",
            "chips":f"date_posted:{post_age}", 
            }
    search = GoogleSearch(params)
    results = search.get_dict()
    if('jobs_results' in results.keys()):
        jobs_results = results['jobs_results']
        job_columns = ['title', 'company_name', 'location', 'description']
        df = pd.DataFrame(jobs_results, columns=job_columns)
        return df

def main(job_list, city_state_list):
    job_columns = ['title', 'company_name', 'location', 'description']
    main_df = pd.DataFrame(columns=job_columns)
    df_10jobs = pd.DataFrame()
    for job in job_list:
        for city_state in city_state_list:
            df_10jobs = google_job_search(job, city_state, post_age="month")
            if(not df_10jobs.empty):
                date = dt.datetime.today().strftime('%Y-%m-%d')
                df_10jobs['retrieve_date'] = date
                #print(df_10jobs)
                #main_df = main_df.append(df_10jobs)
                for indices, row in df_10jobs.iterrows():
                    print(row)
                    message = " ".join(row)
                    producer.send('jobs_google', message)
                    producer.flush()
                    time.sleep(5)

if __name__ == "__main__":
    job_list = ["Data Scientist", "Data Analyst", "Data Engineer", 
                "Machine Learning Engineer", "Business Intelligence Analyst"]
    city_state_list = ["Atlanta, GA", "Austin, TX", "Boston, MA", "Chicago, IL", 
                    "Denver, CO", "Dallas-Ft. Worth, TX", "Los Angeles, CA",
                    "New York City, NY", "San Francisco, CA", "Seattle, WA"]
    main(job_list, city_state_list)
    #print(result)
