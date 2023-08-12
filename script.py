"""Jio Cash Management Script with the application of API"""

from whizzbox import db_connector, site_login, config, toolkit, s3_connector as s3c
from dotenv import load_dotenv
import pandas as pd
import requests
import json
import time
import datetime
from datetime import date
from datetime import datetime
from datetime import timedelta
from selenium.webdriver.common.by import By  # to use By parameter in find_elements method
from selenium.webdriver.common.keys import Keys  # to use Keys in send_keys method
from selenium.webdriver.support.ui import WebDriverWait  # to wait till certain elements were located
from selenium.webdriver.support import expected_conditions as EC  # to use expected conditions with WebDriverWait
import numpy as np  # to perform numerical calculations
import os  # to work with files and folders

# assigning value to LOCAL_MACHINE variable based on the presence of .git folder
PROJECT_NAME = 'whiz-jio-cash'

# assign the opposite value of LOCAL_MACHINE to ON_SERVER
ON_SERVER = config.ON_SERVER
LOCAL_MACHINE = config.LOCAL_MACHINE

HEADLESS = True
TEST = False  # if it's a test, email recipients are limited
SAMPLE = False	  # if sample, the active sites were limited
SEND_EMAIL = True
SEND_FAIL_EMAIL = True

def get_jio_trip_summary(station_code, user_id, from_date, to_date):
    url = "https://tms.reliancesmart.in/tripui/api/trip/trip-mgmt/v1/trips"

    querystring = {"pageSize": "200", "pageIndex": "1", "status": "INTRANSIT", "movementType": "FTL", "tripId": "",
                   "fromDate": f"{from_date}", "toDate": f"{to_date}"}

    payload = ""
    headers = {
        "cookie": "TS016c9f7d=01ef61aed0b3c0f527e378937eb1cfa3888b283276258e93b7b0ce8fcec39acfe609c6b56ac790f9ea8859e8e080e6d7da33bb028a",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg",
        "Connection": "keep-alive",
        "Cookie": "ixtk_dHJpcA=eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg; TS016c9f7d=01ef61aed0b3c0f527e378937eb1cfa3888b283276258e93b7b0ce8fcec39acfe609c6b56ac790f9ea8859e8e080e6d7da33bb028a; ADRUM_BTa=R:42|g:4e0e6f53-d7f8-4e28-a143-1c8877274f77|n:customer1_a309c9d0-b5ef-4ff1-8978-610c0b29df8f; SameSite=None; ADRUM_BT1=R:42|i:2193917|e:36; lri=62",
        "Referer": "https://tms.reliancesmart.in/tripui/manage",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "nodeId":  f"{station_code}",
        "nodeType": "HUB",
        "sec-ch-ua": '""Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109""',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "'Windows",
        "userId": f"{user_id}",
        "x-request-id": "62"
    }
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

    data = json.loads(response.text)
    req_data = data['data']['trip']
    df = pd.DataFrame(req_data)
    status = 'no_data' if df.empty else 'success'
    print(f'open trips: {status}', end=', ')
    return {'df': df, 'status': status}


def get_jio_trip_all(station_code, user_id, from_date, to_date):

    url = "https://tms.reliancesmart.in/tripui/api/trip/trip-mgmt/v1/trips"

    querystring = {"pageSize": "250", "pageIndex": "1", "movementType": "FTL", "tripId": "", "fromDate": f"{from_date}",
                   "toDate": f"{to_date}"}

    payload = ""
    headers = {
        "cookie": "TS016c9f7d=01ef61aed0b3c0f527e378937eb1cfa3888b283276258e93b7b0ce8fcec39acfe609c6b56ac790f9ea8859e8e080e6d7da33bb028a",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg",
        "Connection": "keep-alive",
        "Cookie": "ixtk_dHJpcA=eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg; TS016c9f7d=01ef61aed0b3c0f527e378937eb1cfa3888b283276258e93b7b0ce8fcec39acfe609c6b56ac790f9ea8859e8e080e6d7da33bb028a; ADRUM_BTa=R:42|g:4e0e6f53-d7f8-4e28-a143-1c8877274f77|n:customer1_a309c9d0-b5ef-4ff1-8978-610c0b29df8f; SameSite=None; ADRUM_BT1=R:42|i:2193917|e:36; lri=62",
        "Referer": "https://tms.reliancesmart.in/tripui/manage",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "nodeId":  f"{station_code}",
        "nodeType": "HUB",
        "sec-ch-ua": '""Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109""',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "'Windows",
        "userId": f"{user_id}",
        "x-request-id": "62"
    }
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

    data = json.loads(response.text)
    req_data = data['data']['trip']
    df = pd.DataFrame(req_data)
    status = 'no_data' if df.empty else 'success'
    print(f'all trips: {status}', end=', ')
    return {'df': df, 'status': status}


def get_jio_pis_summary(station_code, user_id):
    url = "https://tms.reliancesmart.in/tripui/api/trip/trip-mgmt/v1/pis/daylevelcash"

    payload = ""
    headers = {
        "cookie": "TS016c9f7d=01ef61aed0bdab02e36ff5f4e0498b1c83387f41141114c4de5f89bfe86b7880b53b787c9ee4a8e7d56b2028bf9bbab53cffcfa88a",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg",
        "Connection": "keep-alive",
        "Cookie": "ixtk_dHJpcA=eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg; TS016c9f7d=01ef61aed0bdab02e36ff5f4e0498b1c83387f41141114c4de5f89bfe86b7880b53b787c9ee4a8e7d56b2028bf9bbab53cffcfa88a; SameSite=None; ADRUM_BTa=R:47|g:79248c5e-b618-419c-bd2c-a4968851fdc0|n:customer1_a309c9d0-b5ef-4ff1-8978-610c0b29df8f; ADRUM_BT1=R:47|i:2193917|e:22; lri=71",
        "Referer": "https://tms.reliancesmart.in/tripui/pis-summary",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "nodeId": f"{station_code}",
        "nodeType": "HUB",
        "sec-ch-ua": '""Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109""',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "'Windows",
        "userId": f"{user_id}",
        "x-request-id": "71"
    }

    response = requests.request("GET", url, data=payload, headers=headers)

    data = json.loads(response.text)
    req_data = data['data']['dayLevelCashDetails']
    df = pd.DataFrame(req_data)
    status = 'no_data' if df.empty else 'success'
    print(f'pis summary: {status}', end=', ')
    return {'df': df, 'status': status}


def get_jio_manage_pis(station_code, user_id, from_date, to_date):
    url = "https://tms.reliancesmart.in/tripui/api/trip/trip-mgmt/v1/pis"

    querystring = {"fromDate": f"{from_date}", "toDate": f"{to_date}"}

    payload = ""
    headers = {
        "cookie": "TS016c9f7d=01ef61aed0bdab02e36ff5f4e0498b1c83387f41141114c4de5f89bfe86b7880b53b787c9ee4a8e7d56b2028bf9bbab53cffcfa88a",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg",
        "Connection": "keep-alive",
        "Cookie": "ixtk_dHJpcA=eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg; TS016c9f7d=01ef61aed0bdab02e36ff5f4e0498b1c83387f41141114c4de5f89bfe86b7880b53b787c9ee4a8e7d56b2028bf9bbab53cffcfa88a; SameSite=None; ADRUM_BTa=R:46|g:1079ae2e-006d-4d2b-93ce-926c3f0cdba2|n:customer1_a309c9d0-b5ef-4ff1-8978-610c0b29df8f; ADRUM_BT1=R:46|i:2193917|e:33; lri=77",
        "Referer": "https://tms.reliancesmart.in/tripui/pis-manage",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "nodeId": f"{station_code}",
        "nodeType": "HUB",
        "sec-ch-ua": '""Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109""',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "'Windows",
        "userId": f"{user_id}",
        "x-request-id": "77"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

    data = json.loads(response.text)
    req_data = data['data']['pisDetailsList']
    df = pd.DataFrame(req_data)
    status = 'no_data' if df.empty else 'success'
    print(f'manage pis: {status}', end=', ')
    return {'df': df, 'status': status}


def get_return_trip(station_code, user_id, from_date, to_date):
    url = "https://tms.reliancesmart.in/tripui/api/trip/trip-mgmt/v1/return/trips"

    querystring = {"pageSize": "100", "pageIndex": "1", "status": "", "tripId": "", "fromDate": f"{from_date}",
                   "toDate": f"{to_date}"}

    payload = ""
    headers = {
        "cookie": "TS016c9f7d=01ef61aed031c12c3625a74d28ddf8bac1be213dc0fe6b065ece7a99b86e5dc516ac0d0fa84196d5072cdc9e2f4e59734bcdf4fc62",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg",
        "Connection": "keep-alive",
        "Cookie": "ixtk_dHJpcA=eyJhbGciOiJSUzI1NiJ9.eyJ0ZW5hbnRzIjpudWxsLCJmaXJzdE5hbWUiOiJHaXJpc2ggIiwibGFzdE5hbWUiOiJCZXRhc3VyIiwicm9sZXMiOlt7Im5hbWUiOiJOb2RlTWFuYWdlciJ9XSwiaWQiOiIzYjFjNTU1Zi02Y2VkLTQ4ZDUtOGRmYS1mNTY3OTcwMTRjYTgiLCJncmFudFR5cGUiOiJDUkVERU5USUFMX0dSQU5UIiwidHlwZSI6IkFjY2VzcyIsImV4cCI6MTY5MjIwNTQwMCwiaWF0IjoxNjkxMzQxNDAwLCJ1c2VybmFtZSI6IkhWMkIifQ.ARCRRBCviyS3MNYYAjLumSwQ2ajEbIJ_DhMi5uWT6UP7Mo77jITCC4zU2455EGJ1xiG3U6fdXyL2q1-AFavpUwqzhFT_gYp6mn0Pv-NZ1JzvkXOuIQRDNt6uNxXdN69ORwaY-zdmdnF1iRnM3cbx9v5nlHqqcsrpGVImjPtkmhcpg1XR_A6FCpWqex4etglKGz_4O2pfMY1e9_CJ2IKfr65H4W0llu_ewUZTokSlbPJjL_uLqnlyxbUyLHO2c-GfKVtT0zmrFBXX6fCYOqfI6dEc5pjZSuQhT9Jjld9M7lhvyGXX2VKECQx7CsjpZQVKGzgbhDvvAlqQvvsb49DgWg; SameSite=None; TS016c9f7d=01ef61aed031c12c3625a74d28ddf8bac1be213dc0fe6b065ece7a99b86e5dc516ac0d0fa84196d5072cdc9e2f4e59734bcdf4fc62; ADRUM_BTa=R:51|g:926f6aa3-e799-4493-a5df-4f4093743367|n:customer1_a309c9d0-b5ef-4ff1-8978-610c0b29df8f; ADRUM_BT1=R:51|i:2193917|e:26; lri=82",
        "Referer": "https://tms.reliancesmart.in/tripui/listreturntrips",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "nodeId":  f"{station_code}",
        "nodeType": "HUB",
        "sec-ch-ua": '""Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111""',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "'Windows",
        "userId": f"{user_id}",
        "x-request-id": "82"
    }
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

    data = json.loads(response.text)
    req_data = data['data']['trip']
    df = pd.DataFrame(req_data)
    status = 'no_data' if df.empty else 'success'
    print(f'return trips: {status}')
    return {'df': df, 'status': status}


def get_the_download_status(stn, status):
    return {'station_code': stn,
            'download_status': status}


def df_column_additions(df):
    df.insert(loc=0, column='station_code', value=stn_code)
    df.insert(loc=1, column='Site Code', value=site_code)
    df.insert(loc=2, column='scraping_date', value=today)
    df.insert(loc=3, column='scraping_time', value=hour)

    return df


def convert_epoch_date(x):
    if x != x:
        return np.nan
    else:
        return datetime.fromtimestamp(int(str(x)[:10]))


def convert_date(x):
    if x != x:
        return np.nan
    else:
        return pd.to_datetime(x).strftime("%Y-%m-%d")


def format_date_columns(df, col_key_word):
    df = df.fillna(np.nan)
    cols = df.columns
    cols_time_date = list(filter(lambda x: x.endswith(col_key_word), cols))

    for col in cols_time_date:
        df[col] = df[col].apply(lambda x: convert_epoch_date(x))
    return df


def filter_df_equal_to_value(df, column_name, value):
    new_df = df[df[column_name] == value]
    new_df = new_df.reset_index(drop=True)
    return new_df


def filter_df_keywords(df, column_name, keyword):
    new_df = df[df[column_name].str.contains(keyword)]
    new_df = new_df.reset_index(drop=True)
    return new_df


def rename_columns(df, df_type):
    if df_type == 'summary':
        df = df.rename(columns={'createdDate': 'Created Date', 'postedDate': 'Posted Date',
                                'postedBy': 'Posted By', 'pisNo': 'PIS ID', 'docNo': 'Document ID',
                                'amount': 'Amount ( ₹ )', 'status': 'Status', 'errorDesc': 'Error Message'})
    elif df_type == 'pis':
        df = df.rename(columns={'settlementDate': 'Settlement Date', 'totalSettledTrips': 'COD Settled Trips',
                                'totalAmount': 'COD Settled Amount', 'createdAmount': 'PIS Created',
                                'postedAmount': 'PIS Posted', 'pendingPostingAmount': 'Pending Posting',
                                'pendingCreationAmount': 'PIS Pending Creation',
                                'amountOnHoldForCreation': 'Creation In Progress'})

    else:
        df = df.rename(columns={'tripId': 'Trip Id', 'tripType': 'Trip Type', 'sourceNode': 'Source Station',
                                'plannedStartTime': 'Estimated Start Time', 'plannedEndTime': 'Estimated Delivery Time',
                                'actualStartTime': 'Actual Start Time', 'actualEndTime': 'Actual End Time',
                                'createdTime': 'Creation Time', 'fleetType': 'Fleet Type',
                                'vendorName': 'Vendor Name', 'driverMobileNumber': 'Driver Number',
                                'assignedVehicle': 'Vehicle Number', 'assignedDpId': 'Driver',
                                'vehicleModel': 'Vehicle Type', 'amountCollected': 'Amount Collected',
                                'amountToBeCollected': 'Amount To Be Collected'})
    return df


def rearrange_columns(df, df_type):
    if df_type == 'summary':
        df = df[['Created Date', 'PIS ID', 'Amount ( ₹ )', 'Posted Date', 'Posted By',
                 'Document ID', 'Status', 'station_code', 'scraping_date', 'scraping_time', 'Site Code']]

    elif df_type == 'pis':
        df['Action'] = ''
        df = df[['Settlement Date', 'COD Settled Trips', 'COD Settled Amount', 'PIS Created',
                 'Creation In Progress', 'PIS Posted', 'Pending Posting', 'PIS Pending Creation', 'Action',
                 'station_code', 'scraping_date', 'scraping_time', 'Site Code']]

    else:
        df[['Loaded Orders', 'Loaded HUs/BAGs', 'Planned Orders', 'Packed HUs/BAGs', 'Actual Orders Dispatched',
            'Actual HUs/BAGs Dispatch', 'Drops', 'Action']] = ''

        df = df[['Trip Id', 'Trip Type', 'Creation Time', 'Estimated Start Date', 'Estimated Start Time',
                 'Estimated Delivery Time', 'Amount Collected', 'Amount To Be Collected', 'Fleet Type', 'Vehicle Type',
                 'Vehicle Number', 'Driver', 'Driver Number', 'Vendor Name',
                 'Loaded Orders', 'Loaded HUs/BAGs', 'Planned Orders', 'Packed HUs/BAGs', 'Actual Orders Dispatched',
                 'Actual HUs/BAGs Dispatch', 'Drops', 'Action', 'Source Station', 'station_code', 'scraping_date',
                 'scraping_time', 'Site Code']]
    return df


def format_date_column_jio(old_df, column_name):
    new_df = old_df[column_name + '_portal'].str.split(" ", n=1, expand=True)[0].str.split("/", n=2, expand=True)
    new_df[column_name] = new_df[2].astype(str) + '-' + new_df[1].astype(str) + '-' + new_df[0].astype(str)
    old_df[column_name] = new_df[column_name]

    cols_pis = list(old_df.columns)
    cols_pis = [cols_pis[-1]] + cols_pis[:-1]
    old_df = old_df[cols_pis]
    return old_df


def merger_df_site_details(df1, df2, join_type):
    list_of_columns = ['Site Code', 'Client Station Code', 'Client', 'OM', 'RM']
    new_df = pd.merge(df1, df2[list_of_columns], on=['Site Code'], how=join_type)
    return new_df


def replace_value(df, column_name, to_be_replace, replaced):
    df[column_name] = df[column_name].replace(to_be_replace, replaced)
    return df


def filter_df_doesnot_equal_to_value(df, column_name, value):
    new_df = df[df[column_name] != value]
    new_df = new_df.reset_index(drop=True)
    return new_df


def deposit_remit_df(df, older_remit):
    if older_remit:
        new_column_name = 'Amount Not Deposited - Older'
    else:
        new_column_name = 'Amount Not Deposited - ' + str(today)
    if not df.empty:
        df = pd.pivot_table(df,
                            index=['scraping_date', 'scraping_time', 'Site Code', 'Client Station Code', 'Client', 'RM',
                                   'OM'],
                            values='Pending Posting',
                            aggfunc='sum',
                            fill_value=0)

        df = df.rename(columns={'Pending Posting': new_column_name})

        df = df.reset_index()

    else:
        df = pd.DataFrame()
        df['scraping_date'] = today
        df['scraping_time'] = hour
        df['Site Code'] = ''
        df['Client Station Code'] = ''
        df['Client'] = ''
        df['RM'] = ''
        df['OM'] = ''
        df[new_column_name] = ''
    return df


def create_remit_df(df, older_remit):
    if older_remit:
        new_column_name = 'Jio Only - PIS Pending Creation - Older'
    else:
        new_column_name = 'Jio Only - PIS Pending Creation - ' + str(today)
    if not df.empty:
        df = pd.pivot_table(df,
                            index=['scraping_date', 'scraping_time', 'Site Code', 'Client Station Code', 'Client', 'RM',
                                   'OM'],
                            values='PIS Pending Creation',
                            aggfunc='sum',
                            fill_value=0)

        df = df.rename(columns={'PIS Pending Creation': new_column_name})

        df = df.reset_index()

    else:
        df = pd.DataFrame()
        df['scraping_date'] = today
        df['scraping_time'] = hour
        df['Site Code'] = ''
        df['Client Station Code'] = ''
        df['Client'] = ''
        df['RM'] = ''
        df['OM'] = ''
        df[new_column_name] = ''
    return df


def create_summary(df1, df2, df3, df4):
    cms = creds_df[['Site Code', 'CMS Time']]
    column_list = ['scraping_date', 'scraping_time', 'Site Code', 'Client Station Code', 'Client', 'RM', 'OM']

    main_df = pd.merge(df1, df2, on=column_list, how='outer')
    main_df = pd.merge(main_df, df3, on=column_list, how='outer')
    main_df = pd.merge(main_df, df4, on=column_list, how='outer')
    main_df = pd.merge(main_df, cms, on=['Site Code'], how='left')
    main_df = main_df.fillna(0)

    main_df['Overall Cash Pendency - Older'] = (main_df['Amount Not Deposited - Older'] +
                                                main_df['Jio Only - PIS Pending Creation - Older'])
    main_df['Overall Cash Pendency - ' + str(today)] = (main_df['Amount Not Deposited - ' + str(today)] +
                                                        main_df['Jio Only - PIS Pending Creation - ' + str(today)])
    main_df[['Deposited To Company Account', 'Already Recovered',
             'Pending Recovery Shared', 'Legal Dispute', 'Remarks']] = ''
    main_df = main_df[['OM', 'RM', 'Site Code', 'Client Station Code', 'Client', 'CMS Time',
                       'Jio Only - PIS Pending Creation - Older',
                       'Jio Only - PIS Pending Creation - ' + str(today),
                       'Amount Not Deposited - Older', 'Amount Not Deposited - ' + str(today),
                       'Overall Cash Pendency - Older', 'Overall Cash Pendency - ' + str(today),
                       'Deposited To Company Account', 'Already Recovered',
                       'Pending Recovery Shared', 'Legal Dispute', 'Remarks']]
    return main_df


def trips_not_closed(df):
    if not df[df['Estimated Start Date'] <= df['scraping_date']].empty:
        older_df = df[df['Estimated Start Date'] <= df['scraping_date']]
    else:
        older_df = pd.DataFrame()
        older_df['Site Code'] = ''
        older_df['scraping_date'] = today
        older_df['scraping_time'] = hour
    return older_df


def failed_site_message(df, client_name):
    if df.empty:
        text = 'Automated checks are successful for all stations.'
    else:
        if not df[df['Client'].str.contains(client_name)].empty:
            failed_stations = df[df['Client'].str.contains(client_name)]['Site Code'].unique()
            failed_stns_str = str(
                str(failed_stations).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))
            text = f"""Automated checks for these <b>{len(failed_stations)}</b> \
            station(s) have failed - {failed_stns_str}.\
            Please login to the Amazon Station Command Center and conduct a manual check on their cash management."""
        else:
            text = 'Automated checks are successful for all stations.'
    return text


def open_trips_message(df):
    if not df[df['Trip Id'] != 0].empty:
        trips_count = df['Trip Id'].unique()
        trips_amount = round(df['Amount To Be Collected'].sum(), 2)
        trips_amount_str = str(trips_amount)
        trips_stations = df['Site Code'].unique()
        trips_stns_str = str(
            str(trips_stations).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))
        text = f"""Till today's date, there are <b>{len(trips_count)}</b> open Trip(s) amounting to \
        ₹ <b>₹ {trips_amount_str}/-</b> from <b>{len(trips_stations)}</b> \
                station(s) - {trips_stns_str}."""
    else:
        text = "Till today's date, there is no Open Trip."
    return text


def uncreated_message(df):
    if df.empty:
        text = "Excluding today's date, there is no PIS pending creation."
    else:
        if not df[df['Jio Only - PIS Pending Creation - Older'] != 0].empty:
            uncreated_stations_amount = round(
                df[df['Jio Only - PIS Pending Creation - Older'] != 0]['Jio Only - PIS Pending Creation - Older'].sum(),
                2)
            uncreated_amount_str = str(uncreated_stations_amount)
            uncreated_stations = df[df['Jio Only - PIS Pending Creation - Older'] != 0]['Site Code'].unique()
            uncreated_stations_str = str(
                str(uncreated_stations).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))
            text = f"""Excluding today's date, remittances of ₹ <b>₹ {uncreated_amount_str}/-</b> \
            have not been created yet from\
            for <b>{len(uncreated_stations)}</b> station(s) - {uncreated_stations_str}."""

        else:
            text = "Excluding today's date, there is no PIS pending creation."
    return text


def remittance_message(df):
    if df.empty:
        text = "Excluding today's date, there is no pending posting."
    else:
        if not df[df['Amount Not Deposited - Older'] != 0].empty:
            remit_statons_amount = round(
                df[df['Amount Not Deposited - Older'] != 0]['Amount Not Deposited - Older'].sum(), 2)
            remit_amount_str = str(remit_statons_amount)
            remit_statons = df[df['Amount Not Deposited - Older'] != 0]['Site Code'].unique()
            remit_statons_str = str(
                str(remit_statons).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))

            text = f"""Excluding today's date, remittances of <b>₹ {remit_amount_str}/-</b> \
            have not been deposited yet from <b>{len(remit_statons)}</b> station(s) - {remit_statons_str}."""
        else:
            text = "Excluding today's date, there is no pending posting."
    return text


def login_to_grab_portal(driver, login_id, login_pwd):
    """Log into the amazon logistics eu webpage using the driver,
    login_id and login_pwd passed as a parameter"""
    base_url = "https://ril.grab.in/Admin/index"
    driver.get(base_url)
    try:
        # wait till the login btn appears
        username_xpath = '//*[@id="loginform"]/div[1]/input'
        password_xpath = '//*[@id="password"]'
        login_xpath = '//*[@id="m_login_signin_submit"]'
        # WebDriverWait(driver, 60).until(EC.element_to_be_clickable(By.XPATH, login_xpath))
        driver.find_element(By.XPATH, username_xpath).send_keys(login_id)  # enters the username in username field
        driver.find_element(By.XPATH, password_xpath).send_keys(login_pwd)  # enter the pwd find the pwd field
        login_button = driver.find_element(By.XPATH, login_xpath)  # find the login button
        # using javascript executor instead of click(), to overcome ElementClickIntercepted error on server
        driver.execute_script("arguments[0].click();", login_button)  # click the login_button
    except:
        print('Login not possible!')

    return driver


def check_login_success(driver, page):
    """Checks whether the login is successful or not using the expected and current URL value.
    and returns a logged_in result as a boolean"""
    time.sleep(1)
    actual_url = driver.current_url  # current url value
    expected_url = page
    if actual_url == expected_url:
        # print('Login Success!')
        logged_in_result = True  # store the login status
    else:
        # print('Login Failed!')
        logged_in_result = False
    return logged_in_result


def get_amount_table(url, driver, trips):
    try:
        driver.get(url)
        reset_path = '//*[@id="reset_id"]'
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, reset_path)))
        reset_btn = driver.find_element(By.XPATH, reset_path)
        driver.execute_script("arguments[0].click();", reset_btn)
        search_path = '//*[@id="trip_id_search"]'
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, search_path)))
        time.sleep(2)
        search_driver = driver.find_element(By.XPATH, '//*[@id="trip_id_search"]')
        search_driver.send_keys(trips)
        search_driver.send_keys(Keys.RETURN)
        search_btn = driver.find_element(By.XPATH, '//*[@id="search_id123"]')
        driver.execute_script("arguments[0].click();", search_btn)

        templist = []
        value = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, f'//*[@id="{trips}"]/div[1]/div[2]/div/div[5]/div[9]'))).text
        trip_id = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, f'//*[@id="{trips}"]/div[1]/div[2]/div/div[1]/span'))).text
        table_dict = {'tripId': trip_id, 'amount': value}
        templist.append(table_dict)
        df = pd.DataFrame(templist)
    except:
        df = pd.DataFrame()
        df[['tripId', 'amount']] = '₹0 / ₹0'
    return df


def get_open_trip_amount(url, driver, df, login_id, login_pwd, page):
    main_df = pd.DataFrame()
    check_login_success(login_to_grab_portal(driver=driver, login_id=login_id, login_pwd=login_pwd), page=page)
    for n in range(len(df)):
        trip_id = df['tripId'][n]
        try:
            amount = get_amount_table(url=url, driver=driver, trips=trip_id)
            amount['amount'] = amount['amount'].replace('₹', '', regex=True)
            amount['amountCollected'] = amount['amount'].str.split('/', expand=True)[0]
            amount['amountToBeCollected'] = amount['amount'].str.split('/', expand=True)[1]
            amount = amount.drop(columns={'amount'})
            amount[['amountToBeCollected',
                    'amountCollected']] = amount[['amountToBeCollected', 'amountCollected']].astype(float)
            main_df = pd.concat([main_df, amount])
            main_df = main_df.reset_index(drop=True)
            print(f'{n+1}/{len(df)} - {trip_id} - amount taken')
        except:
            main_df[['amount', 'amountCollected', 'amountToBeCollected']] = 0
            main_df['tripId'] = trip_id
            print(f'{n+1}/{len(df)} - {trip_id} - amount not taken')
    driver.close()
    return main_df


if __name__ == '__main__':
    load_dotenv()
    tz = config.tz
    t1 = time.time()  # execution start time for the python script
    print('\n--------------------***--------------------\n')
    print(f'Execution Started at: {datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Test:{TEST} | Sample:{SAMPLE} | Headless: {HEADLESS}| Send Email:{SEND_EMAIL}| Send Failure Email:{SEND_FAIL_EMAIL}')
    today = datetime.now(tz).strftime("%Y-%m-%d")
    hour = datetime.now(tz).strftime("%H:%M")

    current_date = date.today()
    past_date = current_date - timedelta(days=21)
    future_date = current_date + timedelta(days=1)
    from_past_date = past_date.strftime("%d/%m/%Y")
    to_current_date = current_date.strftime("%d/%m/%Y")
    to_future_date = future_date.strftime("%d/%m/%Y")

    # Setting the time condition for evening and EOD report
    cut_off_hour = '22:00'
    con = hour >= cut_off_hour
    report_time = np.where(con, 'EOD', 'Evening')

    # File Name
    subj = f'{str(today)}_JioMartB2BCashReport_{str(report_time)}'
    data_folderpath = toolkit.create_folder(projectname=PROJECT_NAME,
                                            foldername='data')  # create the parent folder and returns the path
    final_output_fpath = f'{data_folderpath}/{subj}.xlsx'

    url_page = "https://ril.grab.in/Admin/index"
    username = os.getenv('GRAB_ID')
    password = os.getenv('GRAB_PASSWORD')
    trip_url = "https://ril.grab.in/Newcommerce/orders/historicdata"

    failed = []
    download_status_list = []
    download_status_list_all = []
    download_status_list_pis = []
    download_status_list_summary = []
    download_status_list_return = []
    df_list = []
    df_list_pis = []
    df_list_summary = []
    df_list_all = []
    df_list_return = []

    try:
        creds_df = site_login.create_site_login_creds_df(db=db_connector.connect_to_db('whizzard'))

    except Exception as e:
        print(f'Connection to DB Failed. Reason:{type(e).__name__}')
        print('Fetching Jio Sites Data from Excel sheet.')
        creds_df = pd.DataFrame()

    xl_path = f'/home/ubuntu/atom/{PROJECT_NAME}/jio_sites.xlsx' \
        if config.ON_SERVER else f'../{PROJECT_NAME}/jio_sites.xlsx'
    if creds_df.empty:
        failure_message = 'Unable to fetch Jio site details from DB! please check.'
        print(failure_message)
        failure_subj = f'Failure: {str(today)}_JioMartB2BCashReport_{str(report_time)}'
        toolkit.send_failure_email(send=SEND_FAIL_EMAIL, from_email=os.getenv('EMAIL_ID'),
                                   pwd=os.getenv('EMAIL_PASSWORD'),
                                   receiver_email='mrjiteshjadhao@gmail.com',
                                   email_subject=failure_subj, email_message=failure_message)
        creds_df = pd.read_excel(xl_path)
    else:  # if active_sites_df is not empty, store the df as excel to get the updated data for future use
        creds_df.to_excel(xl_path, index=False)
        print('Jio Sites DataFrame has been stored as an Excel file.')

    creds_df['clientName'] = creds_df['clientName'].fillna('Jio')
    creds_df = creds_df.rename(columns={'siteName': 'Site Name', 'siteCode': 'Site Code',
                                        'clientSiteCode': 'Client Station Code',
                                        'client': 'client_portal',
                                        'clientName': 'Client',
                                        'omName': 'OM', 'rmName': 'RM',
                                        'userName': 'UserName',
                                        'password': 'Password',
                                        'timeStr': 'CMS Time',
                                        'active': 'Active'})

    creds_df = filter_df_equal_to_value(df=creds_df, column_name='Active', value=True)
    creds_df = filter_df_keywords(creds_df, 'Client', 'Qwik|Reliance')
    print(f'Total Number of Sites found: {str(len(creds_df))}')

    if SAMPLE:
        creds_df = creds_df.head(2)
    print(f'Number of Sites for scraping: {str(len(creds_df))}')

    for i in range(len(creds_df)):
        stn_code = str(creds_df['UserName'][i])[0:4]
        stn_id = creds_df['UserName'][i]
        site_code = creds_df['Site Code'][i]
        print(f'{i+1}/{len(creds_df)} - {stn_code}', end=' - ')

        try:
            result = get_jio_trip_summary(station_code=stn_code, user_id=stn_id, from_date=from_past_date,
                                          to_date=to_future_date)
            download_status_list.append(get_the_download_status(stn_code, result['status']))
            df_raw = result['df']
            df_raw = df_column_additions(df=df_raw)
            df_list.append(df_raw)

            result = get_jio_trip_all(station_code=stn_code, user_id=stn_id, from_date=from_past_date,
                                      to_date=to_future_date)
            download_status_list_all.append(get_the_download_status(stn_code, result['status']))
            df_raw = result['df']
            df_raw = df_column_additions(df=df_raw)
            df_list_all.append(df_raw)

            result_pis = get_jio_pis_summary(station_code=stn_code, user_id=stn_id)
            download_status_list_pis.append(get_the_download_status(stn_code, result_pis['status']))
            df_raw_pis = result_pis['df']
            df_raw_pis = df_column_additions(df=df_raw_pis)
            df_list_pis.append(df_raw_pis)

            result_summary = get_jio_manage_pis(station_code=stn_code, user_id=stn_id, from_date=from_past_date,
                                                to_date=to_future_date)
            download_status_list_summary.append(get_the_download_status(stn_code, result_pis['status']))
            df_raw_summary = result_summary['df']
            df_raw_summary = df_raw_pis = df_column_additions(df=df_raw_summary)
            df_list_summary.append(df_raw_summary)

            result = get_return_trip(station_code=stn_code, user_id=stn_id, from_date=from_past_date,
                                     to_date=to_current_date)
            download_status_list_return.append(get_the_download_status(stn_code, result['status']))
            df_raw = result['df']
            df_raw = df_column_additions(df=df_raw)
            df_list_return.append(df_raw)

        except Exception as e:
            error_name = type(e).__name__
            print(f'scraping failed! Reason: {error_name}!')
            failed.append(stn_code)

    creds_station = []
    for i in range(len(creds_df)):
        creds_station.append(str(creds_df['UserName'][i])[0:4])
    creds_df['station_code'] = creds_station

    failed_sites = pd.DataFrame()
    failed_sites['station_code'] = failed

    if len(failed) != len(creds_df):
        print(len(failed))
        print(len(creds_df))
        if not failed_sites.empty:
            failed_sites['scraping_date'] = today
            failed_sites['scarping_time'] = hour
            failed_sites = pd.merge(failed_sites, creds_df[['station_code', 'Site Code', 'Client Station Code', 'Client', 'OM', 'RM']],
                                    on=['station_code'], how='left')

        else:
            failed_sites['Site Code'] = ''
            failed_sites['scraping_date'] = ''
            failed_sites['scraping_time'] = ''

        status_df = pd.DataFrame(download_status_list)
        status_all_df = pd.DataFrame(download_status_list_all)
        status_pis_df = pd.DataFrame(download_status_list_pis)
        status_summary_df = pd.DataFrame(download_status_list_summary)
        status_return_df = pd.DataFrame(download_status_list_return)

        pis_df = pd.concat(df_list_pis)
        pis_df = rename_columns(df=pis_df, df_type='pis')
        pis_df = rearrange_columns(df=pis_df, df_type='pis')
        pis_df = merger_df_site_details(df1=pis_df, df2=creds_df, join_type='left')

        summary_df = pd.concat(df_list_summary)
        summary_df = rename_columns(df=summary_df, df_type='summary')
        summary_df = rearrange_columns(df=summary_df, df_type='summary')
        summary_df = merger_df_site_details(df1=summary_df, df2=creds_df, join_type='left')

        return_df = pd.concat(df_list_return).reset_index(drop=True)
        return_df = format_date_columns(return_df, 'Time')
        return_df = merger_df_site_details(df1=return_df, df2=creds_df, join_type='left')

        trip_df = pd.concat(df_list).reset_index(drop=True)
        trip_df = format_date_columns(trip_df, 'Time')

        trip_all_df = pd.concat(df_list_all).reset_index(drop=True)
        trip_all_df = format_date_columns(trip_all_df, 'Time')
        print(f'No of all trips: {len(trip_all_df)}')
        trip_all_for_amount_df = trip_all_df[trip_all_df['status'].str.contains('LOADING_COMPLETED|SETTLEMENT_PENDING|PARTIALLY_SETTLED|INTRANSIT')].reset_index(drop=True)
        trip_all_for_amount_df = pd.concat([trip_all_for_amount_df, trip_df]).reset_index(drop=True)
        trip_all_for_amount_df = trip_all_for_amount_df.drop_duplicates(subset='tripId').reset_index(drop=True)
        print(f'No of trips for which amount is to be taken: {len(trip_all_for_amount_df)}')

        if not trip_all_for_amount_df.empty:
            browser = toolkit.get_driver(downloads_folder=data_folderpath, headless=HEADLESS)
            trip_all_amount_df = get_open_trip_amount(url=trip_url, df=trip_all_for_amount_df, driver=browser,
                                                      login_id=username, login_pwd=password, page=url_page)

        else:
            trip_all_amount_df = pd.DataFrame()
            trip_all_amount_df[['amount', 'amountCollected', 'amountToBeCollected', 'tripId']] = ''

        trip_all_df = pd.merge(trip_all_df, trip_all_amount_df, on='tripId', how='left')
        trip_all_df = merger_df_site_details(df1=trip_all_df, df2=creds_df, join_type='left')
        trip_all_df[['amountCollected', 'amountToBeCollected']] = trip_all_df[['amountCollected', 'amountToBeCollected']].fillna(0)
        trip_all_df = trip_all_df.drop(columns={'hub'})
        if not trip_df.empty:
            trip_amt_collect_dict = dict(zip(trip_all_amount_df['tripId'], trip_all_amount_df['amountCollected']))
            trip_amt_to_be_collect_dict = dict(zip(trip_all_amount_df['tripId'], trip_all_amount_df['amountToBeCollected']))
            trip_df['amountCollected'] = trip_df['tripId'].map(trip_amt_collect_dict)
            trip_df['amountToBeCollected'] = trip_df['tripId'].map(trip_amt_to_be_collect_dict)

            trip_df = rename_columns(df=trip_df, df_type='open_trips')
            trip_df['Estimated Start Date'] = trip_df['Estimated Start Time'].apply(lambda x: convert_date(x))
            trip_df = rearrange_columns(df=trip_df, df_type='open_trips')
            trip_df = merger_df_site_details(df1=trip_df, df2=creds_df, join_type='left')
        else:
            trip_df = pd.DataFrame()
            trip_df[['Trip Id', 'Estimated Start Date', 'scraping_date', 'scraping_time', 'Site Code',
                     'Client Station Code', 'Amount To Be Collected', 'Client', 'OM', 'RM']] = ''

        pis_df.rename(columns={'Settlement Date': 'Settlement Date_portal'}, inplace=True)
        summary_df.rename(columns={'Created Date': 'Created Date_portal'}, inplace=True)

        pis_df = format_date_column_jio(old_df=pis_df, column_name='Settlement Date')
        summary_df = format_date_column_jio(old_df=summary_df, column_name='Created Date')

        summary_df = replace_value(summary_df, 'Document ID', '-', 'Missing')
        summary_df = replace_value(summary_df, 'Posted By', '-', 'Missing')
        summary_df = replace_value(summary_df, 'Posted Date', '-', 'Missing')

        pis_pending_creation_older = filter_df_doesnot_equal_to_value(df=pis_df, column_name='Settlement Date',
                                                                      value=pis_df['scraping_date'])
        pis_pending_posting_older = filter_df_doesnot_equal_to_value(df=pis_pending_creation_older,
                                                                     column_name='Pending Posting',
                                                                     value=0)

        pis_pending_creation_current = filter_df_equal_to_value(df=pis_df, column_name='Settlement Date',
                                                                value=pis_df['scraping_date'])
        pis_pending_creation_current = filter_df_doesnot_equal_to_value(pis_pending_creation_current,
                                                                        'PIS Pending Creation', 0)

        pis_pending_creation_older = create_remit_df(df=pis_pending_creation_older, older_remit=True)
        pis_pending_creation_current = create_remit_df(df=pis_pending_creation_current, older_remit=False)

        pis_pending_posting_older = filter_df_doesnot_equal_to_value(df=pis_df, column_name='Settlement Date',
                                                                     value=pis_df['scraping_date'])
        pis_pending_posting_older = filter_df_doesnot_equal_to_value(df=pis_pending_posting_older,
                                                                     column_name='Pending Posting',
                                                                     value=0)
        pis_pending_posting_current = filter_df_equal_to_value(df=pis_df, column_name='Settlement Date',
                                                               value=pis_df['scraping_date'])
        pis_pending_posting_current = filter_df_doesnot_equal_to_value(df=pis_pending_posting_current,
                                                                       column_name='Pending Posting',
                                                                       value=0)

        pis_pending_posting_older = deposit_remit_df(df=pis_pending_posting_older, older_remit=True)
        pis_pending_posting_current = deposit_remit_df(df=pis_pending_posting_current, older_remit=False)

        summary = create_summary(df1=pis_pending_creation_older, df2=pis_pending_creation_current,
                                 df3=pis_pending_posting_older, df4=pis_pending_posting_current)

        summary_ril = filter_df_keywords(df=summary, column_name='Client', keyword='Reliance')
        summary_qs = filter_df_keywords(df=summary, column_name='Client', keyword='Qwik')

        if not trip_df.empty:
            trip_started_older = trips_not_closed(df=trip_df)
            trip_ril = filter_df_keywords(df=trip_started_older, column_name='Client', keyword='Reliance')
            trip_qs = filter_df_keywords(df=trip_started_older, column_name='Client', keyword='Qwik')
        else:
            trip_started_older = trip_df
            trip_ril = trip_df
            trip_qs = trip_df

        failed_site_message_ril = failed_site_message(df=failed_sites, client_name='Reliance')
        failed_site_message_qs = failed_site_message(df=failed_sites, client_name='Qwik')
        open_trips_message_ril = open_trips_message(df=trip_ril)
        open_trips_message_qs = open_trips_message(df=trip_qs)
        remittance_message_ril = remittance_message(df=summary_ril)
        remittance_message_qs = remittance_message(df=summary_qs)
        uncreated_message_ril = uncreated_message(df=summary_ril)
        uncreated_message_qs = uncreated_message(df=summary_qs)

        with pd.ExcelWriter(final_output_fpath, engine=None) as writer:
            failed_sites.to_excel(writer, sheet_name='Scraping Failed', index=False)
            summary.to_excel(writer, sheet_name='Summary - COD OM wise', index=False)
            summary_df.to_excel(writer, sheet_name='Manage PIS', index=False)
            pis_df.to_excel(writer, sheet_name='PIS Summary', index=False)
            trip_df.to_excel(writer, sheet_name='Trips Started', index=False)
            trip_all_df.to_excel(writer, sheet_name='All Trips', index=False)
            # trip_all_amount_df.to_excel(writer, sheet_name='All Trips Amount', index=False)
            return_df.to_excel(writer, sheet_name='Return Trips', index=False)

        if TEST:
            TO = ['mrjiteshjadhao@gmail.com']
            CC = ['mrjiteshjadhao@gmail.com']
        else:
            TO = ['mrjiteshjadhao@gmail.com']
            CC = ['jitesharvindjadhao1999@gmail.com']

        if hour >= cut_off_hour:
            message = [f"""Hi Team,\n\nPlease find attached cash management report for \
                <i><b>Reliance Jio Mart and Qwik Supply Chain Stations</i></b>,\
                this is for your information and action.\
                Here is the summary of today's report:\n\
                <ul><b>For Reliance Jio Mart:</b>
                <ol><li>{failed_site_message_ril}</li>\
                <li>{open_trips_message_ril}</li>\
                <li>{remittance_message_ril}</li>\
                <li>{uncreated_message_ril}</li></ol></ul>\
                <ul><b>For Qwik Supply Chain Stations:</b>\
                <ol><li>{failed_site_message_qs}</li>\
                <li>{open_trips_message_qs}</li>\
                <li>{remittance_message_qs}</li>\
                <li>{uncreated_message_qs}</li></ol></ul>\n\
                Regards\nKriti Gupta\nAnalytics Team"""]

        else:
            message = [f"""Hi Team,\n\nPlease find attached cash management report for \
            <i><b>Reliance Jio Mart and Qwik Supply Chain Stations</i></b>,\
            this is for your information and action.\
            Here is the summary of today's report:\n\
            <ul><b>For Reliance Jio Mart:</b>
            <ol><li>{failed_site_message_ril}</li>\
                <li>{open_trips_message_ril}</li>\
            <li>{remittance_message_ril}</li>\
            <ul><b>For Qwik Supply Chain  Stations:</b>\
            <ol><li>{failed_site_message_qs}</li>\
                <li>{open_trips_message_qs}</li>\
            <li>{remittance_message_qs}</li></ol></ul>\n\
            Regards\nKriti Gupta\nAnalytics Team"""]

        from_address = os.getenv('EMAIL_ID')

        toolkit.send_email(send=SEND_EMAIL, from_email=from_address,
                           pwd=os.getenv('EMAIL_PASSWORD'),
                           receiver_email=TO, copy=CC, email_subject=subj,
                           email_message=message, attachment_file=[final_output_fpath])

        s3_storage = s3c.connect_to_s3_storage(os.getenv('AWS_ACCESS_KEY_ID'),
                                               os.getenv('AWS_SECRET_ACCESS_KEY'))
        atom_bucket = s3_storage.Bucket('atom-s3')  # selecting a bucket from the s3 storage

        if not SAMPLE:  # upload final output into s3 bucket, only if it's not sample
            s3c.upload_to_s3(atom_bucket, 'jio-cash-management-check', final_output_fpath, f'{subj}.xlsx')
    else:
        scraping_fail_message = 'Unable to get data! please check and update cookies.'
        print(scraping_fail_message)
        scraping_fail_subj = f'Failure: {subj}'
        toolkit.send_failure_email(send=SEND_FAIL_EMAIL, from_email=os.getenv('EMAIL_ID'),
                                   pwd=os.getenv('EMAIL_PASSWORD'),
                                   receiver_email='mrjiteshjadhao@gmail.com',
                                   email_subject=scraping_fail_subj, email_message=scraping_fail_message)

    print(f'Total Time for the Complete Execution : {(time.time() - t1) / 60:.3f} minutes')
    print(f'Execution Completed at: {datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")}')
