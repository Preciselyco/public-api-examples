from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from dotenv import load_dotenv

import requests
import sys
import os
import time


ONE_MINUTES = 60

#@sleep_and_retry
@on_exception(expo, RateLimitException, max_time=60)
@limits(calls=30, period=ONE_MINUTES)
def call_api(url, headers):
    print(f"call_api: {url}")
    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        # X-Ratelimit-Limit: 30
        # X-Ratelimit-Remaining: 29
        # X-Ratelimit-Reset: 1637048940
        resetTime = int(response.headers["X-Ratelimit-Reset"])
        timeUntilReset = resetTime - time.time() 
        raise RateLimitException('API response: {}'.format(response.status_code), timeUntilReset)
    if response.status_code != 200:
        raise Exception('API response: {}'.format(response.status_code))
    
    return response

def fetch_all_documents(token, papiURL, orgId, pageSize):
    documents = dict()
    headers = {'Authorization': 'Token ' + token}
    resp = call_api(f"{papiURL}/organizations/{orgId}/documents?limit={pageSize}", headers)
    totalDocuments = int(resp.headers["Pagination-Total-Hits-Count"])
    totalPages = int(resp.headers["Pagination-Page-Count"])
    currentPage = int(resp.headers["Pagination-Page-Current"])
    print(f"Organization: {orgId} has {totalDocuments} documents and with page size: {pageSize} it will take {totalPages} requests to get all documents.")

    data = resp.json()
    for document in data:
        print(document["id"])
        documents[document["id"]] = document

    while currentPage < totalPages: #iterate pages
        resp = call_api(f"{papiURL}/organizations/{orgId}/documents?limit={pageSize}&page={currentPage+1}", headers)
        currentPage = int(resp.headers["Pagination-Page-Current"])
        data = resp.json()
        for document in data:
            print(document["id"])
            documents[document["id"]] = document

    noDocuments = len(documents.keys())
    print(f"Got {noDocuments} of {totalDocuments}")

def main(argv):
    load_dotenv()
    print("Precisely Public-API client\n\n")
    papiURI = os.environ.get('PAPI_URI')
    papiOrganization = os.environ.get('PAPI_ORGANIZATION')
    papiUser = os.environ.get('PAPI_USER')
    papiPass = os.environ.get('PAPI_PASSWORD')

    response = requests.post(f"{papiURI}/authenticate",json={"username": papiUser, "password": papiPass})
    token = response.json()["accessToken"]

    fetch_all_documents(token, papiURI, papiOrganization, 5)


if __name__ == "__main__":
    main(sys.argv[1:])