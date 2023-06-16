#

import requests
import datetime
from google.cloud import bigquery

PROJECT_ID = '[gcp_project_id]'
BQ_DATASET = 'simplelogin'
BQ_TABLE = 'activities'
BQ = bigquery.Client()


key = "[simplelogin_key]"
headers = {'Authentication':key}

aliases = []
page_id = 0

while True:
    req = requests.get(f'https://app.simplelogin.io/api/v2/aliases?page_id={page_id}',headers=headers)
    aa = req.json()["aliases"]
    for a in aa:
        aliases.append({'id':a['id'],'email':a['email']})
    if len(aa)<=0:
        break
    page_id = page_id+1

activity = []
for alias in aliases:
    page_id = 0
    while True:
        req = requests.get(f'https://app.simplelogin.io/api/aliases/{alias["id"]}/activities?page_id={page_id}',headers=headers)
        activities = req.json()["activities"]
        for aa in activities:
            date = datetime.datetime.fromtimestamp(aa['timestamp']).date()
            if date == (datetime.date.today()-datetime.timedelta(days = 1)):
                act = {'id':alias['id'],'email':alias['email'],'action':aa['action'],'from':aa['from'],'timestamp':aa['timestamp'],'to':aa['to'],'reverse_alias':aa['reverse_alias'],'reverse_alias_address':aa['reverse_alias_address']}
                activity.append(act)
        if len(activities)<=0:
            break
        page_id = page_id+1
for aa in activity:
    print(aa)
table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
errors = BQ.insert_rows_json(table,activity)
print(errors)
return null
