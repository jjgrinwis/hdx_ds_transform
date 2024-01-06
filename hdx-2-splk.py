import clickhouse_connect # The official clickhouse maintained library, other libraries are available
import os
import json
import requests
from time import perf_counter

hec_event = {}
event_list = ""

# Created a separate readlonly username in Hydrolix and make sure to store readonly credentials as env. vars.
hdx_username = os.environ['HDX_USERNAME']
hdx_password = os.environ['HDX_PASSWORD']
hec_token = os.environ['HEC_TOKEN']

# Setting the Splunk HEC token and using the event endpoint, not the raw one.
# Using the event one to add some extra metadata to every event.
headers = {'Authorization':f'Splunk {hec_token}'}
url = 'http://splunk.great-demo.com:8088/services/collector/event'

# get data from Hydrolix from the last # of hours
last_hours = 1

# Our Hydrolix host with a valid certificate and query authentication enabled in hydrolixcluster.yaml k8s file.
# spec:
#   acme_enabled: true
#   enable_query_auth: true
hdx_hostname = 'hydrolix.great-demo.com'

t1_start = perf_counter()
# The clickhouse_connect driver is using http, not the jdbc/clickhouse interface which some other libraries are  using.
# to lookup port use: 'k get services traefik -n hydrolix -o json | jq '.spec.ports[]|select(.name=="clickhouse-http").port'' 
client = clickhouse_connect.get_client(host=hdx_hostname, 
                                       port=8088, 
                                       interface='https', 
                                       username=hdx_username, 
                                       password=hdx_password
                                       )

# Create your own hydrolix/clickhouse SQL query to get data that's interesting to you
# You can test your SQL query on https://play.clickhouse.com/
query = f"""
    SELECT reqTimeSec, reqHost, reqPath, reqId, statusCode, cliIP, cacheStatus 
    FROM my_test_project.ds2 
    WHERE reqHost='api.grinwis.com' AND reqTimeSec > subtractHours(now(), {last_hours})
"""
# Use client.query_df to lookup the required data which will return a nice Pandas dataframe with the results
result = client.query_df(query)
t1_stop = perf_counter()
print(f"hydrolix lookup took {t1_stop - t1_start:.2f} secs")

# let's build our splunk HEC JSON even with some extra metadata
# We added some extra metadata to the JSON HEC event like the data which is the same as the request date.
# {"time":1704455995.000, "host":"hydrolix.great-demo.com", "source": "hdx-2-splunk","event": { "reqTimeSec": 1704455995000, "reqHost": "api.grinwis.com", "reqPath": "headers", "statusCode": "800" }}
# {"time":1704455995.000, "host":"hydrolix.great-demo.com", "source": "hdx-2-splunk","event": { "reqTimeSec": 1704455995000, "reqHost": "api.grinwis.com", "reqPath": "headers", "statusCode": "801" }}
if not result.empty:
    for index, row in result.iterrows():
        # If there is a reqTimeSec field it's of type Timestamp.
        # Set the time of the HEC event to the same time as it was injected into Datastream
        # without time metadata field it will use the time of injection into the HEC
        if 'reqTimeSec' in result.columns:
            hec_event['time'] = row['reqTimeSec'].timestamp()

        # Some general HEC metadata to add to our event
        # https://docs.splunk.com/Documentation/SplunkCloud/9.1.2308/Data/FormateventsforHTTPEventCollector
        hec_event['host'] = "hydrolix.great-demo.com"
        hec_event['source'] = "hdx-2-splunk"

        # convert the row to JSON which will automatically fix any Timestamp type fields.
        hec_event['event'] = row.to_json()

        # HEC expects a call with one or more individual JSON events messages so just create one string with all events.
        event_list += json.dumps(hec_event)

    # now forward the events to our HEC interface
    t2_start = perf_counter()
    with requests.Session() as s:
        r = s.post(url, data=event_list, headers=headers)
        r.raise_for_status
        
        if r.status_code == requests.codes.ok:
            t2_stop = perf_counter()
            print(f"splunk upload took {t2_stop - t2_start:.2f} secs")
            print(f'{len(result.index)} records uploaded')

        else:
            print(f"something went wrong: {r.status_code}:{r.json()}")

else:
    print("no records found")
