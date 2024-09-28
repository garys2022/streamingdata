import requests
import json

def flood_api_station():
    url = "https://environment.data.gov.uk/flood-monitoring/id/stations/1491TH"
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.get(
        url=url, headers=headers
    )
    return json.loads(response.content)