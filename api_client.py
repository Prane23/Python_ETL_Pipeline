import requests

#extract function to get data from api      
def extract(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()  
