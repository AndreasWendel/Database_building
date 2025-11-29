import requests
import json

url = "https://www.wikitable2json.com/api/List_of_S%26P_500_companies?table=0"
try:
    response = requests.get(url)
    data = response.json()
    print("Status Code:", response.status_code)
    print("Data Type:", type(data))
    if isinstance(data, list):
        print("List length:", len(data))
        if len(data) > 0:
            print("First item type:", type(data[0]))
            print("First item keys/content (truncated):", str(data[0])[:200])
    elif isinstance(data, dict):
        print("Keys:", data.keys())
    else:
        print("Data:", data)
except Exception as e:
    print(f"Error: {e}")
