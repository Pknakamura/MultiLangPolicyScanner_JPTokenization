import requests
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
import pandas as pd
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tinydb import TinyDB, Query
from tinyrecord import transaction
from datetime import datetime

# Ensure consistent results from langdetect
DetectorFactory.seed = 0

# List of user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3', 
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
    # Add more user agents as needed
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

# get free proxies from online
def get_proxies():
    url = "https://free-proxy-list.net/"
    # fetch proxy list from online
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    proxy_table = soup.find("table", attrs={"class": "table table-striped table-bordered"})


    # print(proxy_table)

    # # Extract proxy IPs and ports
    proxies = []
    proxy_table = proxy_table.find("tbody")
    # print(proxy_table)
    for row in proxy_table.find_all("tr"):
        proxies.append({
        "ip":   row.find_all("td")[0].string,
        "port": row.find_all("td")[1].string
        })
        # print(row)
    return proxies

def fetch_and_convert_website(url):
    try:
        headers = {'User-Agent': get_random_user_agent()}
        # Fetch the website content
        # try to request with https first and if it fails, try with http
        # this is to avoid the error when the website is not available with https

        # response = requests.get("https://" + url, headers=headers, timeout=10)
        # response.raise_for_status()  # Check if the request was successful
        # if response.status_code != 200:
        #     response = requests.get("http://" + url, headers=headers, timeout=10)
        #     response.raise_for_status()

        # add proxy
        proxies = get_proxies()
        # get random proxy
        # pick random value from list
        proxy = random.choice(proxies)
        response = requests.get("https://" + url, headers=headers, proxies=proxy, timeout=10)
        # response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Check if the request was successful
        
        # Parse the website content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Get text content and remove extra whitespace
        text = soup.get_text(separator=' ', strip=True)
        
        return text
    except requests.exceptions.RequestException as e:
        error_message = f"Error fetching the website {url}: {e}"
        save_error(url, error_message)
        print(error_message)
        return None

def detect_language(text):
    try:
        language = detect(text)
        return language
    except LangDetectException as e:
        error_message = f"Error detecting language: {e}"
        print(error_message)
        return None

def process_single_website(url, db):
    text_content = fetch_and_convert_website(url)
    if text_content:
        language = detect_language(text_content)
        if language:
            with transaction(db) as tr:
                tr.insert({'url': url, 'language': language, 'timestamp': datetime.now().isoformat()})
            return url, language
    return url, None

def process_websites(url_list, db):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_single_website, url, db): url for url in url_list}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing URLs"):
            future.result()

def save_error(url, error_message):
    with open('error_urls.txt', 'a', encoding='utf-8') as error_file:
        error_file.write(f"{url}: {error_message}\n")

def create_url_list_from_file(file_path):
    if 'cloudflare' in file_path:
        df = pd.read_csv(file_path)
        return df['domain'].tolist()

    if 'ahref' in file_path:
        df = pd.read_csv(file_path)
        return df['url'].tolist()
    
    if 'tranco' in file_path:
        # there is no header in the file
        df = pd.read_csv(file_path, header=None)
        return df[1].tolist()

    with open(file_path, 'r') as file:
        url_list = file.readlines()
    return [url.strip() for url in url_list]

def load_processed_urls(db):
    processed_urls = {item['url'] for item in db.all()}
    return processed_urls

def read_error_urls():
    with open('error_urls.txt', 'r', encoding='utf-8') as error_file:
        error_urls = error_file.readlines()
        # split the lines into URL and error message
        error_urls = [line.split(':') for line in error_urls]
    
    # just keep the url in the list
    error_urls = [url[0] for url in error_urls]
    return error_urls

if __name__ == "__main__":
    
    # print(read_error_urls())
    error_url = read_error_urls()

    # Initialize TinyDB
    db = TinyDB('websites_by_language.json')
    websites_table = db.table('websites')
    Websites = Query()

    # # List of URLs to process
    url_list = [
        # 'https://www.example.com',
        # 'https://www.anotherexample.com',
        # Add more URLs as needed
    ]

    # comment this line when running the script for the first time OR when you want to process all the URLs again
    url_list.extend(error_url)

    # List all files in the data folder
    for file in os.listdir("../data"):
        file_list = create_url_list_from_file("../data/" + file)
        url_list.extend(file_list)


    # Remove duplicates
    url_list = list(set(url_list))
    print(f"Total URLs before filtering: {len(url_list)}")

    # Load already processed URLs from TinyDB
    processed_urls = load_processed_urls(websites_table)
    print(f"Total processed URLs: {len(processed_urls)}")

    # Remove already processed URLs from url_list
    url_list = [url for url in url_list if url not in processed_urls]
    print(f"Total URLs to process: {len(url_list)}")

    # Process the websites and update TinyDB
    process_websites(url_list, websites_table)
