# This script processes Korean, Chinese, Mandarin, and English websites to extract home page and internal links.

# CHANGES AND IMPROVEMENTS:
# 1. Increased Parallel Processing:
#    - Increased the number of parallel processes to 20 using `ThreadPoolExecutor`.
# 2. Time-Limited Domain Processing:
#    - Implemented a logic to stop processing a domain if it takes more than 5 minutes and store whatever has been collected so far.
# 3. Added Comments for Better Understanding:
#    - Added new comments to explain the changes and removed previous changes comments.
# 4. Filtered Search Keywords:
#    - Updated the search keywords to include only Chinese, Korean, Mandarin, and English.

import requests
from bs4 import BeautifulSoup
import random
import time
from urllib.parse import urlparse, urljoin
from tinydb import TinyDB, Query
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

keywords_primary = [
    'privac', 'poli', 'ethic', 'terms', 'servic', 'policy', 'data', 'safety',  # English
    '隐私', '政策', '条款', '服务', '数据', '安全',  # Chinese
    '개인정보', '정책', '이용약관', '서비스', '데이터', '안전'  # Korean
]

visited_links = set()
all_links = []

def get_random_user_agent():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'
    ]
    return random.choice(user_agents)

def extract_home_page_links(url):
    headers = {'User-Agent': get_random_user_agent()}
    try:
        response = requests.get(url, timeout=5, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        links = set()
        
        for tag in soup.find_all(["a", "img", "script", "iframe", "form"], href=True, src=True, action=True):
            link = tag.get("href") or tag.get("src") or tag.get("action")
            if link:
                links.add(urljoin(url, link))
        
        return list(links)
    
    except requests.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return []

def extract_links(url):
    headers = {'User-Agent': get_random_user_agent()}
    try:
        response = requests.get(url, timeout=5, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        links = []
        
        for link in soup.find_all("a", href=True):
            full_link = urljoin(url, link.get("href"))
            if full_link and full_link.startswith("http"):
                links.append(full_link)
        return links
    
    except requests.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return []

def recursive_extract(url, original_domain, max_depth=3, current_depth=0, start_time=None):
    if current_depth > max_depth:
        return
    if start_time and datetime.now() - start_time > timedelta(minutes=3):
        print(f"Stopping recursion for {original_domain} after 5 minutes")
        return
    if url not in visited_links:
        visited_links.add(url)
        if original_domain in url:
            print(f"Extracting links from: {url}")
            links = extract_links(url)
            for link in links:
                parsed_link = urlparse(link)
                if parsed_link.netloc.endswith(original_domain):
                    all_links.append(link)
                    recursive_extract(link, original_domain, max_depth, current_depth + 1, start_time)
                    time.sleep(1)

def process_domains(domains, country, max_depth=3):
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_domain = {executor.submit(process_single_domain, domain, country, max_depth): domain for domain in domains}
        for future in as_completed(future_to_domain):
            domain = future_to_domain[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error processing domain {domain}: {e}")

def process_single_domain(domain, country, max_depth):
    global visited_links, all_links
    visited_links = set()
    all_links = []
    home_links = []
    start_time = datetime.now()
    try:
        print(f"Processing domain: {domain}")
        recursive_extract(f"https://{domain}", domain, max_depth, start_time=start_time)
        home_links = extract_home_page_links(f"https://{domain}")
    except Exception as e:
        print(f"Error processing domain https://{domain}: {e}")
        try:
            recursive_extract(f"http://{domain}", domain, max_depth, start_time=start_time)
            home_links = extract_home_page_links(f"http://{domain}")
        except Exception as e:
            print(f"Error processing domain http://{domain} with http: {e}")

    db = TinyDB('websites_by_language.json')
    table = db.table('policy_links')
    timeprocessed = datetime.now().isoformat()
    home_links = list(set(home_links))
    all_links = list(set(all_links))
    data = {
        'domain': domain,
        'home_links': home_links,
        'all_links': all_links,
        'timeprocessed': timeprocessed,
        'country': country
    }
    table.insert(data)

def get_remaining_domains(country_code):
    db = TinyDB('websites_by_language.json')
    websites_table = db.table('websites')
    Website = Query()

    korean_websites = websites_table.search(Website.language == country_code)

    all_korean_websites = [website['url'] for website in korean_websites]
    print(f"Total {country_code} websites: {len(set(all_korean_websites))}")

    # websites_to_process = korean_websites[:100]
    # websites_to_process = [website['url'] for website in websites_to_process]
    # print(websites_to_process)

    policy_links_table = db.table('policy_links')
    existing_policy_links = policy_links_table.all()
    # only korean domains which has policy_link['country'] == "Korea"

    if country_code == "ko":
        existing_policy_links = [policy_link['domain'] for policy_link in existing_policy_links if policy_link['country'] == "Korea"]
    elif country_code == "zh-cn" or country_code == "zh":
        existing_policy_links = [policy_link['domain'] for policy_link in existing_policy_links if policy_link['country'] == "China"]
    elif country_code == "ja":
        existing_policy_links = [policy_link['domain'] for policy_link in existing_policy_links if policy_link['country'] == "Japan"]

    # existing_policy_links = [policy_link['domain'] for policy_link in existing_policy_links if policy_link['country'] == "Korea"]
    # existing_policy_links = [policy_link['domain'] for policy_link in existing_policy_links]
    # existing_policy_links = [policy_link['domain'] for policy_link in existing_policy_links]

    print(f"Number of {country_code} domains in policy links table: {len(set(existing_policy_links))}")

    remaining_websites = []

    for each_website in all_korean_websites:
        if each_website not in existing_policy_links:
            remaining_websites.append(each_website)
    print(f"Total remaining website {len(remaining_websites)}")

    return remaining_websites

if __name__ == "__main__":

    ko_websites_to_process = get_remaining_domains("ko")
    process_domains(ko_websites_to_process, "Korea")

    # chinese_websites_to_process = get_remaining_domains("zh-cn")
    # mandarin_websites_to_process = get_remaining_domains("zh")
    # chinese_mandarin_websites_to_process = chinese_websites_to_process + mandarin_websites_to_process

    # japanese_websites_to_process = get_remaining_domains("ja")
    # process_domains(japanese_websites_to_process, "Japan")



