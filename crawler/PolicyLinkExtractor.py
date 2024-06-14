import requests
from bs4 import BeautifulSoup
import random
import time
from urllib.parse import urlparse
from tinydb import TinyDB, Query
from datetime import datetime

keywords_1 = [
        'privac', 'poli', 'ethic', 'terms', 'servic', 'policy', 'data', 'safety',  # English
        'プライバシー', 'ポリシー', '利用規約', 'サービス', 'データ', '安全',  # Japanese
        '隐私', '政策', '条款', '服务', '数据', '安全',  # Chinese
        '개인정보', '정책', '이용약관', '서비스', '데이터', '안전'  # Korean
    ]
keywords_2 = [u"adatkezel", u"adatvédel", u"agb", u"andmekaitse", u"asmens duomenų", u"bedingungen", u"bảo mật", u"c.g.", u"cg", u"cgu", u"cgv", u"condicion", u"condiciones", u"conditii", u"conditions", u"condizioni", u"condições", u"confidentialitate", u"confidentialite", u"confidentialité", u"confidențialitate", u"cookie", u"cosaint sonraí", u"cosanta sonraí", u"dados pessoais", u"dane osobowe", u"data policy", u"data protection", u"datapolicy", u"datapolitik", u"datenrichtlinie", u"datenschutz", u"dati personali", u"datos personales", u"direitos do titular dos dados", u"disclaimer", u"donnees personnelles", u"données personnelles", u"duomenų sauga", u"eväste", u"feltételek", u"fianáin", u"fianán", u"galetes", u"gdpr", u"gegevensbeleid", u"gegevensbescherming", u"gizlilik", u"henkilötie", u"hinweis", u"informationskapslar", u"integritet", u"isikuandmete", u"jogi nyilatkozat", u"jogi tudnivalók", u"juriidili", u"kakor", u"ketentuan", u"kişisel verilerin", u"kolačić", u"konfidencialiteti", u"konfidencialumas", u"konfidentsiaalsus", u"koşulları", u"kvkk", u"käyttöehdot", u"küpsis", u"legal", u"légal", u"mbrojtja e të dhënave", u"nan", u"naudojimo taisyklės", u"naudotojo sutartis", u"noteikumi", u"obradi podataka", u"ochrana dat", u"ochrana údajov", u"ochrona danych", u"offenlegung", u"osebnih podatkov", u"osobnih podataka", u"osobné údaje", u"osobních údajů", u"osobných údajov", u"pedoman", u"persondata", u"personlige data", u"personlige oplysninger", u"personoplysninger", u"personuppgifter", u"personvern", u"persónuvernd", u"piškotki", u"podmienky", u"podmínky", u"pogoji", u"politica de utilizare", u"politika e privatësisë", u"politika e të dhënave", u"política de dados", u"política de datos", u"používání dat", u"pravidlá", u"pravila", u"pravno", u"privaatsus", u"privacidad", u"privacidade", u"privacitat", u"privacy", u"privasi", u"privatezza", u"privatliv", u"privatnost", u"privatsphäre", u"privatum", u"privātum", u"protecció de dades", u"protecția datelor", u"prywatnoś", u"przetwarzanie danych", u"príobháideach", u"quy chế", u"quy định", u"regler om fortrolighed", u"regulamin", u"rekisteriseloste", u"retningslinjer for data", u"rgpd", u"rgpd", u"riservatezza", u"rpgd", u"rules", u"sekretess", u"slapuk", u"sopimusehdot", u"soukromí", u"sutikimas", u"syarat", u"személyes adatok védelme", u"súkromi", u"sīkdat", u"teisinė", u"temeni", u"termene", u"termeni", u"termini", u"termos", u"terms", u"tiesību", u"tietokäytäntö", u"tietosuoja", u"tingimused", u"téarmaí", u"upotrebi podataka", u"utilisation des donnees", u"utilisation des données", u"uvjeti", u"varstvo podatkov", u"veri ilkesi", u"veri politikası", u"vie privee", u"vie privée", u"vilkår", u"villkor", u"voorwaarden", u"využívania údajov", u"warunki", u"yasal", u"yksityisyy", u"zasady dotyczące danych", u"zasady przetwarzania danych", u"zasebnost", u"zaštita podataka", u"zásady ochrany osobných", u"çerez", u"điều khoản", u"şartları", u"απορρήτου", u"απόρρητο", u"εμπιστευτικότητας", u"ιδιωτικότητας", u"πολιτική δεδομένων", u"προσωπικά δεδομένα", u"προσωπικών δεδομένων", u"όροι", u"бисквитки", u"конфиде", u"конфиденциальность", u"конфіденційність", u"лични данни", u"персональных данных", u"поверителност", u"политика за данни", u"политика использования", u"политика лд", u"политика о подацима", u"пользовательское соглашение", u"правила", u"приватност", u"споразумение", u"условия", u"הסכם שימוש", u"מדיניות נתונים", u"פרטיות", u"תנאי שימוש", u"תקנון", u"الخصوصية", u"حریم خصوصی", u"سياسة البيانات", u"شرایط و قوانین", u"قوانین و مقررات", u"ข้อกำหนดการใช้งาน", u"ข้อกำหนดของการบริการ" u"ข้อตกลงและเงื่อนไข", u"ความเป็นส่วนตัว", u"นโยบายความเป็นส่วนตัว", u"นโยบายคุกกี้", u"ประกาศนโยบายความเป็นส่วนตัว", u"เงื่อนไขและข้อกำหนด", u"ご利用上の注意", u"クッキー", u"プライバシー", u"個人情報", u"数据使用", u"數據使用", u"私隱", u"規約", u"隐私权", u"개인정보", u"이용약관 ", u"프라이버시"]
    
keywords_primary = keywords_1 + keywords_2


visited_links = set()
all_links = []


def extract_home_page_links(url):
    headers = {
        'User-Agent': get_random_user_agent()
    }
    
    try:
        response = requests.get(url, timeout=5, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        links = set()  # Use a set to avoid duplicates
        
        # Extract href links from <a> tags
        for tag in soup.find_all("a", href=True):
            link = tag.get("href")
            if link and link.startswith("http"):
                links.add(link)
        
        # Extract src links from <img> tags
        for tag in soup.find_all("img", src=True):
            link = tag.get("src")
            if link and link.startswith("http"):
                links.add(link)
        
        # Extract src links from <script> tags
        for tag in soup.find_all("script", src=True):
            link = tag.get("src")
            if link and link.startswith("http"):
                links.add(link)
        
        # Extract src links from <iframe> tags
        for tag in soup.find_all("iframe", src=True):
            link = tag.get("src")
            if link and link.startswith("http"):
                links.add(link)
        
        # Extract form action links from <form> tags
        for tag in soup.find_all("form", action=True):
            link = tag.get("action")
            if link and link.startswith("http"):
                links.add(link)
        
        return list(links)
    
    except requests.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return []


def get_random_user_agent():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'
        # Add more user agents if needed
    ]
    return random.choice(user_agents)

def extract_links(url):
    headers = {
        'User-Agent': get_random_user_agent()
    }
    
    try:
        response = requests.get(url, timeout=5, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        links = []
        
        for link in soup.find_all("a", href=True):
            full_link = link.get("href")
            if full_link and full_link.startswith("http"):
                links.append(full_link)
        return links
    
    except requests.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return []

def recursive_extract(url, original_domain):
    if url not in visited_links:
        visited_links.add(url)
        if original_domain in url:
            print(f"Extracting links from: {url}")
            links = extract_links(url)
            for link in links:
                if link not in visited_links:
                    parsed_link = urlparse(link)
                    if parsed_link.netloc.endswith(original_domain):
                        all_links.append(link)
                        recursive_extract(link, original_domain)
                        time.sleep(1)  # Sleep to avoid overwhelming the server

def process_domains(domains):
    for domain in domains:
        global visited_links, all_links
        visited_links = set()
        all_links = []
        home_links = []
        try:
            print(f"Processing domain: {domain}")
            recursive_extract(f"https://{domain}", domain)
            home_links = extract_home_page_links(f"https://{domain}")
        except Exception as e:
            print(f"Error processing domain https://{domain}: {e}")
            # try with http
            try:
                recursive_extract(f"http://{domain}", domain)
                home_links = extract_home_page_links(f"https://{domain}")
            except Exception as e:
                print(f"Error processing domain http://{domain} with http: {e}")

        # start_url = f"https://{domain}"
        # recursive_extract(start_url, domain)
        
        # Store the results in TinyDB
        db = TinyDB('websites_by_language.json')
        # create a table named policy_links if it doesn't exist
        table = db.table('policy_links')
        # table = db.table('websites')
        timeprocessed = datetime.now().isoformat()
        home_links = list(set(home_links))
        all_links = list(set(all_links))
        data = {
            'domain': domain,
            'home_links': home_links,
            'all_links': all_links,
            'timeprocessed': timeprocessed,
            'country': 'Korea'
        }
        table.insert(data)

if __name__ == "__main__":
    # domains = ["naver.com", "anotherdomain.com"]  # Add the list of domains here
    # process_domains(domains)
    
    print("All extracted links for each domain:")
    db = TinyDB('websites_by_language.json')
    websites_table = db.table('websites')
    Website = Query()
    
    korean_websites = websites_table.search(Website.language == "ko")
    print(f"Total websites in Korean: {len(korean_websites)}")

    korean_websites = korean_websites[:100]
    # keep only urls
    korean_websites = [website['url'] for website in korean_websites]
    print(korean_websites)


    # check if any of the url already exist in policy_links table of websites_by_language.json and remove them
    policy_links_table = db.table('policy_links')
    PolicyLink = Query()
    existing_policy_links = policy_links_table.all()
    existing_policy_links = [policy_link['domain'] for policy_link in existing_policy_links]
    print(existing_policy_links)
    korean_websites = [website for website in korean_websites if website not in existing_policy_links]
    print(korean_websites)
    process_domains(korean_websites)




    # # Example usage
    # urls = [
    #     'https://example.com',  # Replace with your URLs
    #     'https://example.jp',
    #     'https://example.cn',
    #     'https://example.kr'
    # ]

    # for url in urls:
    #     print(f"Checking URL: {url}")
    #     policy_links = extract_policy_links(url)
    #     print(f"Potential policy links for {url}:")
    #     for text, href in policy_links:
    #         print(f"  Text: {text}, URL: {href}")
    #     print("\n")
    # ####