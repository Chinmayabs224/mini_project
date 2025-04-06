import time
import pandas as pd
from urllib.parse import quote
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def get_user_keywords():
    """Prompt user for search keywords and return them."""
    while True:
        try:
            keywords = input("Enter your search keywords: ").strip()
            if not keywords:
                raise ValueError("Keywords cannot be empty.")
            return keywords
        except ValueError as e:
            print(f"Error: {e}. Please try again.")

def construct_search_url(keywords, page=0):
    """Construct a Google search URL with keywords and 'past 24 hours' filter."""
    query = quote(keywords)
    base_url = f"https://www.google.com/search?q={query}"
    time_filter = "&tbs=qdr:d"
    start = page * 10
    return f"{base_url}{time_filter}&start={start}"

def setup_driver():
    """Set up a headless Chrome driver with anti-detection."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def fetch_page(url, driver):
    """Fetch the rendered HTML content using Selenium."""
    try:
        print(f"Requesting URL: {url}")
        driver.get(url)
        time.sleep(5)  # Wait for page to load
        html = driver.page_source
        desktop_path = "D:/Machine Learning/debug_page.html"
        with open(desktop_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Page fetched successfully. Raw HTML saved to {desktop_path}")
        return html
    except Exception as e:
        print(f"Error fetching page: {e}")
        return None

def parse_search_results(html):
    """Parse HTML content to extract titles and URLs."""
    if not html:
        print("No HTML content to parse.")
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    
    result_divs = soup.select('div:has(h3) a[href^="http"]')
    print(f"Found {len(result_divs)} potential result divs.")
    
    if not result_divs:
        print("No result divs found. Possible CAPTCHA or block. Check debug_page.html.")
    
    for result in result_divs:
        try:
            title_elem = result.find_parent().select_one('h3')
            link_elem = result
            if title_elem and link_elem and 'href' in link_elem.attrs:
                title = title_elem.get_text()
                url = link_elem['href']
                if url.startswith('http') and 'google' not in url.lower():
                    results.append({'title': title, 'url': url})
        except Exception as e:
            print(f"Error parsing result: {e}")
            continue
    
    print(f"Parsed {len(results)} valid results from page.")
    return results

def scrape_google_search(keywords, max_pages=3):
    """Scrape Google search results using Selenium."""
    driver = setup_driver()
    all_results = []
    
    try:
        for page in range(max_pages):
            print(f"\nFetching page {page + 1}...")
            url = construct_search_url(keywords, page)
            html = fetch_page(url, driver)
            if html:
                page_results = parse_search_results(html)
                if not page_results and page == 0:
                    print("No results found on the first page. Check debug_page.html.")
                all_results.extend(page_results)
            else:
                print("Stopping due to fetch error.")
                break
            time.sleep(2)
    finally:
        driver.quit()
    
    return all_results

def save_results(results):
    """Save results to a DataFrame and CSV on the Desktop."""
    if not results:
        print("No results to save.")
        return None
    
    df = pd.DataFrame(results)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"D:/Machine Learning/search_results_{timestamp}.csv"
    df.to_csv(filename, index=False)
    print(f"Results saved to {filename}")
    return df

def main():
    """Main execution flow."""
    print("Google Search Scraper - Last 24 Hours")
    keywords = get_user_keywords()
    
    try:
        results = scrape_google_search(keywords, max_pages=3)
        
        if results:
            print(f"\nFound {len(results)} results:")
            for i, result in enumerate(results, 1):
                print(f"{i}. {result['title']}\n   {result['url']}")
            save_results(results)
        else:
            print("No results were found or an error occurred during scraping.")
    
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()