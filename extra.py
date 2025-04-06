import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import time
from fake_useragent import UserAgent
import random

# Constants
CSV_FILE = 'search_results_20250403_223812.csv'
OUTPUT_FILE = 'job_data_results.csv'
REQUEST_DELAY = 2  # seconds between requests to be polite
TIMEOUT = 10  # seconds for request timeout

# Initialize user agent generator
ua = UserAgent()

def get_headers():
    """Generate random headers for requests"""
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.google.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

def get_domain(url):
    """Extract domain from URL"""
    parsed_uri = urlparse(url)
    return '{uri.netloc}'.format(uri=parsed_uri)

def scrape_naukri(url):
    """Scrape data from Naukri.com job listings"""
    try:
        response = requests.get(url, headers=get_headers(), timeout=TIMEOUT)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract experience
        experience = soup.find('div', {'class': 'exp'})
        experience = experience.get_text(strip=True) if experience else "Not specified"
        
        # Extract salary
        salary = soup.find('div', {'class': 'salary'})
        salary = salary.get_text(strip=True) if salary else "Not disclosed"
        
        # Extract job details
        job_details = soup.find('div', {'class': 'job-desc'})
        job_details = job_details.get_text(strip=True) if job_details else "No details available"
        
        return {
            'experience': experience,
            'salary': salary,
            'details': job_details
        }
    except Exception as e:
        print(f"Error scraping Naukri: {e}")
        return None

def scrape_apple_jobs(url):
    """Scrape data from Apple job listings"""
    try:
        response = requests.get(url, headers=get_headers(), timeout=TIMEOUT)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Apple jobs typically have this structure
        experience = soup.find('span', {'class': 'job-experience'})
        experience = experience.get_text(strip=True) if experience else "Not specified"
        
        salary = soup.find('span', {'class': 'job-salary'})
        salary = salary.get_text(strip=True) if salary else "Not disclosed"
        
        job_details = soup.find('div', {'class': 'job-description'})
        job_details = job_details.get_text(strip=True) if job_details else "No details available"
        
        return {
            'experience': experience,
            'salary': salary,
            'details': job_details
        }
    except Exception as e:
        print(f"Error scraping Apple Jobs: {e}")
        return None

def scrape_generic_job(url):
    """Generic scraper for job sites we don't have a specific handler for"""
    try:
        response = requests.get(url, headers=get_headers(), timeout=TIMEOUT)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Try to find common elements
        experience = soup.find(string=['Experience', 'Years of Experience', 'Exp'])
        experience = experience.find_next().get_text(strip=True) if experience else "Not specified"
        
        salary = soup.find(string=['Salary', 'Compensation', 'Pay Range'])
        salary = salary.find_next().get_text(strip=True) if salary else "Not disclosed"
        
        # Try to get main content
        job_details = soup.find('div', {'class': ['description', 'job-details', 'content']})
        if not job_details:
            job_details = soup.find('main') or soup.find('article') or soup.find('div', {'role': 'main'})
        
        job_details = job_details.get_text(strip=True, separator='\n') if job_details else "No details available"
        
        return {
            'experience': experience,
            'salary': salary,
            'details': job_details[:1000] + "..." if len(job_details) > 1000 else job_details  # Limit details length
        }
    except Exception as e:
        print(f"Error scraping generic job: {e}")
        return None

def get_scraper_for_url(url):
    """Determine which scraper to use based on URL domain"""
    domain = get_domain(url).lower()
    
    if 'naukri.com' in domain:
        return scrape_naukri
    elif 'apple.com' in domain:
        return scrape_apple_jobs
    else:
        return scrape_generic_job

def process_csv(input_file, output_file):
    """Process the input CSV and write results to output CSV"""
    with open(input_file, mode='r', encoding='utf-8') as infile, \
         open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = ['title', 'url', 'experience', 'salary', 'details']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            url = row['url']
            print(f"Processing: {row['title']} - {url}")
            
            # Get appropriate scraper
            scraper = get_scraper_for_url(url)
            
            # Scrape the data
            scraped_data = scraper(url)
            
            if scraped_data:
                # Combine original data with scraped data
                result = {
                    'title': row['title'],
                    'url': url,
                    'experience': scraped_data['experience'],
                    'salary': scraped_data['salary'],
                    'details': scraped_data['details']
                }
                writer.writerow(result)
                print(f"Successfully scraped: {row['title']}")
            else:
                print(f"Failed to scrape: {row['title']}")
            
            # Be polite - delay between requests
            time.sleep(REQUEST_DELAY + random.uniform(0, 1))

if __name__ == "__main__":
    print("Starting job scraping process...")
    process_csv("D:\Machine Learning\search_results_20250403_223812.csv", "output.csv")
    print(f"Scraping complete. Results saved to {OUTPUT_FILE}")