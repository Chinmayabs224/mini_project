import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import time
from concurrent.futures import ThreadPoolExecutor
import logging
from fake_useragent import UserAgent
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_scraper.log'),
        logging.StreamHandler()
    ]
)

class JobScraper:
    def __init__(self):
        self.ua = UserAgent()
        self.session = requests.Session()
        self.session.headers.update({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
        # Domain-specific selectors (can be extended)
        self.site_selectors = {
            'indeed.com': {
                'title': 'h1.jobsearch-JobInfoHeader-title',
                'company': 'div[data-company-name="true"] a',
                'experience': 'div#jobDetailsSection div:contains("Experience") + div',
                'salary': 'div#salaryInfoAndJobType span',
                'description': 'div#jobDescriptionText',
            },
            'linkedin.com': {
                'title': 'h1.top-card-layout__title',
                'company': 'a.topcard__org-name-link',
                'experience': 'li.description__job-criteria-item:contains("Experience") span',
                'salary': 'li.description__job-criteria-item:contains("Salary") span',
                'description': 'div.show-more-less-html__markup',
            },
            'glassdoor.com': {
                'title': 'div.jobTitle h1',
                'company': 'div.employerName',
                'experience': 'div.jobDescriptionContent div:contains("Experience")',
                'salary': 'div.salaryEstimate',
                'description': 'div.jobDescriptionContent',
            }
        }
        
    def get_domain(self, url):
        """Extract domain from URL"""
        parsed = urlparse(url)
        return parsed.netloc.replace('www.', '')
    
    def get_selector(self, url, field):
        """Get appropriate selector based on domain"""
        domain = self.get_domain(url)
        for site in self.site_selectors:
            if site in domain:
                return self.site_selectors[site].get(field, '')
        return ''
    
    def scrape_job_page(self, url):
        """Scrape a single job listing page"""
        try:
            # Rotate user agent and add delay to avoid blocking
            headers = {'User-Agent': self.ua.random}
            time.sleep(1)  # Be polite with delay between requests
            
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract data using domain-specific selectors
            data = {
                'url': url,
                'title': self._extract_text(soup, self.get_selector(url, 'title')),
                'company': self._extract_text(soup, self.get_selector(url, 'company')),
                'experience': self._extract_text(soup, self.get_selector(url, 'experience')),
                'salary': self._extract_text(soup, self.get_selector(url, 'salary')),
                'description': self._extract_text(soup, self.get_selector(url, 'description')),
                'domain': self.get_domain(url),
            }
            
            return data
        
        except Exception as e:
            logging.error(f"Error scraping {url}: {str(e)}")
            return {
                'url': url,
                'error': str(e)
            }
    
    def _extract_text(self, soup, selector):
        """Helper method to extract text using CSS selector"""
        if not selector:
            return ''
        
        element = soup.select_one(selector)
        return element.get_text(strip=True) if element else ''
    
    def process_csv(self, input_file, output_file, max_workers=5):
        """Process CSV file with URLs and save results"""
        try:
            with open(input_file, mode='r', encoding='utf-8') as infile, \
                 open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
                
                reader = csv.DictReader(infile)
                fieldnames = ['url', 'title', 'company', 'experience', 'salary', 'description', 'domain', 'error']
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                urls = [row['url'] for row in reader if row.get('url')]
                
                # Use threading to speed up scraping
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    results = executor.map(self.scrape_job_page, urls)
                    
                    for result in results:
                        writer.writerow(result)
                        outfile.flush()  # Ensure data is written after each row
                        
            logging.info(f"Scraping completed. Results saved to {output_file}")
            
        except Exception as e:
            logging.error(f"Error processing files: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    scraper = JobScraper()
    
    # Configuration
    INPUT_CSV = "D:\Machine Learning\search_results_20250403_223812.csv"  # CSV with 'url' column
    OUTPUT_CSV = 'job_data.csv'
    
    # Run the scraper
    try:
        logging.info("Starting job scraping process...")
        scraper.process_csv(INPUT_CSV, OUTPUT_CSV)
        logging.info("Scraping process completed successfully.")
    except Exception as e:
        logging.error(f"Scraping process failed: {str(e)}")