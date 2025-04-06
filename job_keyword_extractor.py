import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse
import re
from typing import Dict, List, Optional
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JobKeywordExtractor:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.keywords = {
            'experience': ['experience', 'years of experience', 'work experience'],
            'skills': ['skills', 'required skills', 'qualifications', 'requirements'],
            'education': ['education', 'degree', 'qualification'],
            'location': ['location', 'place', 'work location'],
            'salary': ['salary', 'compensation', 'pay', 'package'],
            'job_type': ['job type', 'employment type', 'full-time', 'part-time', 'contract']
        }

    def is_valid_url(self, url: str) -> bool:
        """Check if the URL is valid and accessible."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def fetch_page_content(self, url: str) -> Optional[str]:
        """Fetch the content of a webpage."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None

    def extract_keywords(self, text: str, keyword_type: str) -> List[str]:
        """Extract relevant information based on keyword type."""
        text = text.lower()
        found_keywords = []
        
        for keyword in self.keywords[keyword_type]:
            # Create a pattern that looks for the keyword and captures text after it
            pattern = f"{keyword}[:\s]+([^.!?]+)"
            matches = re.finditer(pattern, text)
            
            for match in matches:
                found_keywords.append(match.group(1).strip())
        
        return found_keywords

    def process_url(self, url: str) -> Dict:
        """Process a single URL and extract relevant information."""
        if not self.is_valid_url(url):
            logger.warning(f"Invalid URL: {url}")
            return {'url': url, 'error': 'Invalid URL'}

        content = self.fetch_page_content(url)
        if not content:
            return {'url': url, 'error': 'Failed to fetch content'}

        soup = BeautifulSoup(content, 'html.parser')
        text_content = soup.get_text()

        result = {'url': url}
        for keyword_type in self.keywords.keys():
            extracted = self.extract_keywords(text_content, keyword_type)
            result[keyword_type] = extracted if extracted else []

        return result

    def process_csv(self, csv_path: str, url_column: str) -> pd.DataFrame:
        """Process URLs from a CSV file and extract keywords."""
        try:
            df = pd.read_csv(csv_path)
            if url_column not in df.columns:
                raise ValueError(f"Column '{url_column}' not found in CSV file")

            results = []
            total_urls = len(df)
            
            for idx, row in df.iterrows():
                url = row[url_column]
                logger.info(f"Processing URL {idx + 1}/{total_urls}: {url}")
                
                result = self.process_url(url)
                results.append(result)
                
                # Add a small delay to avoid overwhelming servers
                time.sleep(2)

            return pd.DataFrame(results)

        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            raise

def main():
    """Main execution function."""
    try:
        # Get input from user
        csv_path = input("Enter the path to your CSV file: ").strip()
        url_column = input("Enter the name of the column containing URLs: ").strip()

        # Initialize extractor and process CSV
        extractor = JobKeywordExtractor()
        results_df = extractor.process_csv(csv_path, url_column)

        # Save results
        output_file = f"job_keywords_{time.strftime('%Y%m%d_%H%M%S')}.csv"
        results_df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")

        # Display summary
        print("\nExtraction Summary:")
        print(f"Total URLs processed: {len(results_df)}")
        print(f"Successful extractions: {len(results_df[~results_df['error'].notna()])}")
        print(f"Failed extractions: {len(results_df[results_df['error'].notna()])}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 