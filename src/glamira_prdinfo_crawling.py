from config.dir.address import domains_file, products_file, output_dir
from crawler.glamira_scrapper import GlamiraScrapper

domains_file = domains_file
products_file = products_file

output_dir = output_dir
max_workers = 3

def glamira_prdinfo_crawling():
    scraper = GlamiraScrapper(domains_file, products_file, output_dir)
    completed, failed = scraper.run_scraping(max_workers)
    print(f"\nScraping Summary:")
    print(f"Completed: {completed}")
    print(f"Failed: {failed}")
    print(f"Total: {completed + failed}")

if __name__ == "__main__":
    glamira_prdinfo_crawling()