from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv
import time
from pg_scraper_utils.Ranking_utils import scrape_rankings_page, navigate_to_next_page, ranking_fieldnames

def scrape_rankings_table(year: int, max_pages: int = 20) -> None:
    """
    Scrapes PG rankings for a given year and saves to a CSV file named based on amount of players.

    Args:
        year (int): The year for which to scrape rankings.
        max_pages (int, optional): Number of pages to scrape (100 players per page). Defaults to 20.

    Returns:
        None: No value returned - ranking data written to CSV file.

    Raises:
        KeyboardInterrupt: Program quits when User hits CTRL+C.
        Exception: Program quits when error occurs.
    """
    url = f'https://www.perfectgame.org/Rankings/Players/NationalRankings.aspx?gyear={year}'
    csv_filename = f'{year}_PG_Top_{max_pages * 100}_Player_Rankings.csv'
    
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(2) 

    total_players = 0
    
    try:
        for page in range(1, max_pages + 1):
            print(f"Scraping page {page}")
            players_data = scrape_rankings_page(driver)
            
            if not players_data:
                print(f"No data found on page {page}. Stopping.")
                break

            players_data = [player for player in players_data if player['PlayerName'] != 'N/A'] 
            
            total_players += len(players_data)
            
            with open(csv_filename, 'a' if page > 1 else 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=ranking_fieldnames)
                if page == 1:
                    writer.writeheader()
                writer.writerows(players_data) #adds data to CSV for each page as it scrapes
    
            print(f"Scraped Page {page} - Total Players Scraped: {total_players}")
            
            if not navigate_to_next_page(driver, page):
                print("Unable to proceed to next page. Stopping scraper.")
                break

    except KeyboardInterrupt:
        print(f'Keyboard Interrupt caught - shutting down.')

    except Exception as e:
        print(f"An error occurred during scraping: {str(e)}")

    finally:
        driver.quit()

    print(f"Scraping complete. Total players scraped: {total_players}")

#Example Call

if __name__ == '__main__':
    scrape_rankings_table(year=2023, max_pages=25)