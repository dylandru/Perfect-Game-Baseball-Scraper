import asyncio
import httpx
from bs4 import BeautifulSoup
import pandas as pd
import nest_asyncio
from asyncio import Semaphore
from pg_scraper_utils.Event_utils import process_event, get_showcase_info, scrape_data



async def run_event_scraper(start_id: int = 1, end_id: int = 90000, threads: int = 50, retries: int = 5, csv_filename: str = 'Perfect_Game_Workout_Sheet.csv') -> None:
    '''
    Function accesses showcase data available on Perfect Game through a specified range of Event IDs. Async and Semaphore additions used to hasten process
    while waiting on site requests. Semaphores should be used in moderation as to not overwhelm with site requests.
    
    Args:
        start_id: the first PG Event ID in the range of events being scraped
        end_id:  the last PG Event ID in the range of events being scraped
        threads: amount of semaphores available for use
        csv_filename: specified name of csv file where event data is being saved

    Returns:
        None: A saved CSV containing info on all events specified. 

    Raises:
        KeyboardInterrupt: Program quits when User hits CTRL+C.
        Exception: Program quits when error occurs.
    
    '''
    try:
        event_ids = range(start_id, end_id) 
        all_tables = await scrape_data(event_ids, retries, threads)

    except KeyboardInterrupt:
        print(f'Keyboard Interrupt caught - shutting down.')

    except Exception as e:
        print(f"An error occurred during scraping: {str(e)}")

    all_tables.to_csv(csv_filename, index=False)
    print(f"All Perfect Game Workout data for Event ID {start_id} through Event ID {end_id} has been saved to '{csv_filename}'")

''' Example Call'''

if __name__ == '__main__':
    asyncio.run(run_event_scraper(start_id=1, end_id=50000, threads=20, csv_filename='/content/drive/MyDrive/UK Scouting Datasets/pg_all_player_info.csv'))
