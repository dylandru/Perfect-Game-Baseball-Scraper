import asyncio
import httpx
from bs4 import BeautifulSoup
import pandas as pd
import nest_asyncio
from asyncio import Semaphore
from pg_scraper_utils import process_event, get_showcase_info, scrape_data



async def run_event_scraper(start_id: int = 1, end_id: int = 90000, threads: int = 50, csv_filename: str = 'Perfect_Game_Workout_Sheet.csv'):
    '''
    Function accesses showcase data available on Perfect Game through a specified range of Event IDs. Async and Semaphore additions used to hasten process
	while waiting on site requests. Semaphores should be used in moderation as to not overwhelm with site requests.
    
    param - start_id: the first PG Event ID in the range of events being scraped
    param - end_id:  the last PG Event ID in the range of events being scraped
    param - threads: amount of semaphores available for use
    param - csv_filename: specified name of csv file where event data is being saved
    
    '''
    event_ids = range(start_id, end_id) 
    all_tables = await scrape_data(event_ids, threads)

    all_tables.to_csv(csv_filename, index=False)
    print(f"All Perfect Game Workout data for Event ID {start_id} through Event ID {end_id} has been saved to '{csv_filename}'")

''' Example Call'''

#if __name__ == '__main__':
#    asyncio.run(run_event_scraper(start_id=1, end_id=50000, threads=20, csv_filename='/content/drive/MyDrive/UK Scouting Datasets/pg_all_player_info.csv'))
