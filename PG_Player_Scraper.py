import asyncio
import httpx
from bs4 import BeautifulSoup
import pandas as pd
import nest_asyncio
from asyncio import Semaphore
from pg_scraper_utils import get_player_info, process_player


async def scrape_all_players(player_id_start: int, player_id_end: int, csv_filename, threads: int = 20):
    '''
    Function accesses player data available on Perfect Game through a specified range of Event IDs. Async and Semaphore additions used to hasten process
	while waiting on site requests. Semaphores should be used in moderation as to not overwhelm with site requests.
    
    param - player_id_start: the first PG Player ID in the range of players being scraped
    param - player_id_end:  the last PG Player ID in the range of players being scraped
    param - threads: amount of semaphores available for use
    param - csv_filename: specified name of csv file where player data is being saved
    
    '''
    sem = Semaphore(threads)  # limits number of task, defaults to 20
    player_ids = range(player_id_start, player_id_end)
    tasks = []

    async def bound_process_player(sem, player_id): #limits simultaneous downloads
        async with sem: 
            return await process_player(player_id)

    for player_id in player_ids:
        task = bound_process_player(sem, player_id)
        tasks.append(task)
        print(f"Player {player_id} has been added to the queue")

    all_player_info = await asyncio.gather(*tasks)
    all_player_info = [player for player in all_player_info if player is not None]
    all_tables = pd.DataFrame(all_player_info).assign(
        Height=lambda df: df['Height'].replace('-', '~', regex=True),
        Weight=lambda df: df['Weight'].replace('¬†¬†', '', regex=True),
        Hometown=lambda df: df['Hometown'].replace('¬†¬†', '', regex=True)
    )
    all_tables.to_csv(csv_filename, index=False)


'''Example Call'''

#if __name__ == '__main__':
#    asyncio.run(scrape_all_players(player_id_start=500000, player_id_end=600001, csv_filename='/content/drive/MyDrive/UK Scouting Datasets/pg_all_player_info.csv'))