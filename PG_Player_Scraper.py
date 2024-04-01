import asyncio
import httpx
from bs4 import BeautifulSoup
import pandas as pd
import nest_asyncio
from asyncio import Semaphore
from pg_scraper_utils import get_player_info, process_player, find_id_from_name, find_ids_from_filters, get_stats_table_info


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

async def scrape_player_by_name(player_name: str):
    matching_players = await find_id_from_name(player_name)
    if len(matching_players) == 1:
        player_id = matching_players[0]['PlayerID']
        print(f"Found one match: {matching_players[0]['PlayerName']} (ID: {player_id})")
        player_data = await process_player(player_id)
        print(player_data)  
        return player_data
    elif len(matching_players) > 1:
        print("Found multiple matches:")
        for i, player in enumerate(matching_players, start=1):
            print(f"{i}. {player['PlayerName']} (ID: {player['PlayerID']})")
        selected_index = int(input("Select player by typing corresponding number: ")) - 1
        if 0 <= selected_index < len(matching_players):
            player_id = matching_players[selected_index]['PlayerID']
            player_data = process_player(player_id)
            pd.set_option('display.max_rows', 38)
            print(player_data)
        else:
            print("Invalid Number!")
    else:
        print("Name not found")
        
async def scrape_players_by_filter(age: int = None, position: str = None, graduation_year: int = None, csv_filename: str = "filtered_pg_players.csv"):
    '''
    Function accesses player data for player on Perfect Game meeting a specified criteria. 
    Multiple filters can be used - consult parameters below to ensure Player IDs are filtered properly.
    
    param - age: integer with age of player rounded to nearest year (ex. 18)
    param - position: string with the abbreviation of position (ex. C = Catcher, 1B = 1st Base, etc) 
                      that matches with any postion combo
    param - graduation_year: integer with full year of graduation (ex. 2024)
    param - csv_filename: specified name of csv file where player data is being saved
    '''

    
    filtered_player_ids = await find_ids_from_filters(age=age, position=position, graduation_year=graduation_year)
    
    if not filtered_player_ids:
        print("No players found with the given filters.")
        return

    print(f"Found {len(filtered_player_ids)} players. Processing...")
    
    async with httpx.AsyncClient() as client:
        # Creates task for given players IDs
        tasks = [process_player(player_id) for player_id in filtered_player_ids]
        
        # Gather player data for given IDs asynchronously
        players_data = await asyncio.gather(*tasks)

    players_data = [player for player in players_data if player is not None]
    pd.DataFrame(players_data).to_csv(csv_filename, index=False)

    print(f"Filtered data for {len(players_data)} PG players with filters of Age: {age}, Position: {position}, and Grad Year: {graduation_year} has been written to {csv_filename}")


'''Example Call to Scrape All Players'''

#if __name__ == '__main__':
#    asyncio.run(scrape_all_players(player_id_start=0, player_id_end=1000001, csv_filename='/content/drive/MyDrive/UK Scouting Datasets/pg_all_player_info.csv'))

'''Example Call to Scrape Player by Name'''

#if __name__ == '__main__':
#    asyncio.run(scrape_player_by_name(player_name="Cade Arrambide"))

'''Example Call to Scrape Players that meet Certain Criteria'''

#if __name__ == '__main__':
#    asyncio.run(scrape_players_by_filter(age=18, position="C", graduation_year=2024))
