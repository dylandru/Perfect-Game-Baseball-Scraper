from bs4 import BeautifulSoup
import httpx
import asyncio
import pandas as pd
import nest_asyncio
import traceback
from asyncio import Semaphore


async def get_player_info(soup):
    elements = {
        'PlayerName': 'ContentTopLevel_ContentPlaceHolder1_lblPlayerName',
        'School': 'ContentTopLevel_ContentPlaceHolder1_hl4yearCommit',
        'BestPGGrade': 'ContentTopLevel_ContentPlaceHolder1_lblBestPGGrade',
        'HSGrad': 'ContentTopLevel_ContentPlaceHolder1_lblHSGrad',
        'Position': 'ContentTopLevel_ContentPlaceHolder1_lblPos',
        'Hometown': 'ContentTopLevel_ContentPlaceHolder1_lblHomeTown',
        'TournamentTeam': 'ContentTopLevel_ContentPlaceHolder1_hlTournamentTeam',
        'HS': 'ContentTopLevel_ContentPlaceHolder1_lblHS',
        'BT': 'ContentTopLevel_ContentPlaceHolder1_lblBT',
        'Age': 'ContentTopLevel_ContentPlaceHolder1_lblAge',
        'Height': 'ContentTopLevel_ContentPlaceHolder1_lblHt',
        'Weight': 'ContentTopLevel_ContentPlaceHolder1_lblWt',
        'Fastball': 'ContentTopLevel_ContentPlaceHolder1_lblPGEventResultsFB',
        '60YardDash': 'ContentTopLevel_ContentPlaceHolder1_lblPGEventResults60',
        '10YardSplit': 'ContentTopLevel_ContentPlaceHolder1_lblPGEventResults10',
        'OFVelocity': 'ContentTopLevel_ContentPlaceHolder1_lblPGEventResultsOF',
        'IFVelocity': 'ContentTopLevel_ContentPlaceHolder1_lblPGEventResultsIF',
        '1BVelocity': 'ContentTopLevel_ContentPlaceHolder1_lblPGEventResults1B',
        'ExitVelocity': 'ContentTopLevel_ContentPlaceHolder1_lblPGEventResultsExitVelo'
    }
    
    #Attempts to map labels to values found in site
    try: 
        player_info = {field: soup.find(id=element_id).text if soup.find(id=element_id) else "N/A" for field, element_id in elements.items()}
        return player_info

    except Exception as e:
        print(f"An error occurred: {e}")
        return {field: "N/A" for field in elements.keys()}


async def get_stats_table_info(soup):
    table = soup.find("table", {"class": "table table-condensed"})
    if table:
        stats = {}
        rows = table.find_all('tr')[1:]
        
        #Looks for Top Results, Class Averages, and Percentiles for each category above
        for row in rows: 
            cols = row.find_all('td')
            category = cols[0].text.strip() if cols else 'N/A'
            stats[category] = {
                'TopResult': cols[1].text.strip() if len(cols) > 1 else 'N/A',
                'ClassAvg': cols[2].text.strip() if len(cols) > 2 else 'N/A',
                'Percentile': cols[3].text.strip() if len(cols) > 3 else 'N/A',
            }
        return stats
    else:
        return {'TableNotFound': 'True'}

async def process_player(player_id):
    url = f'https://www.perfectgame.org/Players/PlayerProfile.aspx?ID={player_id}'
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            player_info = await get_player_info(soup)
            player_info['PlayerID'] = player_id
            stats_info = await get_stats_table_info(soup)

            player_info.update(stats_info) #puts player_info and stats_info into dictionary
            
            print(f"Successfully scraped data for Player ID {player_id}")
            return player_info

    except Exception as e:
        print(f"An exception occurred while processing player ID {player_id}: {e}")
        return None

