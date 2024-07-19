from bs4 import BeautifulSoup
import httpx
import asyncio
import pandas as pd
import nest_asyncio
import traceback
from asyncio import Semaphore
import re


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

async def process_player(player_id, retries):
    url = f'https://www.perfectgame.org/Players/PlayerProfile.aspx?ID={player_id}'
    attempt = 0
    while attempt < retries:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')

                player_info = await get_player_info(soup)
                player_info['PlayerID'] = player_id
                stats_info = await get_stats_table_info(soup)

                player_info.update(stats_info)
                print(f"Successfully scraped data for Player ID {player_id}")
                return pd.json_normalize(player_info).replace('\n', ' ', regex=True)
            
        except IndexError as e:
                print(f"IndexError for Event {player_id}: {e}") #does not retry if Index Error - these are caused by entries not existing
                return None

        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed for Player ID {player_id}: {e}")
            if attempt == retries:
                print(f"All {retries} retries failed for Player ID {player_id}")
                return None

async def find_id_from_name(name: str):
    
    player_id_info = pd.read_csv("Perfect-Game-Baseball-Scraper/resource_files/perfect_game_player_ids.csv", usecols=['PlayerID','PlayerName']) 
    matching_players = player_id_info[player_id_info['PlayerName'].str.contains(name, case=False, na=False)]
    
    return matching_players[['PlayerName', 'PlayerID']].to_dict('records')


async def find_ids_from_filters(age=None, position=None, graduation_year=None):

    # Load the player information from a CSV file
    player_id_info = pd.read_csv("Perfect-Game-Baseball-Scraper/resource_files/perfect_game_player_ids.csv").assign(
        Age=lambda x: x['Age'].astype(str),
        GraduationYear=lambda x: x['HSGrad'].astype(str)
    )
    
    # Apply filters based on provided arguments, using the modified DataFrame
    if age is not None:
        age_str = str(age)
        modified_df = player_id_info[player_id_info['Age'].str.contains(age_str, na=False)]
    
    if position is not None:
        modified_df = player_id_info[player_id_info['Position'].str.contains(position, case=False, na=False)]
    
    if graduation_year is not None:
        grad_year_str = str(graduation_year)
        modified_df = player_id_info[player_id_info['GraduationYear'].str.contains(grad_year_str, na=False)]
    
    # Return the filtered list of player names and IDs
    return modified_df[['PlayerName', 'PlayerID']].to_dict('records')

def clean_string(s):
    return re.sub(r'^0+\s*', '', str(s).strip())
    