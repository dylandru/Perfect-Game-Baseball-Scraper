from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv
import pandas as pd
import time

ranking_fieldnames = ['NatPGRank', 'PlayerName', 'Position', 'BT', 'Height', 'Weight', 'Hometown', 'TournamentTeam', 'HS', 'Commit', 'Report']

def parse_player_row(player_row, report_row) -> dict:


    player_data = {}
    fields = {
        'NatPGRank': ('span', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_lblNatRank_'),
        'PlayerName': ('a', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_hlPlayerName_'),
        'BT': ('span', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_lblBatsThrows_'),
        'Height': ('span', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_lblHeight_'),
        'Weight': ('span', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_lblWeight_'),
        'Hometown': ('span', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_lblHometown_'),
        'TournamentTeam': ('span', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_lblTravelTeam_'),
        'HS': ('a', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_hl2yr_'),
        'Commit': ('a', 'ContentTopLevel_ContentPlaceHolder1_gvPlayers_hl4yr_'),
    }
    
    for field, (tag, id_start) in fields.items():
        element = player_row.find(tag, {'id': lambda x: x and x.startswith(id_start)})
        player_data[field] = element.text.strip() if element else 'N/A'
    
    pos_cell = player_row.find_all('td')[2] #specific position field not in HTML, needs to be manually specified
    player_data['Position'] = pos_cell.text.strip() if pos_cell else 'N/A'
    
    if report_row: #not all players have comments
        report_element = report_row.find('span', {'id': lambda x: x and x.startswith('ContentTopLevel_ContentPlaceHolder1_gvPlayers_InternalCommentLiteral_')}) 
        player_data['Report'] = report_element.text.strip() if report_element else 'N/A'
    else:
        player_data['Report'] = 'N/A'
    
    return player_data

def scrape_rankings_page(driver) -> list:

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    table = soup.find('table', {'id': 'ContentTopLevel_ContentPlaceHolder1_gvPlayers'})
    if not table:
        print("No rankings table found on the page")
        return []

    players_data = []
    rows = table.find_all('tr')[1:]
    
    for i in range(0, len(rows), 2):  # processes rows consistent with table structure
        player_row = rows[i]
        comment_row = rows[i + 1] if i + 1 < len(rows) else None
        
        player_data = parse_player_row(player_row, comment_row)
        players_data.append(player_data)

    return players_data

def navigate_to_next_page(driver, current_page) -> bool:

    try:
        paging_row = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//tr[@class='pagingnavy']"))
        )
        td_elements = paging_row.find_elements(By.TAG_NAME, "td")
        
        if td_elements[-1].text.strip() == 'Last >': #stops for last page
            last_page_link = td_elements[-1].find_element(By.TAG_NAME, "a")
            if not last_page_link.is_enabled():
                print("Reached the last page. Stopping.")
                return False

        next_page_number = current_page + 1
        for td in td_elements[1:-1]:  #skips current page and last td element
            if td.text.strip() == str(next_page_number):
                next_page_link = td.find_element(By.TAG_NAME, "a")
                driver.execute_script("arguments[0].click();", next_page_link)
                return True
        
        ellipsis_td = td_elements[-2] #ensures the second ellipsis is clicked to properly navigate forward
        if '...' in ellipsis_td.text: #goes to ellipsis if next page number not found on page
            ellipsis_link = ellipsis_td.find_element(By.TAG_NAME, "a")
            driver.execute_script("arguments[0].click();", ellipsis_link)
            return True
        
        print("Page or ellipsis not found - script may be complete.")
        return False

    except Exception as e:
        print(f"Error navigating to page {current_page + 1}: {str(e)}")
        return False

    finally:
        WebDriverWait(driver, 5).until(
            EC.staleness_of(driver.find_element(By.ID, "ContentTopLevel_ContentPlaceHolder1_gvPlayers"))
        )

