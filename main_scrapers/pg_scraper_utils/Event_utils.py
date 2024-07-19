import asyncio
import httpx
from bs4 import BeautifulSoup
import pandas as pd
import nest_asyncio
from io import StringIO


# Function to parse showcase labels from HTML on site
async def get_showcase_info(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        title_element = soup.find("a", {"id": "ContentTopLevel_ContentPlaceHolder1_EventHeader1_lblEventNameNew"})
        date_element = soup.find("span", {"id": "ContentTopLevel_ContentPlaceHolder1_EventHeader1_lblDatesNew"})

        title = title_element.text if title_element else "N/A"
        date = date_element.text if date_element else "N/A"

        return title, date
    except Exception as e:
        print(f"Error in get_showcase_info: {e}")
        return "N/A", "N/A"

# Function to process Single Event ID
async def process_event(event_id, retries, semaphore):
    url = f'https://www.perfectgame.org/events/Showcases/WorkoutResults.aspx?event={event_id}'
    attempt = 0
    while attempt < retries:
        async with semaphore, httpx.AsyncClient() as client:
            try:
                response = await client.get(url)
                html = response.text
                tables = pd.read_html(StringIO(html))
                title, date = await get_showcase_info(html) #utilizes specified HTML parser to find proper labels

                if len(tables) > 0:
                    table = (tables[4]
                            .assign(ShowcaseTitle=title, ShowcaseDate=date)
                            .pipe(lambda df: df.replace(to_replace='&nbsp', value=' ', regex=True))) #categorizes and cleans a given event
                    print(f"Event {event_id} processed")
                    return table
                
            except IndexError as e: 
                print(f"IndexError for Event {event_id}: {e}") #does not retry if Index Error - these are caused by data not existing
                return None
            
            except Exception as e:
                attempt += 1
                print(f"Attempt {attempt} failed for Event {event_id}: {e}")
                if attempt == retries:
                        print(f"All {retries} retries have failed for Event {event_id} ")
                        return None

#Function to concurrently scrape the data and concat into a table
async def scrape_data(event_ids, retries, threads):
    all_tables = pd.DataFrame()
    semaphore = asyncio.Semaphore(threads)  # controls amount of concurrency

    tasks = [process_event(event_id, retries, semaphore) for event_id in event_ids]
    tables = await asyncio.gather(*tasks) #utilizes created tasks to asynchronously process events

    all_tables = pd.concat([table for table in tables if table is not None], ignore_index=True) 

    return all_tables

