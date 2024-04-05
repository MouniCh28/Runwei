import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd

def scrape_opportunities():
    url = 'https://www.antler.co/platform/#apply-now'
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        opportunities = []
        unique_opportunities = set()
        
        opportunity_elements = soup.find_all('div', class_='button-group is-center')

        for opportunity_element in opportunity_elements:
            # Extract relevant information from each opportunity element
            link = opportunity_element.find('a')
            opportunity_url = link.get('href')
            description = opportunity_element.find('p').text.strip() if opportunity_element.find('p') else 'No description available'
            opportunity_key = (opportunity_url, description)  # Combine URL and description as a tuple
            
            # Check if the opportunity (URL and description combination) is already in the set
            if opportunity_key not in unique_opportunities:
                unique_opportunities.add(opportunity_key)  # Add the opportunity to the set
                opportunities.append({'URL': opportunity_url, 'description': description})

        print("Scraping completed successfully!")
        return opportunities
    else:
        print(f"Failed to scrape {url}. Status code: {response.status_code}")
        return None

def create_opportunities_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS opportunities (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT,
                        description TEXT
                     )''')
def insert_opportunity(cursor, url, description):
    # Check if the opportunity already exists in the database
    cursor.execute('SELECT COUNT(*) FROM opportunities WHERE url = ? AND description = ?', (url, description))
    count = cursor.fetchone()[0]
    
    if count == 0:
        # Insert the opportunity into the database
        cursor.execute('INSERT INTO opportunities (url, description) VALUES (?, ?)', (url, description))
        print(f"Opportunity inserted: {url}")
    else:
        print(f"Opportunity already exists: {url}")

opportunities = scrape_opportunities()

conn = sqlite3.connect('opportunities.db')
cursor = conn.cursor()

create_opportunities_table(cursor)

# Insert each unique opportunity into the database
for opportunity in opportunities:
    insert_opportunity(cursor, opportunity['URL'], opportunity['description'])

conn.commit()
conn.close()
conn = sqlite3.connect('opportunities.db')
df = pd.read_sql_query('SELECT * FROM opportunities', conn)
conn.close() 
print(df)