import requests
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3


def log_progess(message):
    log_file = 'code_log.txt'
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} - {message}\n"
    with open(log_file, 'a') as log_file:
        log_file.write(log_entry)
    print(log_entry)

def extract(log_progress):
    log_progress("Preliminaries complete. Initiating ETL process")
    url = requests.get('https://en.wikipedia.org/wiki/List_of_largest_banks')
    page = url.text
    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find_all('table')
    rows = table[2].find_all('tr')
    data = []
    for row in rows[1:]:
        col = row.find_all('td')
        if col:
            name = col[1].find('a')['title']
            mc_usd_billion = col[2].get_text().replace(',', '').strip() 
            data.append({
                'Name': name,
                'MC_USD_Billion': float(mc_usd_billion)
            })   
    log_progress(f"Data extraction complete. Initiating transformation process")
    df = pd.DataFrame(data)
    df.set_index('Name', inplace=True)
    print(df)
    return df


def transform(df, log_progress):
    csv_file = pd.read_csv('exchange_rate.csv')
    gbp_rate = csv_file.loc[csv_file['Currency'] == 'GBP', 'Rate'].values[0]
    eur_rate = csv_file.loc[csv_file['Currency'] == 'EUR', 'Rate'].values[0]
    inr_rate = csv_file.loc[csv_file['Currency'] == 'INR', 'Rate'].values[0]

    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * gbp_rate).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * eur_rate).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * inr_rate).round(2)

    log_progress("Data transformation complete. Initiating loading process")
    return df

def load_to_csv(transformed_df, output_csv, log_progress):
    transformed_df.to_csv(output_csv)
    log_progess("Data saved to CSV file")

    return


def load_to_db(transformed_df, db, log_progress, queries=None):
    sql_connection = sqlite3.connect('db')
    transformed_df.to_sql('Largest_banks', sql_connection, if_exists='replace', index = False)
    log_progress("Data loaded to the database table. Executing queries.")
    queries = [
        'SELECT * FROM largest_banks;',
        'SELECT AVG(MC_GBP_Billion) FROM Largest_banks;'
    ]
    for query in queries:
        try:
            result = pd.read_sql_query(query, sql_connection)
            print(result)  
            log_progress(f"Executed query: {query}")
        except Exception as e:
            log_progress(f"Error executing query: {query}, Error: {e}")

    log_progress("Progress Complete.")
    log_progress("Server connection Closed.")
    sql_connection.close() 


def main(log_progress):
    df = extract(log_progress)
    transformed_df = transform(df, log_progress)
    print(transformed_df)
    load_to_csv(transformed_df, './Largest_banks_data.csv', log_progess)
    load_to_db(transformed_df, 'Banks.db', log_progress)

if __name__ == "__main__":
    main(log_progess)