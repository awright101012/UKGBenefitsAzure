import logging
import configparser
import requests
import pandas as pd
from io import StringIO
import time
from sqlalchemy import create_engine

def main():
    # Step 1: Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Step 2: Get the login credentials from the configuration file
    api_key = config.get('Credentials', 'api_key')
    username = config.get('Credentials', 'username')
    password = config.get('Credentials', 'password')
    company_shortname = config.get('Credentials', 'company')

    # Step 3: Login
    login_url = 'https://secure.saashr.com/ta/rest/v1/login'

    headers = {
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    login_data = {
        'credentials': {
            'username': username,
            'password': password,
            'company': company_shortname
        }
    }

    response = requests.post(login_url, headers=headers, json=login_data)
    response_data = response.json()

    # Check for login success
    if 'token' in response_data:
        # Step 4: Extract the token
        token = response_data['token']

        # Step 5: Set the Authentication header for subsequent requests
        auth_header = {'Authentication': f'Bearer {token}'}

        # Create a function that will handle the token timeout
        def make_request(url, headers, params=None, method='get'):
            response = requests.request(method, url, headers=headers, params=params)

            # If token has expired
            if response.status_code == 401:
                # Re-login
                login_response = requests.post(login_url, headers=headers, json=login_data)
                login_response_data = login_response.json()

                if 'token' in login_response_data:
                    # Update the token
                    token = login_response_data['token']
                    headers = {'Authentication': f'Bearer {token}'}
                    # Retry the failed request
                    response = requests.request(method, url, headers=headers, params=params)
            return response

        # Step 6: Define the company short names and settings IDs
        company_settings = {
                'Agmark': '42008082',
            'CHPI': '42006262',
            '1NMAIN': '42006217',
            'real-mesa': '42006247',
            'ambuilders': '42006269',
            'Atria': '42008091',
            'BONDML': '42006257',
            'CAYMAS': '42008067',
            'brightideas': '42006249',
            'CPCI': '42008095',
            'CCFM': '42006256',
            'Cybermaxx': '42006254',
            'DSF': '42008097',
            'HighTide': '42008081',
            'HCAL': '42006264',
            'BOWL': '42006252',
            'LBMCEP': '42006260',
            #'LBMCFS': '41990735',
            'LBMCIA': '42008086',
           # 'LBMCMS': '41990738',
            'LBMCPBS': '42006266',
            'LBMCPS': '42008094',
            'LBMCSS': '42006258',
            'LBMCTS': '42008073',
            'LBMCWS': '42006253',
            'LBMCPC': '42008070',
            'MMCAnesthesia': '42008068',
            'MVI': '42008084',
            'nashvilletrail': '42006255',
            'NHPartners': '42006246',
            'Parthenon': '42006265',
            'sanusom': '42006271',
            'SMPChatt': '42006250',
            'SurgNet': '42008090',
            'dermcenter': '42006267',
            'Boost': '42008079',
            'Frist': '42008069',
            'VeruStat': '42008076',
            'infocus': '42008065',
            'WorldTravel': '42006263',
        }

        # Step 7: Prepare an empty DataFrame for the merged report data
        merged_data = pd.DataFrame()

        # Token expiration time
        token_expiration_time = time.time() + 3600  # Set token expiration to 1 hour from now
        token_expiration_margin = 300  # 5 minutes before token expiration

        # Step 8: Loop through the company short names and settings IDs
        for target_company_shortname, settings_id in company_settings.items():
            # Step 9: Pull the report for the current company and settings ID
            report_url = f'https://secure.saashr.com/ta/rest/v1/report/saved/{settings_id}'

            query_params = {
                'company:shortname': target_company_shortname
            }

            headers = {
                'Accept': 'text/csv',  # Request report data in CSV format
                'Authentication': f'Bearer {token}'
            }

            response = make_request(report_url, headers=headers, params=query_params)

            # Check if the response is successful
            if response.ok:
                report_data = response.text

                # Create a DataFrame from the report data
                report_df = pd.read_csv(StringIO(report_data), low_memory=False)


                # Add a "Company" column with the current company short name
                report_df['Company'] = target_company_shortname

                # Append the current report data to the merged data DataFrame
                merged_data = pd.concat([merged_data, report_df], ignore_index=True)
            else:
                print(f'Error: Failed to retrieve the report for {target_company_shortname}. Status code: {response.status_code}')
    else:
        print('Error: Login failed. Please check your credentials.')

    # Step 10: Define the connection parameters
    server = config.get('Database', 'server')
    database = config.get('Database', 'database')
    db_username = config.get('Database', 'username')
    db_password = config.get('Database', 'password')
    driver = 'ODBC+Driver+17+for+SQL+Server'


    # Create a connection string using SQLAlchemy URL format
    connection_string = f'mssql+pyodbc://{db_username}:{db_password}@{server}/{database}?driver={driver}'

    # Create an engine using SQLAlchemy
    engine = create_engine(connection_string)

    # Step 11: Write the DataFrame to the SQL Server database
    merged_data.to_sql('benefitsbilling', engine, if_exists='replace', index=False)

    logging.info('Data written to SQL Server database')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
