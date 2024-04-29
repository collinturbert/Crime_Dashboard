import os
import time
import math
import json
import logging
import requests
import datetime
import concurrent.futures
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statistics import mean
from decouple import config
from sqlalchemy import create_engine, text

# Data is from https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi

### CONFIGURATION ###
LOG_FOLDER = 'log_files'
LOG_FILENAME = f'Logfile - Crimes Grabber ({datetime.date.today()}).log'

DB_CONFIG = {
    "host": config('PostgreSQL_HOST'),
    "user": config('PostgreSQL_USER'),
    "password": config('PostgreSQL_PASSWORD'),
    "database": config('PostgreSQL_DATABASE'),
}

API_KEY = config('FBI_API')


### CLASSES ###
class DatabaseConnection:
    def __enter__(self):
        self.engine = connect_db()
        return self.engine

    def __exit__(self, exc_type, exc_value, traceback):
        self.engine.dispose()

        
### FUNCTIONS ###
# Initialize the logging system
def setup_logging():
    os.makedirs(LOG_FOLDER, exist_ok=True)
    log_file = os.path.join(LOG_FOLDER, LOG_FILENAME)
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f'Log file created/accessed at {datetime.datetime.now()}')


# Connect to PostgreSQL database
def connect_db():
    try:
        database_url = f'postgresql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@localhost:{DB_CONFIG["host"]}/{DB_CONFIG["database"]}'
        engine = create_engine(database_url)
        logging.info(f'Connected to the database at {datetime.datetime.now()}')
        return engine
    
    except Exception as e:
        logging.error(f'Error connecting to the database: {e}')
        raise

# Get agency codes
def get_agency_code():
    # Initialize list to add agency codes
    agency_list = []

    # Get data from agency API
    agency_url = f'https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/CO?API_KEY={API_KEY}'
    response = requests.get(agency_url)
    json_data = json.loads(response.text)

    # Add agency code to agency_list
    agency_list = [agency_dict['ori'] for agency_dict in json_data]

    # Return agency information and agency code list
    return json_data, agency_list


# Get agency level crime data
def get_agency_crimes(agency_code):
    url = f'https://api.usa.gov/crime/fbi/cde/arrest/agency/{agency_code}/all?from=2000&to=2024&API_KEY={API_KEY}'
    response = requests.get(url)
    
    if response.status_code == 200:
        json_data = json.loads(response.text)
        crime_data = json_data['data']
        for crime_dict in crime_data:
            crime_dict['Agency'] = agency_code

        return crime_data
    
    else:
        print(f'Got response status code: {response.status_code} for url:{url}')


# Get state level data
def get_state_crimes():
    url = f'https://api.usa.gov/crime/fbi/cde/arrest/state/CO/all?from=2000&to=2024&API_KEY={API_KEY}'
    response = requests.get(url)
    
    if response.status_code == 200:
        json_data = json.loads(response.text)
        crime_data = json_data['data']
        return crime_data
    
    else:
        print(f'Got response status code: {response.status_code} for url: {url}')



# Plot the data on scatterplot
def plot_scatter(df, x_values, columns_to_plot):
    try:
        colors = plt.cm.rainbow(np.linspace(0, 1, len(columns_to_plot)))
        #
        for i, col in enumerate(columns_to_plot):
            plt.plot(df[x_values], df[col], marker='o', markersize=2, c=colors[i], label=col)

        # Add labels and title
        plt.xlabel('Year')
        plt.ylabel('Crime Count')
        plt.title('Plot of Crime Counts in Colorado by Type')

        # Add a legend to distinguish the colors
        legend = plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        # Calculate the required figure size based on the legend's size
        fig = plt.gcf()
        fig.canvas.draw()
        legend_bbox = legend.get_window_extent()
        legend_width = legend_bbox.width / fig.dpi
        legend_height = legend_bbox.height / fig.dpi
        new_width = fig.get_figwidth() + legend_width * 1.5  # Add some padding
        fig.set_size_inches(new_width, fig.get_figheight())
        
        # Show the plot
        plt.tight_layout()
        plt.show()
        logging.info('Succesfully plotted!')

    except Exception as e:
        logging.error(f'Error occured during plotting: {e}')


# Main function
def main():
    setup_logging()

    try:
        with DatabaseConnection() as engine:
            # Get state level list of crimes
            state_crimes = get_state_crimes()
            state_crimes_df = pd.DataFrame(state_crimes)
            state_crimes_df.to_sql('state_crimes', engine, if_exists='replace', index=False)
            logging.info('Got state crimes data')
            
            # Get list of agency codes
            agency_data, agency_codes = get_agency_code()
            agency_info_df = pd.DataFrame(agency_data)
            agency_info_df.to_sql('agency_info', engine, if_exists='replace', index=False)
            logging.info('Got agency information')      

            # Get agency level list of crimes
            with concurrent.futures.ThreadPoolExecutor(max_workers = 40) as executor:
                futures = [executor.submit(get_agency_crimes, agency_code)
                           for agency_code in agency_codes]

            # Flattend the output and create dataframe
            agency_crimes = [crime for future in concurrent.futures.as_completed(futures)
                                 for crime in future.result()]
            agency_crimes_df = pd.DataFrame(agency_crimes)
            agency_crimes_df.to_sql('agency_crimes', engine, if_exists='replace', index=False)
            logging.info('Got agency crimes data')
            
            # Plot something
            columns_to_plot = ['Rape', 'Aggravated Assault']    
            plot_scatter(state_crimes_df, 'data_year', columns_to_plot)

    except Exception as e:
        logging.error(f'An error occurred in the main function: {e}')
        print(f'An error occurred in the main function: {e}')
        raise
    
    finally:
        logging.info(f'Finished running at {datetime.datetime.now()}')
        

### EXECUTION ###
if __name__ == '__main__':
    main()
