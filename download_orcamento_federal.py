#
#Downloading all past data from the source 'Portal da Transparencia' (https://portaldatransparencia.gov.br)
#The files are:
#    
#       -Revenues (10 files, one for each year from 2014 to 2023)
#       -Expesenses Budget (10 files, one for each year from 2014 to 2023)
#       -Execution of expenses (117 files, one for each month in each year from 01/2014 to 09/2023)
#
#Total of 137 files.
#Date of download 15/09/2023 10:30 AM
#
import requests
import os
import zipfile
import time

def download_files (category, year):
    
    for element in year:

        
        # URL of the zip file you want to download
        zip_url = f'https://portaldatransparencia.gov.br/download-de-dados/{category}/{element}'
        
        # Specify the folder where you want to save the downloaded zip file
        download_folder = 'transparencia\\Dados\\' + category
        
        # Ensure the folder exists; create it if it doesn't
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)
        
        # File name for the downloaded zip file
        
        custom_filename = category + str(element) +'.zip'
        
        zip_filename = os.path.join(download_folder, custom_filename)
        
        # Send an HTTP GET request to the zip file URL

        user_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'} #Mimicks the chrome navigator
        response = requests.get(zip_url, headers = user_agent)
        
        if response.status_code == 200:
            # Save the downloaded zip file to the specified folder
            with open(zip_filename, 'wb') as file:
                file.write(response.content)
            
            print(f'Zip file downloaded successfully to {zip_filename}')
            
            # Wait until the file exists (maximum 60 seconds)
            max_wait_time = 60  # adjust this as needed
            wait_interval = 1   # Check every 1 second
            waited_time = 0
        
            while not os.path.exists(zip_filename) and waited_time < max_wait_time:
                time.sleep(wait_interval)
                waited_time += wait_interval
        
            if os.path.exists(zip_filename):
                # Unzip the downloaded zip file
                with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                    zip_ref.extractall(download_folder)
                
                print(f'Zip file extracted to {download_folder}')
                
                # Remove the downloaded zip file
                os.remove(zip_filename)
                
            else:
                print('File download completed, but the downloaded file was not found.')
        else:
            print(f"Failed to download the zip file. Status code: {response.status_code}")

    
# list containing years from 2014 to 2023 to iterate the function argument 'year'

years = [year for year in range(2014, 2024)]

# list containing years from 2014 to 2023 and months to iterate the function argument 'year'

years_months = [year * 100 + month for year in range(2014, 2024) for month in range(1, 13)]

#'Receitas' (Revenues) from 2014 to 2023 (only one file for a entire year)

download_files('receitas', years)

#'Orcamento da despesa' (Expesenses Budget) from 2014 to 2023 
#(only one file for a entire year)

download_files('orcamento-despesa', years)

#'Execucao da despesa' (Execution of expenses) from 2014 to 2023 
#(one file for each month)

download_files('despesas-execucao', years_months)



