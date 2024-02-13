import urllib
import os
import logging

# import airflow.task logger
_LOGGER = logging.getLogger("airflow.task")
def fetch_csv_data(url, output_dir):
    """
    Function to fetch data from an URL.
    
    Args:
        url (str): The URL from which to fetch data.
        output_dir (str): The directory where the downloaded file will be saved.
    
    Returns:
        -
    """
    try:
        # Open the URL and download the file
        response = urllib.request.urlopen(url)
        # Extract filename from URL
        filename = os.path.join(output_dir, url.split('/')[-1])
        with open(filename, 'wb') as f:
            f.write(response.read())
                
        _LOGGER.info(f"Downloaded: {filename}")
        print(f"Downloaded: {filename}")
    except Exception as e:
        _LOGGER.warning(f"Failed to download {url}: {e}")

      
