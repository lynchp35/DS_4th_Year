import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import re

class FM_webscraper:
    
    """ 
    The goal of this class is to scrape property data from the Irish property site Daft.ie.
    The class utilizes PySpark for parralization.
    Creating ....
    
    The parameters for the class are:
    url: This is the base url the model will use. Allows you to search the whole of Ireland or can use a differnt url for a small search.
    start_page: what page the model will start scraping from. Will use for parralization.
    end_page: what page the model will stop scraping at. Will use for parralization.
    page_size: The number of property listings each page should return. Usually set to 20.
    """
    
    def __init__(self, url_list):
        self.url_list = url_list
        self.extra_artist_data = {"Time":[], "Sim1":[],  "Sim2":[],  "Sim3":[]}
        
    def start_scraping(self):

        for current_url in tqdm(self.url_list):
            response = requests.get(current_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            try:
                time = soup.find("dd", "catalogue-metadata-description").text
            except AttributeError:
                time = np.nan
            for i, sim_artists in enumerate(soup.find_all("div", "artist-similar-artists-sidebar-item")):
                self.extra_artist_data[f"Sim{i+1}"].append(sim_artists.find("a", "link-block-target").text)
            self.extra_artist_data["Time"].append(time)

            self.imput_null_values()
        
        return self.extra_artist_data

    def imput_null_values(self):

        """
        Appends a null value for features in the dict which are missing an entry. 
        """
        maxLen = max([len(self.extra_artist_data[key]) for key in self.extra_artist_data])
        for key in self.extra_artist_data:
            if len(self.extra_artist_data[key]) < maxLen:
                self.extra_artist_data[key].append(np.nan)

        if sum(np.array([len(self.extra_artist_data[key]) for key in self.extra_artist_data]) == maxLen) != len(self.extra_artist_data):
            print("Error dictionary not of uniform length")
            self.imput_null_values()

if __name__ == "__main__":
    

    artist = pd.read_csv("data/artists.dat", sep="\t")
    artist_url = artist["url"]
    scraper = FM_webscraper(artist_url)

    extra_artist_data = scraper.start_scraping()
    extra_artist_data = pd.DataFrame(extra_artist_data)

    path = "data/extra_artist_data.csv"
    extra_artist_data.to_csv(path)
