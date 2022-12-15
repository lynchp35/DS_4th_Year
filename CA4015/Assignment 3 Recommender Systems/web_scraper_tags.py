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
    
    def __init__(self, url_list, artistID_list):
        self.url_list = url_list
        self.artistID_list = artistID_list
        self.extra_artist_data = {"artistID":[],"tag1":[],  "tag2":[],  "tag3":[],  "tag4":[],  "tag5":[]}
        
    def start_scraping(self):
        
        tag_file = open("data/top_five_artists_tags.csv", "w")
        tag_file.write("artistID, tag1,  tag2, tag3, tag4, tag5\n")
        counter = 1
        for current_url, current_artistID in tqdm(zip(self.url_list,self.artistID_list)):
            try:
                response = requests.get(current_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                current_row = [np.nan]*6
                current_row[0] = (str(current_artistID))
                self.extra_artist_data["artistID"].append(current_artistID)
                for i, tag in enumerate(soup.find_all("li", "tag")[:5]):
                    self.extra_artist_data[f"tag{i+1}"].append(tag.text)
                    current_row[i+1] = tag.text
                current_line = ",".join(current_row)
                tag_file.write(current_line + "\n")
                if counter % 250 == 0:
                    tag_file.close()
                    tag_file = open("data/top_five_artists_tags.csv", "a")
                counter += 1 
                self.imput_null_values()
                    
            except:
                pass
        
        tag_file.close()
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
    artistID_list = artist["id"]
    scraper = FM_webscraper(artist_url, artistID_list)

    extra_artist_data = scraper.start_scraping()
    extra_artist_data = pd.DataFrame(extra_artist_data)

    path = "data/top_five_artists_tags_2.csv"
    extra_artist_data.to_csv(path,index=False)
