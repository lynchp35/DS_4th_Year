import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

class Daft_web_scraper:
    
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
    
    def __init__(self, url, start_page, end_page, page_size=20):
        self.url = url
        self.start_page = start_page
        self.end_page = end_page
        self.page_size = page_size
        self.property_dict = {"Bed":[],
                              "Bath":[],
                              "m²":[],
                              "Price":[],
                              "Address":[],
                              "Property_type":[],
                              "New_build":[],
                              "Page":[],
                    }
        
    def start_scraping(self):
        """ 
        The function is used to start the scraping process.
        It uses a for loop to go through all the pages in [start_page, end_page],
        and BeautifulSoup() to retreive the page data.
        
        The soup is then passed the extract_data function.
        """
        
        for i in range(self.start_page, self.end_page):
            current_url = f"{self.url}?from={i*self.page_size}&pageSize={self.page_size}"
            response = requests.get(current_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            self.extract_data(soup, i)
        
        return self.property_dict


    def extract_data(self, soup, i):
        
        """ 
        This function is used to find all property listings using the 'soup' then it checks if the listing is a single listing
        or part of a sublisting.
        A sublisting is common for new builds as the builder is selling multiple properties in the new house developement.
        
        If the listing is a single listing the data is extracted using the extract_single_listing function.
        If it is a sublisting (multiple listings) the data is extracted using the extract_sub_listing function.
        """
        
        for property_listings in soup.find_all("li", "SearchPage__Result-gg133s-2 djuMQD"):

            try:
                property_address = property_listings.find_all("p", "TitleBlock__Address-sc-1avkvav-8 dzihyY")[0].text
                self.extract_single_listing(property_listings, i)
            except IndexError:
                property_address = property_listings.find_all("p", "TitleBlock__Address-sc-1avkvav-8 hCMmam")[0].text
                self.extract_sub_listing(property_listings, i)
            
            # In some cases a feature is missing from the listing e.g no data on floor-area, this function appeneds a null value.
            self.imput_null_values()


    def extract_single_listing(self, property_listings, i):
        
        """
        This function extracts data for single listing properties 
        The parameter property_listings is passed from the extract_data function.
        
        This function finds the following features:
        
        Price: The price of the property, typically of type int but maybe string.
        New_build: For the case of single listing this value is no.
        Address: The address of the property.
        Page: The page the listing is found, used to help find errors.
        Property_type: The type of property, such as House, Apartment, etc.
        
        More features are extracted using the extract_property_details function.
        """

        property_price = property_listings.find_all("span", "TitleBlock__StyledSpan-sc-1avkvav-5 fKAzIL")[0].text
        property_address = property_listings.find_all("p", "TitleBlock__Address-sc-1avkvav-8 dzihyY")[0].text
        property_type = property_listings.find_all("p", "TitleBlock__CardInfoItem-sc-1avkvav-9 cKZZql")

        # Price is typically in the format €xxx,xxx but in some cases it could be a string such as Price at application.
        property_price = self.convert_price_to_int(property_price, i)

        self.property_dict["Price"].append(property_price)
        self.property_dict["New_build"].append("no")
        self.property_dict["Address"].append(property_address)
        self.property_dict["Page"].append(i)

        if len(property_type) > 0:
            self.property_dict["Property_type"].append(property_type[0].text)
        else:
            self.property_dict["Property_type"].append(np.nan)



        property_details = property_listings.find_all("p", "TitleBlock__CardInfoItem-sc-1avkvav-9 iLMdur")
        # This function tries to extract the number of beds, baths and the floor-area.
        self.extract_property_details(property_details)



    def extract_sub_listing(self, property_listings, i):
        
        """
        This function extracts data for sublisting properties 
        The parameter property_listings is passed from the extract_data function.
        
        This function finds the following features:
        
        Price: The price of the property, typically of type int but maybe string.
        New_build: For the case of single listing this value is no.
        Address: The address of the property.
        Page: The page the listing is found, used to help find errors.
        m²: In this case all values are null, since not all properties in the listing are of equal size.
        
        More features are extracted using the extract_sub_listing_property_details function.
        """
        
        property_address = property_listings.find_all("p", "TitleBlock__Address-sc-1avkvav-8 hCMmam")[0].text
        sub_listings = property_listings.find_all("div", "SubUnit__StyledCol-sc-10x486s-4 bIjqYp")
        for listing in sub_listings:
            property_price = listing.find_all("p","SubUnit__Title-sc-10x486s-5 feGTKf")

            if len(property_price) > 0:
                property_price = self.convert_price_to_int(property_price[0].text, i)
                details = listing.find_all("div","SubUnit__CardInfoItem-sc-10x486s-7 YYbRy")
                property_details = details[0].text.split(" · ")
                
                # This function tries to extract the number of beds, baths and the property_type.
                self.extract_sub_listing_property_details(property_details)

                self.property_dict["m²"].append(np.nan)    
                self.property_dict["Price"].append(property_price)    
                self.property_dict["Address"].append(property_address)    
                self.property_dict["New_build"].append("yes")
                self.property_dict["Page"].append(i)

    def convert_price_to_int(self, price, i):
        
        """
        This function tries to convert the price to an int if not returns the orginal string.
        """
        try:
            price = int("".join(price.split("€")[1].split(",")))
        except:
            if price.strip() == "Price on Application" or price.strip() == "AMV: Price on Application":
                price = "Price on Application"
            else:
                print(price, i)
                price = price.strip()
        return price

    def extract_property_details(self, property_details):
        
        """
        This function extracts the number of beds, baths and floor-area for single listed properties.
        """

        for detail in property_details:
            try:
                detail_value = detail.text.split()[0]
                detail_type = detail.text.split()[1]
            except:
                print(detail)

            if detail_type in ["Bed","Bath","m²"]:
                try:
                    self.property_dict[detail_type].append(int(detail_value))
                except ValueError:
                    self.property_dict[detail_type].append(detail_value)
            elif detail_type == "ac":
                size = self.convert_ac_m2(detail_value)
                self.property_dict["m²"].append(size)
            else:
                print(f"Found {detail}, while trying to extract bed, bath and floor-area")

    def extract_sub_listing_property_details(self, property_details):
        
        """
        This function extracts the number of beds, baths and property_type for sublisted properties.
        """

        for detail in property_details[:-1]:
            try:
                detail_value = detail.split(" ")[0]
                detail_type = detail.split(" ")[1]
            except:
                print(detail)

            self.property_dict[detail_type].append(detail_value)

        self.property_dict["Property_type"].append(property_details[-1])

    def convert_ac_m2(self, value):
        """
        It is common for sites to be listed using acres, this converts it to m2.
        """
        
        return float(value) / (0.00024711)

    def imput_null_values(self):
        
        """
        Appends a null value for features in the dict which are missing an entry. 
        """
        maxLen = max([len(self.property_dict[key]) for key in self.property_dict])
        for key in self.property_dict:
            if len(self.property_dict[key]) < maxLen:
                self.property_dict[key].append(np.nan)

        if sum(np.array([len(self.property_dict[key]) for key in self.property_dict]) == maxLen) != len(self.property_dict):
            print("Error dictionary not of uniform length")
            print([(key, len(self.property_dict[key])) for key in self.property_dict])
            self.imput_null_values()

if __name__ == "__main__":
    path = "data/"
    url = "https://www.daft.ie/property-for-sale/ireland"
    
    start_page = 0
    end_page = 987
    page_size = 20
    
    s1 = Daft_web_scraper(url, start_page, end_page)

    property_dict = s1.start_scraping()
    
    properties = pd.DataFrame(property_dict)
    
    properties.to_csv(f"{path}daft_from_page_{start_page}_till_page_{end_page}_by_{page_size}.csv")
