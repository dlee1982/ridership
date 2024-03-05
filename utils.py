import pandas as pd
import numpy as np
import requests
from functools import cached_property

class DataLoader: 
    """
    Base data loader class not intended for direct use
    """

    def __init__(self, ridership_choice, max_records) -> None: 
        self.url_path = ""

    @cached_property
    def dataset(self): 
        """
        loads the dataset only once 
        """

        return self.load_dataset()
    
    def load_dataset(self) -> None:
        """
        To be implemented by subclasses. Defines how datasets are loaded.
        """
        
        raise NotImplementedError('To be implemented by subclasses.')
    
class GetRiderData(DataLoader):
    "class for loading the ridership dataset from City of Chicago"

    def __init__(self, ridership_choice="daily_total", max_records=10_000) -> None:
        super().__init__(ridership_choice, max_records)
        self.ridership_choice = ridership_choice
        if self.ridership_choice == "daily_total": 
            self.url_path = "https://data.cityofchicago.org/resource/6iiy-9s97.json"
            self.limit = 1000
            self.max_records = max_records
            self.sort_order = "&$order=service_date DESC"
        if self.ridership_choice == "stations": 
            self.url_path = "https://data.cityofchicago.org/resource/5neh-572f.json"
            self.limit = 1000
            self.max_records = max_records
            self.sort_order = "&$order=date DESC"

    def load_dataset(self) -> pd.DataFrame:
        """
        limit: total limit of data
        max_records: total number of records
        base_url_add: api url address
        """
        
        base_url = self.url_path
        records = []
        offset = 0
        limit = self.limit
        max_records = self.max_records
        sort_order = self.sort_order

        while offset < max_records:
            query_url = f"{base_url}?$limit={limit}&$offset={offset}{sort_order}"
            response = requests.get(query_url)

            if response.status_code == 200:
                data = response.json()
                if not data:  # Break the loop if no data is returned
                    break
                records.extend(data)
                offset += limit
            else:
                print(f"Failed to retrieve data at offset {offset}")
                break

        if records: 
            return self.tweak_data(pd.DataFrame(records))
            # return pd.DataFrame(records)
        else: 
            print("No records returned")

    def tweak_data(self, _df:pd.DataFrame) -> pd.DataFrame: 
        """
        creates the year_only, month_only and year_month columns
        """

        def create_year_month(_df:pd.DataFrame) -> pd.DataFrame: 
            """
            creates the year_month column
            """
            
            return (_df
                    .assign(year_month = lambda _df: _df.year_only + "-" + _df.month_only)
                    .drop(columns=['month_only'])
                    )
        
        cols_list = list(_df.columns)

        if "service_date" and "rail_boardings" in _df.columns.to_list():
            return (_df
                    .astype(
                        {
                            'service_date':'datetime64[ns]', 
                            'day_type':'string', 
                            'bus':'int', 
                            'rail_boardings':'int', 
                            'total_rides':'int'
                        }
                            )
                    .assign(year_only = lambda _df: _df.service_date.dt.year.astype(str), 
                            month_only = lambda _df: np.where(_df.service_date.dt.month.astype(str).str.len() < 2, 
                                                              "0" + (_df.service_date.dt.month.astype(str)), 
                                                              _df.service_date.dt.month.astype(str))
                            )
                    .pipe(create_year_month)
                )