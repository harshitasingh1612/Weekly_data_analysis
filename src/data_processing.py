import pandas as pd
from urllib.request import urlretrieve
from urllib.parse import urlparse
from pathlib import Path
from . import schema
#from spark_setup import build_spark_session
import matplotlib.pyplot as plt

class dataProcessing:
    def download_file_from_URL(self, url:urlparse, file_location:str):
        """Download file from the specified URL and save it to the target location.

        Args:
            url: URL of the file

        Returns:
            Saves file locally
        """
        # Download the file from URL and save it to the directory
        urlretrieve(url, file_location)


    def read_csv_from_filepath(self, file_location: str) -> pd.DataFrame:
        """Reads csv file from the specified path.

        Args:
            spark: SparkSession.
            path: Location of the file

        Returns:
            A pandas DataFrame
        """

        # if the specified path exists, return the DataFrame else throw an exception
        return pd.read_csv(file_location, sep=',', dtype=schema.metrics_schema)

    def transform_data(self, metrics_df: pd.DataFrame):

        # Calculate conversion rate
        weekly_aggregates_df = metrics_df.groupby([pd.Grouper(key='week')])[['visits', 'conversions']].sum()
        weekly_aggregates_df = weekly_aggregates_df.reset_index()
        weekly_aggregates_df = weekly_aggregates_df.rename(
            columns= {
                'visits': 'weekly_visits',
                'conversions': 'weekly_conversions'
            }
        )
        
        weekly_aggregates_df['weekly_conversion_rate'] = weekly_aggregates_df['weekly_conversions']/weekly_aggregates_df['weekly_visits']
        
        # Plot the conversion rate over time
        plt.figure(figsize=(12, 6))
        plt.rcParams["font.weight"] = "bold"
        plt.rcParams["axes.labelweight"] = "bold"
        plt.xticks(weekly_aggregates_df['week'])
        plt.plot(weekly_aggregates_df.index, weekly_aggregates_df['weekly_conversion_rate'], marker='o', linestyle='-')
        plt.title('Weekly Conversion Rate Over Time')
        plt.xlabel('Week')
        plt.ylabel('Conversion Rate')
        plt.grid(True)
        plt.show()

        return weekly_aggregates_df

    def metric_change_decomposition_per_country(self, metrics_df):
        results = []

        # Get unique weeks
        weeks = sorted(metrics_df['week'].unique())
        
        # Iterate over each pair of consecutive weeks
        for i in range(1, len(weeks)):
            week_before = weeks[i-1]
            week_after = weeks[i]
            
            # Calculate total conversions and visits for each week
            total_conversions_before = metrics_df.loc[metrics_df['week'] == week_before]['conversions'].sum()
            total_visits_before = metrics_df.loc[metrics_df['week'] == week_before]['visits'].sum()
            total_conversions_after = metrics_df.loc[metrics_df['week'] == week_after]['conversions'].sum()
            total_visits_after = metrics_df.loc[metrics_df['week'] == week_after]['visits'].sum()
            
            # Iterate over each country
            for country in metrics_df['country'].unique():
                # Extract data for each week
                country_data_before = metrics_df[(metrics_df['country'] == country) & (metrics_df['week'] == week_before)]
                country_data_after = metrics_df[(metrics_df['country'] == country) & (metrics_df['week'] == week_after)]
                
                # Calculate conversion rates and proportions
                rate_before = (country_data_before['conversions'].sum() / 
                            country_data_before['visits'].sum() if country_data_before['visits'].sum() != 0 else 0)
                rate_after = (country_data_after['conversions'].sum() / 
                            country_data_after['visits'].sum() if country_data_after['visits'].sum() != 0 else 0)
                proportion_before = country_data_before['visits'].sum() / total_visits_before
                proportion_after = country_data_after['visits'].sum() / total_visits_after
                
                # Calculate effects
                rate_change = proportion_after * (rate_after - rate_before)
                proportion_change = rate_before * (proportion_after - proportion_before)
                
                # Append results
                results.append({
                    'country': country,
                    'week_before': week_before,
                    'week_after': week_after,
                    'rate_change_effect': rate_change,
                    'proportion_change_effect': proportion_change
                })

        # Create a DataFrame for results
        results_df = pd.DataFrame(results)
        return results_df

    def calculate_week_metrics(self, metrics_df, week_before, week_after):
        
        # Calculate total conversions and visits for each week
        total_conversions_before = metrics_df.loc[metrics_df['week'] == week_before]['conversions'].sum()
        total_visits_before = metrics_df.loc[metrics_df['week'] == week_before]['visits'].sum()
        total_conversions_after = metrics_df.loc[metrics_df['week'] == week_after]['conversions'].sum()
        total_visits_after = metrics_df.loc[metrics_df['week'] == week_after]['visits'].sum()

        return total_conversions_before, total_visits_before, total_conversions_after, total_visits_after

    def get_next_decomposition_combination(self, possibilitiesLists, position):
        lists = reversed(possibilitiesLists)
        combination = []
        for lst in lists:
            index = position % len(lst)
            combination.insert(0, lst[index])
            position //= len(lst)
        return combination

    def metric_change_decomposition_per_country_per_browser(self, metrics_df):
        results = []

        # Get unique weeks
        weeks = sorted(metrics_df['week'].unique())
        
        # Iterate over each pair of consecutive weeks
        for i in range(1, len(weeks)):
            week_before = weeks[i-1]
            week_after = weeks[i]
            
            # # Calculate total conversions and visits for each week
            total_visits_before = metrics_df.loc[metrics_df['week'] == week_before]['visits'].sum()
            total_visits_after = metrics_df.loc[metrics_df['week'] == week_after]['visits'].sum()
        
            # Iterate over each country
            for country in metrics_df['country'].unique():
                for browser in metrics_df['browser'].unique():
                # Extract data for each period
                    subset_before = metrics_df[(metrics_df['country'] == country) & 
                                    (metrics_df['browser'] == browser) & 
                                    (metrics_df['week'] == week_before)]
                    subset_after = metrics_df[(metrics_df['country'] == country) & 
                                    (metrics_df['browser'] == browser) & 
                                    (metrics_df['week'] == week_after)]

                    # Calculate conversion rates and proportions
                    rate_before = (subset_before['conversions'].sum() / 
                                subset_before['visits'].sum() if subset_before['visits'].sum() != 0 else 0)
                    rate_after = (subset_after['conversions'].sum() / 
                                subset_after['visits'].sum() if subset_after['visits'].sum() != 0 else 0)
                    proportion_before = subset_before['visits'].sum() / total_visits_before
                    proportion_after = subset_after['visits'].sum() / total_visits_after
                    
                    # Calculate effects
                    rate_change = proportion_after * (rate_after - rate_before)
                    proportion_change = rate_before * (proportion_after - proportion_before)
                    
                    
                    # Append results
                    results.append({
                        'country': country,
                        'browser': browser,
                        'week_before': week_before,
                        'week_after': week_after,
                        'rate_change_effect': rate_change,
                        'proportion_change_effect': proportion_change
                    })

        # Create a DataFrame for results
        results_df = pd.DataFrame(results)
        print(results_df)
        return results_df

    def metric_change_decomposition_per_parameterized_dimensions(self, metrics_df, dimensions):
        results = []

        # Get unique weeks
        weeks = sorted(metrics_df['week'].unique())
        
        possibilitiesLists = []
        total_possibilities = 1
        for dimension in dimensions:
            dimensionValues = metrics_df[dimension].unique()
            total_possibilities *= len(dimensionValues)
            possibilitiesLists.append(dimensionValues)
        
        # Iterate over each pair of consecutive weeks
        for i in range(1, len(weeks)):
            week_before = weeks[i-1]
            week_after = weeks[i]
            
            # # Calculate total conversions and visits for each week
            total_visits_before = metrics_df.loc[metrics_df['week'] == week_before]['visits'].sum()
            total_visits_after = metrics_df.loc[metrics_df['week'] == week_after]['visits'].sum()

            for nextPossibility in range(total_possibilities):
                combination = get_next_decomposition_combination(possibilitiesLists, nextPossibility)
                # Extract data for each period
                query_str = ' & '.join("{} == '{}'".format(dimensions[x], combination[x]) for x in range(len(dimensions)))
                subset_before = metrics_df.query("{} & week == {}".format(query_str, week_before))
                subset_after = metrics_df.query("{} & week == {}".format(query_str, week_after))
                
                # Calculate conversion rates and proportions
                rate_before = (subset_before['conversions'].sum() / 
                            subset_before['visits'].sum() if subset_before['visits'].sum() != 0 else 0)
                rate_after = (subset_after['conversions'].sum() / 
                            subset_after['visits'].sum() if subset_after['visits'].sum() != 0 else 0)
                proportion_before = subset_before['visits'].sum() / total_visits_before
                proportion_after = subset_after['visits'].sum() / total_visits_after
                
                # Calculate effects
                rate_change = proportion_after * (rate_after - rate_before)
                proportion_change = rate_before * (proportion_after - proportion_before)
                # Append results
                res_dict = dict()
                for x in range(len(dimensions)):
                    res_dict[dimensions[x]] = combination[x]

                res_dict['week_before'] = week_before
                res_dict['week_after'] = week_after
                res_dict['rate_change_effect'] = rate_change
                res_dict['proportion_change_effect'] = proportion_change
                results.append(res_dict)
            
        
        # Create a DataFrame for results
        results_df = pd.DataFrame(results)
        print(results_df)
        return results_df
