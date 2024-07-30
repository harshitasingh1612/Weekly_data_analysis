import pandas as pd
from urllib.request import urlretrieve
from urllib.parse import urlparse
from pathlib import Path
from . import schema
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator
import matplotlib as mpl
import seaborn as sns
import math
from matplotlib.animation import FuncAnimation

class dataProcessing:

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

    def transform_data(self, metrics_df: pd.DataFrame) -> pd.DataFrame:
        """Reads the pandas Dataframe and calculate the weekly conversion rate
        across all the countries and browsers

        Args:
            metrics_df: Dataframe of the csv dataset

        Returns:
            A weekly aggregated Dataframe
        """
        # Calculate conversion rate
        weekly_aggregates_df = metrics_df.groupby([pd.Grouper(key='week')]).agg({'visits': 'sum', 'conversions': 'sum'})
        weekly_aggregates_df = weekly_aggregates_df.reset_index()
        weekly_aggregates_df = weekly_aggregates_df.rename(
            columns= {
                'visits': 'weekly_visits',
                'conversions': 'weekly_conversions'
            }
        )
        
        weekly_aggregates_df['weekly_conversion_rate'] = weekly_aggregates_df['weekly_conversions']/weekly_aggregates_df['weekly_visits']
        self.plot_with_dynamic_axes(weekly_aggregates_df, 'week', 'weekly_conversion_rate')
        self.plot_with_dynamic_axes(weekly_aggregates_df, 'week', 'weekly_visits')
        
        # Plot the conversion rate over time
        # plt.figure(figsize=(12, 6))
        # plt.rcParams["font.weight"] = "bold"
        # plt.rcParams["axes.labelweight"] = "bold"
        # plt.gca().ticklabel_format(axis='y', style='plain')
        # plt.xticks(weekly_aggregates_df['week'])
        # plt.plot(weekly_aggregates_df.index, weekly_aggregates_df['weekly_conversion_rate'], marker='o', linestyle='-')
        # plt.title('Weekly Conversion Rate Over Time')
        # plt.xlabel('Week')
        # plt.ylabel('Conversion Rate')
        # plt.grid(True)
        # plt.show()

        # # Plot the global visitors over time
        # plt.figure(figsize=(12, 6))
        # plt.rcParams["font.weight"] = "bold"
        # plt.rcParams["axes.labelweight"] = "bold"
        # plt.gca().ticklabel_format(axis='y', style='plain')
        # ax = weekly_aggregates_df['weekly_visits'].plot()
        # ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        # plt.xticks(weekly_aggregates_df['week'])
        # plt.plot(weekly_aggregates_df.index, weekly_aggregates_df['weekly_visits'], marker='o', linestyle='-')
        # plt.title('Weekly visits Over Time')
        # plt.xlabel('Week')
        # plt.ylabel('Global Visits')
        # plt.grid(True)
        # plt.show()

        return weekly_aggregates_df

    def decompose_and_calculate_effects_by_country(self, metrics_df: pd.DataFrame)  -> pd.DataFrame:
        """Reads the Dataframe and decomposes the week-on-week conversion rate 
        changes into rate and proportion changes per country.

        Args:
            metrics_df: Dataframe of the csv dataset

        Returns:
            A DataFrame with the decomposition results for each country
        """
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

    def get_next_decomposition_combination(self, possibilitiesLists: list, position: int)-> list:
        """Generate a specific combination dynamically based on a given position

        Args:
            possibilitiesLists: A list of possibilities
            position: represents the index of the desired combination.

        Returns:
            a combination at a specified position
        """
        lists = reversed(possibilitiesLists)
        combination = []
        for lst in lists:
            index = position % len(lst)
            combination.insert(0, lst[index])
            position //= len(lst)
            
        return combination

    def decompose_and_calculate_effects_by_country_browser(self, metrics_df)-> pd.DataFrame:
        """Reads the Dataframe and decomposes the week-on-week conversion rate 
        changes into rate and proportion changes per country and browser.

        Args:
            possibilitiesList : 
            metrics_df: Dataframe of the csv dataset

        Returns:
            A DataFrame with the decomposition results for each country and browser
        """
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
        return results_df

    def decompose_and_calculate_effects_by_dimension(self, metrics_df: pd.DataFrame, dimensions:list)-> pd.DataFrame:
        """Reads the Dataframe and decomposes the week-on-week conversion rate 
        changes into rate and proportion changes per dimension.

        Args:
            metrics_df: Dataframe of the csv dataset
            dimensions : parameter that you want to decompose the metric change with

        Returns:
            A DataFrame with the decomposition results for each dimension
        """
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
                combination = self.get_next_decomposition_combination(possibilitiesLists, nextPossibility)
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

        results_df.to_csv('enriched_data.csv')
        return results_df
    
    def analyze_decomposition_by_dimension(self, results_df: pd.DataFrame, dimension):
        """
        This function helps us to identify and visualise the 
        rate change and proportion change effects per country/browser.

        Args:
            results_df : Decomposed dataframe
        """
        # Summarize total effects
        total_rate_change = results_df['rate_change_effect'].sum()
        total_proportion_change = results_df['proportion_change_effect'].sum()
        
        print(f"Total Rate Change Effect: {total_rate_change}")
        print(f"Total Proportion Change Effect: {total_proportion_change}")

        # Sort by contribution
        sorted_rate_change = results_df.sort_values(by='rate_change_effect', ascending=False)
        sorted_proportion_change = results_df.sort_values(by='proportion_change_effect', ascending=False)

        # Visualize top contributors
        top_rate_contributors = sorted_rate_change.head(10)
        top_proportion_contributors = sorted_proportion_change.head(10)
        top_rate_contributors  = top_rate_contributors.reset_index()
        top_proportion_contributors  = top_proportion_contributors.reset_index()

        col_name = dimension
        if isinstance(dimension, list):
            col_name = ' - '.join(dimension)
            top_rate_contributors[col_name] = top_rate_contributors[dimension].apply(lambda x: ' - '.join(x.astype(str)), axis=1)
            top_proportion_contributors[col_name] = top_proportion_contributors[dimension].apply(lambda x: ' - '.join(x.astype(str)), axis=1)
        
        self.plot_with_dynamic_axes(top_rate_contributors, col_name , 'rate_change_effect')
        self.plot_with_dynamic_axes(top_proportion_contributors, col_name, 'proportion_change_effect')
        
    def analyze_metrics_per_country(self, weekly_country_data):
        groupByColumns = ['week', 'country']
        weekly_country_data = weekly_country_data.groupby(groupByColumns).agg({'visits': 'sum', 'conversions': 'sum'}).reset_index()
        total_visits_df = weekly_country_data.groupby(['week']).agg({'visits': 'sum'}).reset_index()
        total_visits_df = total_visits_df.rename(columns= {'visits': 'global_visits'})
        merged_df = pd.merge(weekly_country_data, total_visits_df, on='week')
        merged_df['conversion_rate'] = merged_df['conversions'] / merged_df['global_visits']

        # Plot the global conversion rate per country over time
        self.plot_with_dynamic_axes(merged_df, 'week', 'conversion_rate', 'country')

        # Plot the visits per country over time
        self.plot_with_dynamic_axes(merged_df, 'week', 'visits', 'country', add_locator=True)
        
    def comma_format(self,x, pos):
        if x == 0:
            return "0"
        elif x % 1 == 0:
            return f'{int(x):,}'  # Format as int with commas
        else:
            return f'{x:,.2f}'  # Format as float with commas, two decimal places

    # Dynamic plotting function
    def plot_with_dynamic_axes(self, df, x_col, y_col, group_by_col=None, add_locator=False):
        plt.figure(figsize=(12, 6))
        plt.rcParams["font.weight"] = "bold"
        plt.rcParams["axes.labelweight"] = "bold"
        #plt.xticks(df[x_col])
        plt.gca().ticklabel_format(axis='y', style='plain')
        max_value = 0  # Initialize max_value for dynamic y-axis limit
       
        # Determine the data type of x_col
        if pd.api.types.is_numeric_dtype(df[x_col]):
            is_numeric_x = True
        else:
            is_numeric_x = False

        if group_by_col:
            # Group the data by the specified column
            for label, row in df.groupby(group_by_col):
                if is_numeric_x:
                    plt.plot(row[x_col], row[y_col], marker='o', linestyle='-', label=label)
                else:
                    plt.bar(row[x_col].astype(str), row[y_col], label=label)
                max_value = max(max_value, row[y_col].max())  # Update max_value if higher value is found
            plt.legend(title=group_by_col.title())
        else:
            # Plot data without grouping
            if is_numeric_x:
                plt.plot(df[x_col], df[y_col], marker='o', linestyle='-')
            else:
                plt.bar(df[x_col].astype(str), df[y_col])
            max_value = df[y_col].max()

        # Handle x-axis label formatting
        if not is_numeric_x or isinstance(df[x_col].iloc[0], list):
            plt.xticks(df[x_col].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x)), rotation=45, ha='right')
        else:
            plt.xticks(df[x_col])

        plt.title(f'{y_col.replace("_", " ").title()} Over {x_col.replace("_", " ").title()}' + (f' by {group_by_col.title()}' if group_by_col else ''))
        plt.xlabel(x_col.replace("_", " ").title())
        plt.ylabel(y_col.replace("_", " ").title())

        # Apply comma formatting to the y-axis
        ax = plt.gca()
        ax.yaxis.set_major_formatter(FuncFormatter(self.comma_format))  
        if add_locator:
            ax.yaxis.set_minor_locator(MultipleLocator(250000))
            # Set major ticks with 500,000 increments for the whole range
            ax.yaxis.set_major_locator(MultipleLocator(250000))
        if is_numeric_x:
            plt.grid(True)  # Only show gridlines for line charts (numeric x-axis)
        else:
            plt.grid(False)
        plt.tight_layout()
        plt.show()
