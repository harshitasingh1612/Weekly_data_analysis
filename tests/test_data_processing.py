import pandas as pd
import pytest
import unittest
from unittest.mock import patch
from src.data_processing import dataProcessing

class TestDataProcessing(unittest.TestCase):

    def test_decomposition_function(self):
    # Sample Data
        data = {
            'week': [1, 1, 2, 2],
            'country': ['A', 'B', 'A', 'B'],
            'browser': ['Chrome', 'Firefox', 'Chrome', 'Firefox'],
            'conversions': [100, 150, 200, 250],
            'visits': [1000, 1500, 2000, 2500]
        }
        
        metrics_df = pd.DataFrame(data)
        dimensions = ['country', 'browser']

        def get_next_decomposition_combination(possibilitiesLists, position):
            lists = reversed(possibilitiesLists)
            combination = []
            for lst in lists:
                index = position % len(lst)
                combination.insert(0, lst[index])
                position //= len(lst)
            return combination
        
    def decompose_and_calculate_effects(self, metrics_df, dimensions):
        results = dataProcessing().metric_change_decomposition_per_parameterized_dimensions(metrics_df, dimensions)
            
        # Create a DataFrame for results
        results_df = pd.DataFrame(results)

        # Test assertions
        assert not results_df.empty, "Results should not be empty"
        assert 'rate_change_effect' in results_df.columns, "Column 'rate_change_effect' missing"
        assert 'proportion_change_effect' in results_df.columns, "Column 'proportion_change_effect' missing"

        # Example check for a specific value
        assert results_df['rate_change_effect'].sum() != 0, "Rate change effects should not all be zero"
        assert results_df['proportion_change_effect'].sum() != 0, "Proportion change effects should not all be zero"
        
        print("All tests passed.")

        # results = []

        # # Get unique weeks
        # weeks = sorted(metrics_df['week'].unique())
        
        # possibilitiesLists = []
        # total_possibilities = 1
        # for dimension in dimensions:
        #     dimensionValues = metrics_df[dimension].unique()
        #     total_possibilities *= len(dimensionValues)
        #     possibilitiesLists.append(dimensionValues)
        
        # # Iterate over each pair of consecutive weeks
        # for i in range(1, len(weeks)):
        #     week_before = weeks[i-1]
        #     week_after = weeks[i]
            
        #     # Calculate total conversions and visits for each week
        #     total_visits_before = metrics_df.loc[metrics_df['week'] == week_before]['visits'].sum()
        #     total_visits_after = metrics_df.loc[metrics_df['week'] == week_after]['visits'].sum()

        #     for nextPossibility in range(total_possibilities):
        #         combination = get_next_decomposition_combination(possibilitiesLists, nextPossibility)
        #         # Extract data for each period
        #         query_str = ' & '.join("{} == '{}'".format(dimensions[x], combination[x]) for x in range(len(dimensions)))
        #         subset_before = metrics_df.query("{} & week == {}".format(query_str, week_before))
        #         subset_after = metrics_df.query("{} & week == {}".format(query_str, week_after))
                
        #         # Calculate conversion rates and proportions
        #         rate_before = (subset_before['conversions'].sum() / 
        #                     subset_before['visits'].sum() if subset_before['visits'].sum() != 0 else 0)
        #         rate_after = (subset_after['conversions'].sum() / 
        #                     subset_after['visits'].sum() if subset_after['visits'].sum() != 0 else 0)
        #         proportion_before = subset_before['visits'].sum() / total_visits_before
        #         proportion_after = subset_after['visits'].sum() / total_visits_after
                
        #         # Calculate effects
        #         rate_change = proportion_after * (rate_after - rate_before)
        #         proportion_change = rate_before * (proportion_after - proportion_before)
                
        #         # Append results
        #         res_dict = dict()
        #         for x in range(len(dimensions)):
        #             res_dict[dimensions[x]] = combination[x]

        #         res_dict['week_before'] = week_before
        #         res_dict['week_after'] = week_after
        #         res_dict['rate_change_effect'] = rate_change
        #         res_dict['proportion_change_effect'] = proportion_change
        #         results.append(res_dict)
            
        # # Create a DataFrame for results
        # results_df = pd.DataFrame(results)
        
        # # Assertions
        # assert not results_df.empty, "Results should not be empty"
        # assert 'rate_change_effect' in results_df.columns, "Missing 'rate_change_effect'"
        # assert 'proportion_change_effect' in results_df.columns, "Missing 'proportion_change_effect'"
        
        # # Print the results for manual inspection
        # print(results_df)

        # print("All tests passed.")

# Run the test
def main():
    testObj = TestDataProcessing()
    testObj.test_decomposition_function()

# Run the test
if __name__ == '__main__':
    unittest.main()