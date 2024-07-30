import pandas as pd
import pytest
import unittest
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
        results = dataProcessing().decompose_and_calculate_effects_by_country(metrics_df, dimensions)
            
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

# Run the test
def main():
    testObj = TestDataProcessing()
    testObj.test_decomposition_function()

# Run the test
if __name__ == '__main__':
    unittest.main()