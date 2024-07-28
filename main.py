from src import data_processing, schema

def main():
    dataProcessObj = data_processing.dataProcessing()
    metrics_df = dataProcessObj.read_csv_from_filepath('S&A - Written Project - Data Set - raw_data.csv')
    weekly_aggregates_df = dataProcessObj.transform_data(metrics_df)
    # metrics_df = metrics_df[(metrics_df['country'] == 'AJ') & (metrics_df['browser'] == 'safari')]
    conversion_rate_decomposition_per_country = dataProcessObj.decompose_and_calculate_effects_by_country(metrics_df)
    conversion_rate_decomposition_per_country_per_browser = dataProcessObj.decompose_and_calculate_effects_by_country_browser(metrics_df)
    conversion_rate_decomposition_per_parameterized_dimensions = dataProcessObj.decompose_and_calculate_effects_by_dimension(metrics_df, ['country', 'browser'])
    dataProcessObj.analyze_decomposition(conversion_rate_decomposition_per_parameterized_dimensions)
if __name__ == "__main__":
    main()