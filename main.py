from src import data_processing, schema

def main():
    dataProcessObj = data_processing.dataProcessing()
    metrics_df = dataProcessObj.read_csv_from_filepath('S&A - Written Project - Data Set - raw_data.csv')
    aggregate = dataProcessObj.transform_data(metrics_df)
    weekly_country_data = dataProcessObj.analyze_metrics_per_country(metrics_df)
    decomposed_effects_by_country = dataProcessObj.decompose_and_calculate_effects_by_country(metrics_df)
    decomposed_effects_by_country_browser = dataProcessObj.decompose_and_calculate_effects_by_country_browser(metrics_df)
    decomposed_effects_by_dimension = dataProcessObj.decompose_and_calculate_effects_by_dimension(metrics_df, ['country', 'browser'])
    dataProcessObj.analyze_decomposition_by_dimension(decomposed_effects_by_dimension, ['country'])
    dataProcessObj.analyze_decomposition_by_dimension(decomposed_effects_by_dimension,  ['country', 'browser'])
if __name__ == "__main__":
    main()