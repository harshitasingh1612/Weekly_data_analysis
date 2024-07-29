from src import data_processing, schema

def main():
    dataProcessObj = data_processing.dataProcessing()
    metrics_df = dataProcessObj.read_csv_from_filepath('S&A - Written Project - Data Set - raw_data.csv')
    dataProcessObj.transform_data(metrics_df)
    
    # Decomposed_effects_by_country = dataProcessObj.decompose_and_calculate_effects_by_country(metrics_df)
    # Decomposed_effects_by_country_browser = dataProcessObj.decompose_and_calculate_effects_by_country_browser(metrics_df)
    # Decomposed_effects_by_dimension = dataProcessObj.decompose_and_calculate_effects_by_dimension(metrics_df, ['country', 'browser'])
    # print(Decomposed_effects_by_dimension)
    # dataProcessObj.analyze_decomposition(Decomposed_effects_by_country)
    #df = Decomposed_effects_by_dimension[(Decomposed_effects_by_dimension['country'] == 'AJ') & (Decomposed_effects_by_dimension['week_before'] == 6) & (Decomposed_effects_by_dimension['week_after'] == 7)]
    #print(df)
    # dataProcessObj.calculate_weekly_country_increase(metrics_df, 7)
    dataProcessObj.plot_metrics_per_country(metrics_df)
    # dataProcessObj.calculate_weekly_browser_increase(metrics_df, 7)
    # dataProcessObj.calculate_weekly_country_browser_increase(metrics_df, 7)
    # dataProcessObj.calculate_weekly_country_visits_increase(metrics_df, 7)
    # dataProcessObj.calculate_weekly_browser_visits_increase(metrics_df, 7)
    # dataProcessObj.calculate_weekly_country_browser_visits_increase(metrics_df, 7)
    # df = dataProcessObj.calculate_aggregates_by_country(metrics_df)
    # dataProcessObj.week_country_analysis(df)

if __name__ == "__main__":
    main()