import pandas


def load_and_clean_csv(csv_path):
    df = pandas.read_csv(csv_path)

    # some are clearly bad data, others are not invest models.
    # some are preprocessors and dropped because they are not useful
    # as standalone models and each of their runs should also be represented
    # by a count of the actual model.
    drop_models = [
        'mock.mock',
        'opal.core.gui',
        'OPAL.opal.sediment_sm.gui',
        'OPAL.opal.core.gui',
        'OPAL.opal.carbon_sm.gui',
        'natcap.invest.forage',
        'rangeland_production.forage',
        'execute',
        'testing',
        'natcap.invest.nearshore_wave_and_erosion.nearshore_wave_and_erosion',
        'natcap.invest.xbeach_storm_impact',
        '__main__',
        'natcap.invest.xbeach_storm_surge',
        'model_name',
        'test_ui_inputs',
        'mesh_models.mesh_scenario_generator',
        'N,N',
        'natcap.invest.coastal_blue_carbon.preprocessor',
        'natcap.invest.blue_carbon.blue_carbon_preprocessor',
        'natcap.invest.habitat_risk_assessment.hra_preprocessor',
        'natcap.invest.crop_production.crop_production',
        'natcap.invest.carbon.carbon_combined',
        'natcap.invest.habitat_suitability',
        '\\N'
    ]
    total_rows = len(df)
    df = df[~df['model_name'].isin(drop_models)]
    print(f'dropping {total_rows - len(df)} rows for models we dont care about')

    # Shorten names for convenience & readability
    df['model'] = df['model_name'].apply(lambda x: x.split('.').pop())

    # Reassign some names that seem to have changed over time
    names_map = {
        'recreation': 'recmodel_client',
        'blue_carbon': 'coastal_blue_carbon',
        'coastal_blue_carbon2': 'coastal_blue_carbon',
        'nutrient': 'ndr',
        'delineateit2': 'delineateit',
        'overlap_analysis_mz': 'overlap_analysis',
        'urban_heat_island_mitigation': 'urban_cooling_model',
        'hydropower_water_yield': 'annual_water_yield'
    }

    def reassign_name(name):
        try:
            return(names_map[name])
        except KeyError:
            return name

    df['model'] = df['model'].apply(reassign_name)
    print('remaining model counts:')
    print(df['model'].value_counts())

    # Make a proper datetime datatype
    df['datetime'] = pandas.to_datetime(df['time'], utc=True)
    total_rows = len(df)
    df.dropna(subset=['datetime'], inplace=True)
    print(f'dropping {total_rows - len(df)} rows with missing timestamp')

    return df
