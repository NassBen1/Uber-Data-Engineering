#Fichier de trnasformation des données uber et transformation en dict pour le chargement dans BigQuery par rapport au fichier Traitement fichier Uber.ipynb


import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    #Faire passer les dates en datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    #Créer un dataframe datetime avec les colonnes tpep_pickup_datetime et tpep_dropoff_datetime
    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim['pick_hour']=datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day']=datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month']=datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year']=datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['weekday']=datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['drop_hour']=datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day']=datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month']=datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year']=datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday']=datetime_dim['tpep_dropoff_datetime'].dt.weekday
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'weekday', 'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    #Creer un dataframe passenger_count_dim avec la colonne passenger_count
    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index

    #Creer un dataframe trip_distance_dim avec la colonne trip_distance
    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index

    rate_code_type = {1: 'Standard rate',
                  2: 'JFK',
                  3: 'Newark',
                  4: 'Nassau or Westchester',
                  5: 'Negotiated fare',
                  6: 'Group ride'}

    #Creer un dataframe rate_code_dim avec la colonne RateCodeID
    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

    #Créer la table pickup_location_dim avec les colonnes pickup_location_id, pickup_latitude, pickup_longitude
    pickup_location_dim = df[['pickup_latitude', 'pickup_longitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_latitude', 'pickup_longitude']]

    #Créer la table dropoff_location_dim avec les colonnes dropoff_location_id, dropoff_latitude, dropoff_longitude
    dropoff_location_dim = df[['dropoff_latitude', 'dropoff_longitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude']]

    #Créer le dictionnaire payment_type_name
    payment_type_name = {1: 'Credit card',
                        2: 'Cash',
                        3: 'No charge',
                        4: 'Dispute',
                        5: 'Unknown',
                        6: 'Voided trip'}

    #Créer la table payment_type_dim avec les colonnes payment_type_id, payment_type et le payment_type_name
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

    #Traitement de VendorID
    vendor_name = {1: 'Creative Mobile Technologies, LLC',
                2: 'VeriFone Inc.'}

    vendor_dim = df[['VendorID']].drop_duplicates().reset_index(drop=True)
    vendor_dim['vendor_id'] = vendor_dim.index
    vendor_dim['vendor_name'] = vendor_dim['VendorID'].map(vendor_name)
    vendor_dim = vendor_dim[['vendor_id', 'VendorID', 'vendor_name']]

    #Création fact_table
    fact_table=df.merge(passenger_count_dim, on='passenger_count') \
    .merge(trip_distance_dim, on='trip_distance') \
    .merge(vendor_dim, on='VendorID') \
    .merge(rate_code_dim, on='RatecodeID') \
    .merge(pickup_location_dim, on=['pickup_latitude', 'pickup_longitude']) \
    .merge(dropoff_location_dim, on=['dropoff_latitude', 'dropoff_longitude']) \
    .merge(payment_type_dim, on='payment_type') \
    .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime']) \
    [['VendorID', 'passenger_count_id', 'trip_distance_id', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id', 'datetime_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']]

    print('Transformation réussie')

    return {"datetime_dim": datetime_dim.to_dict(orient='dict'),
            "passenger_count_dim": passenger_count_dim.to_dict(orient='dict'),
            "trip_distance_dim": trip_distance_dim.to_dict(orient='dict'),
            "rate_code_dim": rate_code_dim.to_dict(orient='dict'),
            "pickup_location_dim": pickup_location_dim.to_dict(orient='dict'),
            "dropoff_location_dim": dropoff_location_dim.to_dict(orient='dict'),
            "payment_type_dim": payment_type_dim.to_dict(orient='dict'),
            "vendor_dim": vendor_dim.to_dict(orient='dict'),
            "fact_table": fact_table.to_dict(orient='dict')}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
