import os
import datetime as datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

PROJECT_ID = 'airy-lodge-252816'
BUCKET = 'gs://butlerpeter-beam'

# PTransform: split stadium_location into city and state

DIR_PATH_OUT = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
class formatString(beam.DoFn):
    def process(self, element):
        
        record = element

        # get records from bigquery table that don't need to be changed
        stadium_name = record.get('stadium_name')
        stadium_open = record.get('stadium_open')
        stadium_close = record.get('stadium_close')
        stadium_type = record.get('stadium_type')
        stadium_address = record.get('stadium_address')
        stadium_weather_station_code = record.get('stadium_weather_station_code')
        stadium_weather_type = record.get('stadium_weather_type')
        stadium_capacity = record.get('stadium_capacity')
        stadium_surface = record.get('stadium_surface')
        station_id = record.get('STATION')
        station_name = record.get('NAME')
        latitude = record.get('LATITUDE')
        longitude = record.get('LONGITUDE')
        elevation = record.get('ELEVATION')


        # break up stadium_location field into city and state
        if (record.get('stadium_location') is not None):
            stadium_location = record.get('stadium_location').split(',')
            
            stadium_city = stadium_location[0]
            stadium_state = stadium_location[1]
                
        else:
            stadium_city = stadium_location
            stadium_state = stadium_location
        
        # format for BigQuery
        
        entry = {
        
        'stadium_name' : stadium_name,
        'stadium_city' : stadium_city,
        'stadium_state' : stadium_state,
        'stadium_open' : stadium_open,
        'stadium_close' : stadium_close,
        'stadium_type' : stadium_type,
        'stadium_address' : stadium_address,
        'stadium_weather_station_code' : stadium_weather_station_code,
        'stadium_weather_type' : stadium_weather_type,
        'stadium_capacity' : stadium_capacity,
        'stadium_surface' : stadium_surface,
        'station_id' : station_id,
        'station_name' : station_name,
        'latitude' : latitude,
        'longitude' : longitude,
        'elevation' : elevation,
        }
        
        print(entry)
        return [entry]

    
PROJECT_ID = 'airy-lodge-252816'


# Project ID is needed for BigQuery data source, even for local execution.
options = {
       'project': PROJECT_ID,
                             }

opts = beam.pipeline.PipelineOptions(flags=[], **options)
       
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM   nfl_weather_workflow_modeled.nfl_stadiums LIMIT 100'))
    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH_OUT + 'query_pcoll.txt')
    
    # apply ParDo to the PCollection 
    formatedString_pcoll = query_results | 'formating strings' >> beam.ParDo(formatString())
    
    # write PCollection to log file
    formatedString_pcoll | 'Write to log 2' >> WriteToText(DIR_PATH_OUT + 'formated_pcoll.txt')
    
    
    dataset_id = 'nfl_weather_workflow_modeled'
    table_id = 'nfl_stadiums_Beam'
    table_schema = 'stadium_name:STRING,stadium_city:STRING,stadium_state:STRING,stadium_open:INTEGER,stadium_close:INTEGER,stadium_type:STRING,stadium_address:STRING,stadium_weather_station_code:STRING,stadium_weather_type:STRING,stadium_capacity:INTEGER,stadium_surface:STRING,station_id:STRING,station_name:STRING,latitude:FLOAT,longitude:FLOAT,elevation:FLOAT'
    
    formatedString_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(dataset=dataset_id,table=table_id,schema=table_schema,project=PROJECT_ID,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))