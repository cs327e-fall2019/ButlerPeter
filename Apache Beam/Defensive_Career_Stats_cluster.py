import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from datetime import datetime

# PTransform: change '--' values to '0' and convert from string to int
class formatInt(beam.DoFn):
    def process(self, element):
        # get records from bigquery table that don't need to be changed
        record = element
        player_id = record.get('player_id')
        year = record.get('year')
        sacks = record.get('sacks')
        yards_per_int = record.get('yards_per_int')
        
        # change '--' to '0' for necessary fields
        if (record.get('total_tackles') == '--' or record.get('total_tackles') is None):
            total_tackles = '0'
        else:
            total_tackles = record.get('total_tackles')
            
        if (record.get('solo_tackles') == '--' or record.get('solo_tackles') is None):
            solo_tackles = '0'
        else:
            solo_tackles = record.get('solo_tackles')
            
        if (record.get('assisted_tackles') == '--' or record.get('assisted_tackles') is None):
            assisted_tackles = '0'
        else:
            assisted_tackles = record.get('assisted_tackles')
            
        if (record.get('safties') == '--' or record.get('safties') is None):
            safties = '0'
        else:
            safties = record.get('safties')
            
        if (record.get('passes_defended') == '--' or record.get('passes_defended') is None):
            passes_defended = '0'
        else:
            passes_defended = record.get('passes_defended')
            
        if (record.get('ints') == '--' or record.get('ints') is None):
            ints = '0'
        else:
            ints = record.get('ints')
            
        if (record.get('ints_for_tds') == '--' or record.get('ints_for_tds') is None):
            ints_for_tds = '0'
        else:
            ints_for_tds = record.get('ints_for_tds')
            
        if (record.get('int_yards') == '--' or record.get('int_yards') is None):
            int_yards = '0'
        else:
            int_yards = record.get('int_yards')
            
        if (record.get('longest_int_return') == '--' or record.get('longest_int_return') is None):
            longest_int_return = '0'
        else:
            longest_int_return = record.get('longest_int_return')

             
        # convert necessary fields to int and remove unnecessary characters
        total_tackles = int(total_tackles)
        solo_tackles = int(solo_tackles)
        assisted_tackles = int(assisted_tackles)
        safties = int(safties)
        passes_defended = int(passes_defended)
        ints = int(ints)
        ints_for_tds = int(ints_for_tds)
        int_yards = int(int_yards)
        longest_int_return = longest_int_return.replace('T', '')
        longest_int_return = int(longest_int_return)
        
        entry = {'player_id': player_id,
                 'year': year,
                 'total_tackles': total_tackles,
                 'solo_tackles': solo_tackles,
                 'assisted_tackles': assisted_tackles,
                 'sacks': sacks,
                 'safties': safties,
                 'passes_defended': passes_defended,
                 'ints': ints,
                 'ints_for_tds': ints_for_tds,
                 'int_yards': int_yards,
                 'yards_per_int': yards_per_int,
                 'longest_int_return': longest_int_return
        }
        
        # create key, value pairs
        return [entry]

#PTransform: change '--' to '0' and convert sack field from string to float
class formatFloat(beam.DoFn):
    def process(self, element):
        
        
        record = element
        
        # change '--' to '0' and convert sack field from string to float
        if (record['sacks'] == '--' or record['sacks'] is None):
            record['sacks'] = '0'
            
        record['sacks'] = float(record['sacks'])
        
        return [record]
    
PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'


# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transorm-basic',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4',
    'num_workers': 10
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)
       
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM nfl_stats_modeled.Defensive_Career_Stats'))
    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'input.txt')
    
    # apply ParDo to the PCollection 
    formatedInt_pcoll = query_results | 'convert fields to INTEGER' >> beam.ParDo(formatInt())
    
    # write PCollection to log file
    formatedInt_pcoll | 'Write to log 2' >> WriteToText(DIR_PATH + 'input2.txt')
    
    # apply ParDo to the PCollection 
    output_pcoll = formatedInt_pcoll | 'convert fields to FLOAT' >> beam.ParDo(formatFloat())
    
    # write PCollection to log file
    output_pcoll | 'Write to output log' >> WriteToText(DIR_PATH + 'output.txt')
    
    dataset_id = 'nfl_stats_modeled'
    table_id = 'Defensive_Career_Stats_Beam_DF'
    table_schema = 'player_id:STRING,year:INTEGER,total_tackles:INTEGER,solo_tackles:INTEGER,assisted_tackles:INTEGER,sacks:FLOAT,safties:INTEGER,passes_defended:INTEGER,ints:INTEGER,ints_for_tds:INTEGER,int_yards:INTEGER,yards_per_int:FLOAT,longest_int_return:INTEGER'
    
    output_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(dataset=dataset_id,table=table_id,schema=table_schema,project=PROJECT_ID,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
