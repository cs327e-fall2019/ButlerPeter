import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from datetime import datetime

# PTransform: replace '--' values with '0' and convert to appropriate type (Int/Float)
class formatInt(beam.DoFn):
    def process(self, element):
        
        r = element
        
        # get records from bigquery table that don't need to be changed
        player_id = r.get('player_id')
        year = r.get('year')
        position = r.get('position')
        rushing_attempts_per_game = r.get('rushing_attempts_per_game')
        yards_per_carry = r.get('yards_per_carry')
        rushing_yards_per_game = r.get('rushing_yards_per_game')
        percentage_of_rushing_first_downs = r.get('percentage_of_rushing_first_downs')
        
        # change '--' values to '0' and convert to int
        if (r.get('rushing_attempts') == '--'):
            rushing_attempts = '0'
        else:
            rushing_attempts = r.get('rushing_attempts')
            
        if (r.get('rushing_yards') == '--'):
            rushing_yards = '0'
        else:
            rushing_yards = r.get('rushing_yards')
            
        if (r.get('rushing_tds') == '--'):
            rushing_tds = '0'
        else:
            rushing_tds = r.get('rushing_tds')
            
        if (r.get('longest_run') == '--'):
            longest_run = '0'
        else:
            longest_run = r.get('longest_run')
            
        if (r.get('rushing_first_downs') == '--'):
            rushing_first_downs = '0'
        else:
            rushing_first_downs = r.get('rushing_first_downs')
            
        if (r.get('rushing_more_than_20_yards') == '--'):
            rushing_more_than_20_yards = '0'
        else:
            rushing_more_than_20_yards = r.get('rushing_more_than_20_yards')
        
        if (r.get('rushing_more_than_40_yards') == '--'):
            rushing_more_than_40_yards = '0'
        else:
            rushing_more_than_40_yards = r.get('rushing_more_than_40_yards')
            
        if (r.get('fumbles') == '--'):
            fumbles = '0'
        else:
            fumbles = r.get('fumbles')
            
        longest_run = longest_run.replace('T', '')
        rushing_yards = rushing_yards.replace(',', '')
        
        entry = {'player_id': player_id,
                 'year': year,
                 'position': position,
                 'rushing_attempts': int(rushing_attempts),
                 'rushing_attempts_per_game': rushing_attempts_per_game,
                 'rushing_yards': rushing_yards,
                 'yards_per_carry': yards_per_carry,
                 'rushing_yards_per_game': rushing_yards_per_game,
                 'rushing_tds': int(rushing_tds),
                 'longest_run': int(longest_run),
                 'rushing_first_downs': int(rushing_first_downs),
                 'percentage_of_rushing_first_downs': percentage_of_rushing_first_downs,
                 'rushing_more_than_20_yards': int(rushing_more_than_20_yards),
                 'rushing_more_than_40_yards': int(rushing_more_than_40_yards),
                 'fumbles': int(fumbles)
        }
        return [entry]
    
# PTransform: combine year and game_date fields and format to correct date format
class formatFloat(beam.DoFn):
    def process(self, element):
        
        r = element
        
        # change from string to float
        if (r['yards_per_carry'] == '--'):
            r['yards_per_carry'] = '0.0'
            
        r['yards_per_carry'] = float(r['yards_per_carry'])
        
        if (r['rushing_yards_per_game'] == '--'):
            r['rushing_yards_per_game'] = '0.0'
            
        r['rushing_yards_per_game'] = float(r['rushing_yards_per_game'])
        
        if (r['percentage_of_rushing_first_downs'] == '--'):
            r['percentage_of_rushing_first_downs'] = '0.0'
            
        r['percentage_of_rushing_first_downs'] = float(r['percentage_of_rushing_first_downs'])
        
        
        return [r]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'


# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transorm-rbcs',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4',
    'num_workers': 10
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)
       
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM nfl_stats_modeled.Rushing_Career_Stats'))
    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'input.txt')
    
    # apply ParDo to the PCollection 
    formated_pcoll = query_results | 'formating ints' >> beam.ParDo(formatInt())
    
    # write PCollection to log file
    formated_pcoll | 'Write to log 2' >> WriteToText(DIR_PATH + 'input2.txt')
    
    # apply ParDo to the PCollection 
    output_pcoll = formated_pcoll | 'formating floats' >> beam.ParDo(formatFloat())
    
    # write PCollection to log file
    output_pcoll | 'Write to output log' >> WriteToText(DIR_PATH + 'output.txt')
    
    dataset_id = 'nfl_stats_modeled'
    table_id = 'Rushing_Career_Stats_Beam_DF'
    table_schema = 'player_id:STRING,year:INTEGER,position:STRING,rushing_attempts:INTEGER,rushing_attempts_per_game:FLOAT,rushing_yards:INTEGER,yards_per_carry:FLOAT,rushing_yards_per_game:FLOAT,rushing_tds:INTEGER,longest_run:INTEGER,rushing_first_downs:INTEGER,percentage_of_rushing_first_downs:FLOAT,rushing_more_than_20_yards:INTEGER,rushing_more_than_40_yards:INTEGER,fumbles:INTEGER'
    
    output_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(dataset=dataset_id,table=table_id,schema=table_schema,project=PROJECT_ID,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
