import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from datetime import datetime

# PTransform: change '--' values to '0' and convert from string to int
class formatInt(beam.DoFn):
    def process(self, element):
        
        record = element

        # get records from bigquery table that don't need to be changed
        player_id = record.get('player_id')
        year = record.get('year')
        completion_percentage = record.get('completion_percentage')
        passing_yards_per_attempt = record.get('passing_yards_per_attempt')
        passing_yards_per_game = record.get('passing_yards_per_game')
        percentage_of_tds_per_attempts = record.get('percentage_of_tds_per_attempts')
        int_rate = record.get('int_rate')
        passer_rating = record.get('passer_rating') 
        pass_attempts_per_game = record.get('pass_attempts_per_game')
        
        # convert necessary fields to INT
        if (record.get('passes_attempted') == '--'):
            passes_attempted = '0'
        else:
            passes_attempted = record.get('passes_attempted')
            
        if (record.get('passes_completed') == '--'):
            passes_completed = '0'
        else:
            passes_completed = record.get('passes_completed')
            
        if (record.get('passing_yards') == '--'):
            passing_yards = '0'
        else:
            passing_yards = record.get('passing_yards')
            
        passing_yards = passing_yards.replace(',', '')
            
        if (record.get('td_passes') == '--'):
            td_passes = '0'
        else:
            td_passes = record.get('td_passes')
            
        if (record.get('ints') == '--'):
            ints = '0'
        else:
            ints = record.get('ints')
            
        if (record.get('longest_pass') == '--'):
            longest_pass = '0'
        else:
            longest_pass = record.get('longest_pass')
        longest_pass = longest_pass.replace('T', '')
        
        if (record.get('passes_longer_than_20_yards') == '--'):
            passes_longer_than_20_yards = '0'
        else:
            passes_longer_than_20_yards = record.get('passes_longer_than_20_yards')
            
        if (record.get('passes_longer_than_40_yards') == '--'):
            passes_longer_than_40_yards = '0'
        else:
            passes_longer_than_40_yards = record.get('passes_longer_than_40_yards')
            
        if (record.get('sacks') == '--'):
            sacks = '0'
        else:
            sacks = record.get('sacks')
        
        if (record.get('sacked_yards_lost') == '--'):
            sacked_yards_lost = '0'
        else:
            sacked_yards_lost = record.get('sacked_yards_lost')
        
        # format for BigQuery
        entry = {'player_id': player_id,
                 'year': year,
                 'passes_attempted': int(passes_attempted),
                 'passes_completed': int(passes_completed),
                 'completion_percentage': completion_percentage,
                 'pass_attempts_per_game': pass_attempts_per_game,
                 'passing_yards': int(passing_yards),
                 'passing_yards_per_attempt': passing_yards_per_attempt,
                 'passing_yards_per_game': passing_yards_per_game,
                 'td_passes': int(td_passes),
                 'percentage_of_tds_per_attempts': percentage_of_tds_per_attempts,
                 'ints': int(ints),
                 'int_rate': int_rate,
                 'longest_pass': int(longest_pass),
                 'passes_longer_than_20_yards': int(passes_longer_than_20_yards),
                 'passes_longer_than_40_yards': int(passes_longer_than_40_yards),
                 'sacks': int(sacks),
                 'sacked_yards_lost': int(sacked_yards_lost),
                 'passer_rating': passer_rating
        }
        
        return [entry]

#PTransform: change '--' to '0' and convert sack field from string to float
class formatFloat(beam.DoFn):
    def process(self, element):
        
        
        r = element
        
        # convert necessary fields to float
        if (r['completion_percentage'] == '--'):
            r['completion_percentage'] = '0.0'
        r['completion_percentage'] = float(r['completion_percentage'])
        
        if (r['passing_yards_per_attempt'] == '--'):
            r['passing_yards_per_attempt'] = '0.0'
        r['passing_yards_per_attempt'] = float(r['passing_yards_per_attempt'])
        
        if (r['passing_yards_per_game'] == '--'):
            r['passing_yards_per_game'] = '0.0'
        r['passing_yards_per_game'] = float(r['passing_yards_per_game'])
        
        if (r['percentage_of_tds_per_attempts'] == '--'):
            r['percentage_of_tds_per_attempts'] = '0.0'
        r['percentage_of_tds_per_attempts'] = float(r['percentage_of_tds_per_attempts'])
        
        if (r['int_rate'] == '--'):
            r['int_rate'] = '0.0'
        r['int_rate'] = float(r['int_rate'])
        
        if (r['passer_rating'] == '--'):
            r['passer_rating'] = '0.0'
        r['passer_rating'] = float(r['passer_rating'])
        
        print()
        print(r)
        print()
        
        return [r]
    
PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'


# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transorm-pcs',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4',
    'num_workers': 10
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)
       
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM nfl_stats_modeled.Passing_Career_Stats'))
    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'input.txt')
    
    # apply ParDo to the PCollection 
    formated_pcoll = query_results | 'formating INTs' >> beam.ParDo(formatInt())
    
    # write PCollection to log file
    formated_pcoll | 'Write to log 2' >> WriteToText(DIR_PATH + 'input2.txt')
    
    # apply ParDo to the PCollection 
    output_pcoll = formated_pcoll | 'formating FLOATs' >> beam.ParDo(formatFloat())
    
    # write PCollection to log file
    output_pcoll | 'Write to output log' >> WriteToText(DIR_PATH + 'output.txt')
    
    dataset_id = 'nfl_stats_modeled'
    table_id = 'Passing_Career_Stats_Beam_DF'
    table_schema = 'player_id:STRING,year:INTEGER,passes_attempted:INTEGER,passes_completed:INTEGER,completion_percentage:FLOAT,pass_attempts_per_game:FLOAT,passing_yards:INTEGER,passing_yards_per_attempt:FLOAT,passing_yards_per_game:FLOAT,td_passes:INTEGER,percentage_of_tds_per_attempts:FLOAT,ints:INTEGER,int_rate:FLOAT,longest_pass:INTEGER,passes_longer_than_20_yards:INTEGER,passes_longer_than_40_yards:INTEGER,sacks:INTEGER,sacked_yards_lost:INTEGER,passer_rating:FLOAT'
    
    output_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(dataset=dataset_id,table=table_id,schema=table_schema,project=PROJECT_ID,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
