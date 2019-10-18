import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from datetime import datetime

# PTransform: replace '--' values with null and convert to appropriate type (Int/Float)
class formatInt(beam.DoFn):
    def process(self, element):
        # get records from bigquery table
        record = element
        player_id = record.get('player_id')
        season = record.get('season')
        week = record.get('week')
        home_or_away = record.get('home_or_away')
        opponent = record.get('opponent')
        outcome = record.get('outcome')
        score = record.get('score')
        year = record.get('year')
        game_date = record.get('game_date')
        
        # change '--' values to '0' or '0.0' and convert to int/float
        if (record.get('games_started') == '--'):
            games_started = '0'
        else:
            games_started = record.get('games_started')
        games_started = int(games_started)
        
        if (record.get('rushing_attempts') == '--'):
            rushing_attempts = '0'
        else:
            rushing_attempts = record.get('rushing_attempts')
        rushing_attempts = int(rushing_attempts)
        
        if (record.get('rushing_yards') == '--'):
            rushing_yards = '0'
        else:
            rushing_yards = record.get('rushing_yards')
        rushing_yards = int(rushing_yards)   
        
        if (record.get('yards_per_carry') == '--'):
            yards_per_carry = '0.0'
        else:
            yards_per_carry = record.get('yards_per_carry')
        yards_per_carry = float(yards_per_carry)
        
        if (record.get('longest_run') == '--'):
            longest_run = '0'
        else:
            longest_run = record.get('longest_run')
        longest_run = longest_run.replace('T', '')
        longest_run = int(longest_run) 
        
        if (record.get('rushing_tds') == '--'):
            rushing_tds = '0'
        else:
            rushing_tds = record.get('rushing_tds')
        rushing_tds = int(rushing_tds)    
        
        if (record.get('receptions') == '--'):
            receptions = '0'
        else:
            receptions = record.get('receptions')
        receptions = int(receptions)    
        
        if (record.get('receiving_yards') == '--'):
            receiving_yards = '0'
        else:
            receiving_yards = record.get('receiving_yards')
        receiving_yards = int(receiving_yards)    
        
        if (record.get('yards_per_reception') == '--'):
            yards_per_reception = '0.0'
        else:
            yards_per_reception = record.get('yards_per_reception')
        yards_per_reception = float(yards_per_reception)    
        
        if (record.get('longest_reception') == '--'):
            longest_reception = '0'
        else:
            longest_reception = record.get('longest_reception')
        longest_reception = longest_reception.replace('T', '')
        longest_reception = int(longest_reception)    
        
        if (record.get('receiving_tds') == '--'):
            receiving_tds = '0'
        else:
            receiving_tds = record.get('receiving_tds')
        receiving_tds = int(receiving_tds)    
        
        if (record.get('fumbles') == '--'):
            fumbles = '0'
        else:
            fumbles = record.get('fumbles')
        fumbles = int(fumbles)    
        
        if (record.get('fumbles_lost') == '--'):
            fumbles_lost = '0'
        else:
            fumbles_lost = record.get('fumbles_lost')
        fumbles_lost = int(fumbles_lost)    
        
        entry = {'player_id': player_id,
                 'year': year,
                 'season': season,
                 'week': week,
                 'game_date': game_date,
                 'home_or_away': home_or_away,
                 'opponent': opponent,
                 'outcome': outcome,
                 'score': score,
                 'games_started': games_started,
                 'rushing_attempts': rushing_attempts,
                 'rushing_yards': rushing_yards,
                 'yards_per_carry': yards_per_carry,
                 'longest_run': longest_run,
                 'rushing_tds': rushing_tds,
                 'receptions': receptions,
                 'receiving_yards': receiving_yards,
                 'yards_per_reception': yards_per_reception,
                 'longest_reception': longest_reception,
                 'receiving_tds': receiving_tds,
                 'fumbles': fumbles,
                 'fumbles_lost': fumbles_lost
        }
        return [entry]
    
# PTransform: combine year and game_date fields and format to correct date format
class formatDate(beam.DoFn):
    def process(self, element):
        
        record = element
        
        # add year to game_date and format
        game_date = record['game_date']
        game_date = game_date.replace("/", "-")
        new_game_date = str(record['year']) + "-" + game_date
        record['game_date'] = new_game_date
        
        return [record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'


# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transformrbgl',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4',
    'num_workers': 10
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)
       
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM nfl_stats_modeled.Runningback_Game_Logs'))
    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'input.txt')
    
    # apply ParDo to the PCollection 
    formatedInt_pcoll = query_results | 'Format Ints/Floats' >> beam.ParDo(formatInt())
    
    # write PCollection to log file
    formatedInt_pcoll | 'Write to log 2' >> WriteToText(DIR_PATH + 'input2.txt')
    
    # apply ParDo to the PCollection 
    output_pcoll = formatedInt_pcoll | 'Add year to game_date and convert to DATE' >> beam.ParDo(formatDate())
    
    # write PCollection to log file
    output_pcoll | 'Write to output log' >> WriteToText(DIR_PATH + 'output.txt')
    
    dataset_id = 'nfl_stats_modeled'
    table_id = 'Runningback_Game_Logs_Beam_DF'
    table_schema = 'player_id:STRING,year:INTEGER,season:STRING,week:INTEGER,game_date:DATE,home_or_away:STRING,opponent:STRING,outcome:STRING,score:STRING,games_started:INTEGER,rushing_attempts:INTEGER,rushing_yards:INTEGER,yards_per_carry:FLOAT,longest_run:INTEGER,rushing_tds:INTEGER,receptions:INTEGER,receiving_yards:INTEGER,yards_per_reception:FLOAT,longest_reception:INTEGER,receiving_tds:INTEGER,fumbles:INTEGER,fumbles_lost:INTEGER'
    
    output_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(dataset=dataset_id,table=table_id,schema=table_schema,project=PROJECT_ID,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
