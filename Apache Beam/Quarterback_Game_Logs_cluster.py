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
        season = r.get('season')
        week = r.get('week')
        game_date = r.get('game_date')
        home_or_away = r.get('home_or_away')
        opponent = r.get('opponent')
        outcome = r.get('outcome')
        score = r.get('score')
        passer_rating = r.get('passer_rating')
        
        # change '--' values to '0' or '0.0' and convert to int/float
        if (r.get('games_started') == '--'):
            games_started = '0'
        else:
            games_started = r.get('games_started')
            
        if (r.get('passes_completed') == '--'):
            passes_completed = '0'
        else:
            passes_completed = r.get('passes_completed')
            
        if (r.get('passes_attempted') == '--'):
            passes_attempted = '0'
        else:
            passes_attempted = r.get('passes_attempted')
            
        if (r.get('passing_yards') == '--'):
            passing_yards = '0'
        else:
            passing_yards = r.get('passing_yards')
            
        if (r.get('td_passes') == '--'):
            td_passes = '0'
        else:
            td_passes = r.get('td_passes')
            
        if (r.get('ints') == '--'):
            ints = '0'
        else:
            ints = r.get('ints')
            
        if (r.get('sacks') == '--'):
            sacks = '0'
        else:
            sacks = r.get('sacks')
            
        if (r.get('sacked_yards_lost') == '--'):
            sacked_yards_lost = '0'
        else:
            sacked_yards_lost = r.get('sacked_yards_lost')
            
        if (r.get('rushing_yards') == '--'):
            rushing_yards = '0'
        else:
            rushing_yards = r.get('rushing_yards')
            
        if (r.get('rushing_attempts') == '--'):
            rushing_attempts = '0'
        else:
            rushing_attempts = r.get('rushing_attempts')
            
        if (r.get('rushing_tds') == '--'):
            rushing_tds = '0'
        else:
            rushing_tds = r.get('rushing_tds')
            
        if (r.get('fumbles') == '--'):
            fumbles = '0'
        else:
            fumbles = r.get('fumbles')
            
        if (r.get('fumbles_lost') == '--'):
            fumbles_lost = '0'
        else:
            fumbles_lost = r.get('fumbles_lost')
            
        if (r.get('completion_percentage') == '--'):
            completion_percentage = '0'
        else:
            completion_percentage = r.get('completion_percentage')
            
        if (r.get('passing_yards_per_attempt') == '--'):
            passing_yards_per_attempt = '0'
        else:
            passing_yards_per_attempt = r.get('passing_yards_per_attempt')
            
        if (r.get('yards_per_carry') == '--'):
            yards_per_carry = '0'
        else:
            yards_per_carry = r.get('yards_per_carry')
        
        entry = {'player_id': player_id,
                 'year': year,
                 'season': season,
                 'week': week,
                 'game_date': game_date,
                 'home_or_away': home_or_away,
                 'opponent': opponent,
                 'outcome': outcome,
                 'score': score,
                 'games_started': int(games_started),
                 'passes_completed': int(passes_completed),
                 'passes_attempted': int(passes_attempted),
                 'completion_percentage': float(completion_percentage),
                 'passing_yards': int(passing_yards),
                 'passing_yards_per_attempt': float(passing_yards_per_attempt),
                 'td_passes': int(td_passes),
                 'ints': int(ints),
                 'sacks': int(sacks),
                 'sacked_yards_lost': int(sacked_yards_lost),
                 'passer_rating': passer_rating,
                 'rushing_attempts': int(rushing_attempts),
                 'rushing_yards': int(rushing_yards),
                 'yards_per_carry': float(yards_per_carry),
                 'rushing_tds': int(rushing_tds),
                 'fumbles': int(fumbles),
                 'fumbles_lost': int(fumbles_lost)
        }
        return [entry]
    
# PTransform: combine year and game_date fields and format to correct date format
class formatDate(beam.DoFn):
    def process(self, element):
        
        r = element
        
        # add year to game_date and format
        game_date = r['game_date']
        game_date = game_date.replace("/", "-")
        new_game_date = str(r['year']) + "-" + game_date
        r['game_date'] = new_game_date
        
        return [r]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'


# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transorm-qbgl',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4',
    'num_workers': 10
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)
       
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM nfl_stats_modeled.Quarterback_Game_Logs'))
    
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
    table_id = 'Quarterback_Game_Logs_Beam_DF'
    table_schema = 'player_id:STRING,year:INTEGER,season:STRING,week:INTEGER,game_date:DATE,home_or_away:STRING,opponent:STRING,outcome:STRING,score:STRING,games_started:INTEGER,passes_completed:INTEGER,passes_attempted:INTEGER,completion_percentage:FLOAT,passing_yards:INTEGER,passing_yards_per_attempt:FLOAT,td_passes:INTEGER,ints:INTEGER,sacks:INTEGER,sacked_yards_lost:INTEGER,passer_rating:FLOAT,rushing_attempts:INTEGER,rushing_yards:INTEGER,yards_per_carry:FLOAT,rushing_tds:INTEGER,fumbles:INTEGER,fumbles_lost:INTEGER'
    
    output_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(dataset=dataset_id,table=table_id,schema=table_schema,project=PROJECT_ID,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
