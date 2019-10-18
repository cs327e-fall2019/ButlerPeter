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
        yards_per_game = r.get('yards_per_game')
        yards_per_reception = r.get('yards_per_reception')
        
        # change '--' values to '0' and convert to int
        if (r.get('receptions') == '--'):
            receptions = '0'
        else:
            receptions = r.get('receptions')
            
        if (r.get('receiving_yards') == '--'):
            receiving_yards = '0'
        else:
            receiving_yards = r.get('receiving_yards')
            
        if (r.get('longest_reception') == '--'):
            longest_reception = '0'
        else:
            longest_reception = r.get('longest_reception')
            
        if (r.get('receiving_tds') == '--'):
            receiving_tds = '0'
        else:
            receiving_tds = r.get('receiving_tds')
            
        if (r.get('receptions_longer_than_20_yards') == '--'):
            receptions_longer_than_20_yards = '0'
        else:
            receptions_longer_than_20_yards = r.get('receptions_longer_than_20_yards')
            
        if (r.get('receptions_longer_than_40_yards') == '--'):
            receptions_longer_than_40_yards = '0'
        else:
            receptions_longer_than_40_yards = r.get('receptions_longer_than_40_yards')
            
        if (r.get('first_down_receptions') == '--'):
            first_down_receptions = '0'
        else:
            first_down_receptions = r.get('first_down_receptions')
            
        if (r.get('fumbles') == '--'):
            fumbles = '0'
        else:
            fumbles = r.get('fumbles')
            
        longest_reception = longest_reception.replace('T', '')
        receiving_yards = receiving_yards.replace(',', '')
        
        entry = {'player_id': player_id,
                 'year': year,
                 'receptions': int(receptions),
                 'receiving_yards': int(receiving_yards),
                 'yards_per_reception': yards_per_reception,
                 'yards_per_game': yards_per_game,
                 'longest_reception': int(longest_reception),
                 'receiving_tds': int(receiving_tds),
                 'receptions_longer_than_20_yards': int(receptions_longer_than_20_yards),
                 'receptions_longer_than_40_yards': int(receptions_longer_than_40_yards),
                 'first_down_receptions': int(first_down_receptions),
                 'fumbles': int(fumbles)
        }
        return [entry]
    
# PTransform: combine year and game_date fields and format to correct date format
class formatFloat(beam.DoFn):
    def process(self, element):
        
        r = element
        
        # change from string to float
        if (r['yards_per_reception'] == '--'):
            r['yards_per_reception'] = '0.0'
            
        r['yards_per_reception'] = float(r['yards_per_reception'])
        
        
        return [r]

PROJECT_ID = os.environ['PROJECT_ID']


# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)
       
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM nfl_stats_modeled.Receiving_Career_Stats LIMIT 100'))
    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')
    
    # apply ParDo to the PCollection 
    formated_pcoll = query_results | 'formating ints' >> beam.ParDo(formatInt())
    
    # write PCollection to log file
    formated_pcoll | 'Write to log 2' >> WriteToText('input2.txt')
    
    # apply ParDo to the PCollection 
    output_pcoll = formated_pcoll | 'formating floats' >> beam.ParDo(formatFloat())
    
    # write PCollection to log file
    output_pcoll | 'Write to output log' >> WriteToText('output.txt')
    
    qualified_table_name = PROJECT_ID + ':nfl_stats_modeled.Receiving_Career_Stats_Beam'
    table_schema = 'player_id:STRING,year:INTEGER,season:STRING,receptions:INTEGER,receiving_yards:INTEGER,yards_per_reception:FLOAT,yards_per_game:FLOAT,longest_reception:INTEGER,receiving_tds:INTEGER,receptions_longer_than_20_yards:INTEGER,receptions_longer_than_40_yards:INTEGER,first_down_receptions:INTEGER,fumbles:INTEGER'
    
    output_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
