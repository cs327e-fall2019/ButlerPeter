import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from datetime import datetime

# PTransform: format for GroupByKey
class FormatRecords(beam.DoFn):
    def process(self, element):
        
        r = element
        
        # get fields from table
        player_id = r.get('player_id')
        year = r.get('year')
        games_played = r.get('games_played')
        
        # return key, value pairs
        return [(player_id, r)]
   

class DedupRecords(beam.DoFn):
    def process(self, element):
        
        player_id, record_obj = element
        record_list = list(record_obj)
        record = record_list[0]
        
        return [record]
    
PROJECT_ID = os.environ['PROJECT_ID']


# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)
       
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM nfl_stats_modeled.Season_Stats LIMIT 100'))

    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')
    
    # apply ParDo to the PCollection 
    formated_pcoll = query_results | 'formating records' >> beam.ParDo(FormatRecords())
    
    # write PCollection to log file
    formated_pcoll | 'Write to log 2' >> WriteToText('input2.txt')
    
    # Group pcollection by key
    groupby_pcoll = formated_pcoll | 'running GroupByKey' >> beam.GroupByKey()
    
    # write PCollection to log file
    groupby_pcoll | 'Write to log 3' >> WriteToText('input3.txt')
    
    # apply ParDo to the PCollection 
    deduped_pcoll = groupby_pcoll | 'removing duplicates' >> beam.ParDo(DedupRecords())
    
    # write PCollection to log file
    deduped_pcoll | 'Write to output log' >> WriteToText('output.txt')
    
    qualified_table_name = PROJECT_ID + ':nfl_stats_modeled.Season_Stats_Beam'
    table_schema = 'player_id:STRING,year:INTEGER,games_played:INTEGER'
    
    deduped_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
