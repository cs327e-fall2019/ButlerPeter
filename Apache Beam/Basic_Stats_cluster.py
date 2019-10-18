import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from datetime import datetime

# PTransform: change '--' values to '0' and convert from string to int
class formatString(beam.DoFn):
    def process(self, element):
        
        record = element

        # get records from bigquery table that don't need to be changed
        age = record.get('age')
        birth_place = record.get('birth_place')
        birthday = record.get('birthday')
        college = record.get('college')
        current_status = record.get('current_status')
        current_team = record.get('current_team')
        height__inches = record.get('height__inches')
        high_school = record.get('high_school')
        high_school_location = record.get('high_school_location')
        name = record.get('name')
        number = record.get('number')
        player_id = record.get('player_id')
        position = record.get('position')
        weight__lbs = record.get('weight__lbs')
        years_played = record.get('years_played')
        
        # format experience field
        if (record.get('experience') is not None):
            ch_list = ['st', 'nd', 'rd', 'th']
            experience = record.get('experience').split(' ')
            for ch in ch_list:
                experience = experience[0].replace(ch, '') + ' Season(s)'
                
        else:
            experience = record.get('experience')

        # break up years_played field into year_started and year_ended and convert to INTEGER
        if (record.get('years_played') is not None):
            years_played = record.get('years_played').split(' - ')
            
            year_started = int(years_played[0])
            year_ended = int(years_played[1])
                
        else:
            year_started = record.get('years_played')
            year_ended = record.get('years_played')
        
        years_played = ''
        
        # format for BigQuery
        entry = {'age': age,
                 'birth_place': birth_place,
                 'birthday': birthday,
                 'college': college,
                 'current_status': current_status,
                 'current_team': current_team,
                 'experience': experience,
                 'height__inches': height__inches,
                 'high_school': high_school,
                 'high_school_location': high_school_location,
                 'name': name,
                 'number': number,
                 'player_id': player_id,
                 'position': position,
                 'weight__lbs': weight__lbs,
                 'years_played': years_played,
                 'year_started': year_started,
                 'year_ended': year_ended
        }
        
        return [entry]

#PTransform: change '--' to '0' and convert sack field from string to float
class formatDate(beam.DoFn):
    def process(self, element):
        
        
        record = element
        
        # convert birthday from STRING to DATE
        if (record['birthday'] is not None):
            birthday = record['birthday'].split('/')
            year = birthday[2]
            month = birthday[0]
            if (len(month) == 1):
                month = '0' + month
            day = birthday[1]
            birthday = year + '-' + month + '-' + day

            record['birthday'] = birthday
            
            if (len(year) != 4 or len(day) < 1):
                record['birthday'] = None
        
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
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM nfl_stats_modeled.Basic_Stats'))
    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'query_pcoll.txt')
    
    # apply ParDo to the PCollection 
    formatedString_pcoll = query_results | 'formating strings' >> beam.ParDo(formatString())
    
    # write PCollection to log file
    formatedString_pcoll | 'Write to log 2' >> WriteToText(DIR_PATH + 'formated_pcoll.txt')
    
    # apply ParDo to the PCollection 
    output_pcoll = formatedString_pcoll | 'formating birthday' >> beam.ParDo(formatDate())
    
    # write PCollection to log file
    output_pcoll | 'Write to output log' >> WriteToText(DIR_PATH + 'output.txt')
    
    dataset_id = 'nfl_stats_modeled'
    table_id = 'Basic_Stats_Beam_DF'
    table_schema = 'age:INTEGER,birth_place:STRING,birthday:DATE,college:STRING,current_status:STRING,current_team:STRING,experience:STRING,height__inches:INTEGER,high_school:STRING,high_school_location:STRING,name:STRING,number:INTEGER,player_id:STRING,position:STRING,weight__lbs:INTEGER,years_played:STRING,year_started:INTEGER,year_ended:INTEGER'
    
    output_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(dataset=dataset_id,table=table_id,schema=table_schema,project=PROJECT_ID,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))