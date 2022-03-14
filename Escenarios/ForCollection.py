import argparse
import apache_beam as beam

from sys import argv
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions



parser = argparse.ArgumentParser()

parser.add_argument('--input_files',
                    required = False,
                    default = '',
                    help = 'Path files to process')

parser.add_argument('--table_name',
                    required = False,
                    default = '',
                    help = 'Table name')


parser.add_argument('--field_name',
                    required = False,
                    default = '',
                    help = 'field name in query')


parser.add_argument('--db_dev',
                    required = False,
                    default = '',
                    help = 'database development')


parser.add_argument('--db_prod',
                    required = False,
                    default = '',
                    help = 'database production')


parser.add_argument('--table_schema',
                    required = False,
                    default = '',
                    help = 'schema table to create table')


args_cmd, args_beam = parser.parse_known_args()



# Function rows valids and fails

class extract_data(beam.DoFn):
    def process(self, element):

        try:    
            yield beam.pvalue.TaggedOutput('valids', [datetime.strptime(element[0],'%Y-%m-%d').strftime('%Y-%m-%d'), int(element[1])])
        except:
            yield beam.pvalue.TaggedOutput('fails', ', '.join(element))


# Function to create groups

class createGroups(beam.DoFn):

    suma = 0
    group = 0

    def process (self, element):
        
        if self.suma + element[1] > 200000000:
            self.group += 1
            self.suma  = 0 
            element.append(self.group)
        else:
            element.append(self.group)

        self.suma += element[1]

        yield (element[2], element[0])



# Function to generate Querys

class generateQuery(beam.DoFn):
    def __init__(self, table, field, schema, db_dev, db_prod):
        self.table   = table
        self.field   = field
        self.schema  = schema
        self.db_dev  = db_dev
        self.db_prod = db_prod


    def process(self, element):
        
        query_creation = 'CREATE TABLE {}.{} ( {} ) ;'.format(self.db_dev,
                                                              self.table + '_' + str(element[0]), 
                                                              self.schema)


        query_dates = 'INSERT INTO {}.{} SELECT * FROM {}.{} WHERE {} in ({});'.format(self.db_dev, 
                                                                                       self.table + '_' + str(element[0]), 
                                                                                       self.db_prod,
                                                                                       self.table, 
                                                                                       self.field, 
                                                                                       ', '.join("'{}'".format(value) for value in element[1]))

        yield query_creation + '\n' + query_dates
        


# Declaration of Pipeline

with beam.Pipeline(options = PipelineOptions(args_beam)) as pipeline:
    
    read_file =  pipeline  | "Read file" >> beam.io.ReadFromText(args_cmd.input_files)

    divide = read_file     | "Divide file" >> beam.Map(lambda x: x.split("\t"))
                    
    valids, fails = divide | 'Clean data' >> beam.ParDo(extract_data()).with_outputs('valids', 'fails')

    export_fails = fails   | 'Export Data Fails' >> beam.io.WriteToText(file_path_prefix = 'outputs/historic_fails', 
                                                                        file_name_suffix = '.json')

    groups = valids        | 'Create Groups' >> beam.ParDo(createGroups())

    keyGroup = groups      | 'Generate distinct groups' >> beam.GroupByKey()

    query_dates = keyGroup | 'Create Query' >> beam.ParDo(generateQuery(table   = args_cmd.table_name, 
                                                                        field   = args_cmd.field_name, 
                                                                        schema  = args_cmd.table_schema, 
                                                                        db_dev  = args_cmd.db_dev, 
                                                                        db_prod = args_cmd.db_prod))

    export_valids = query_dates | 'Export Data Valid' >> beam.io.WriteToText(file_path_prefix = 'outputs/valids_querys', 
                                                                             file_name_suffix = '.json')

    print_data = query_dates    | 'Print' >> beam.Map(print)




# Execution 

# python3 Transformations/historic.py \
# --input_files "inputs/historic.txt" \
# --db_desa "WHOWNER_TBL_DESA" \    
# --db_prod "DW_WHOWNER" \    
# --table_name "LK_CX_CUS_FISCAL_DATA" \
# --field_name "AUD_UPD_DT" \
# --table_schema "id int" \
# --runner DirectRunner

