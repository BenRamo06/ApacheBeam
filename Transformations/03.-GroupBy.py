from posixpath import join
import apache_beam as beam
import typing
import logging
import json


def printer(element):
    print(type(element))

with beam.Pipeline() as pipe:

    read_file = (pipe | 'Read file' >> beam.io.ReadFromText(file_pattern = 'inputs/jsonFile*')
                      | 'Load json' >> beam.Map(json.loads))

    grouped = read_file | beam.GroupBy(lambda x: (x['id'], x['process'])).aggregate_field(lambda x: x['salary'], max, 'last_salary')

    grouped_def = grouped | beam.Map(printer) 


    print_data = grouped | beam.Map(print)