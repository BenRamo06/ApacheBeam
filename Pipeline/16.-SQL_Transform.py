import typing
import apache_beam as beam
import logging
import json
from apache_beam.transforms.sql import SqlTransform


def only_max(element, data):
    if (element['id'],element['process']) in data:
        return element

Purchase = typing.NamedTuple('Purchase',
                             [('id', int)])

with beam.Pipeline() as pipe:

    read_file = (pipe | 'Read file' >> beam.io.ReadFromText(file_pattern = 'inputs/jsonFile*')
                      | 'Load json' >> beam.Map(json.loads))

    max_value = (read_file  | 'Get Key/Value'   >> beam.Map(lambda x: (x['id'],x['process']))
                            | 'Get max process by Key'  >> beam.CombinePerKey(max))

    filter = read_file | 'Only max process' >> beam.Filter(only_max, data = beam.pvalue.AsIter(max_value))

    row_data = filter | beam.Map(lambda x: beam.Row(id = int(x['id'])))
    
    sql_query = row_data  | SqlTransform(""" SELECT id FROM PCOLLECTION WHERE id >= 1""")

    export_data = row_data | beam.io.WriteToText('outputs/ExampleSQL')

    #print_data = sql_query | beam.Map(print)

