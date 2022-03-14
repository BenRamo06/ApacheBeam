from asyncore import read
import apache_beam as beam
import json


with beam.Pipeline() as p:

    
    read_file = p | 'Read File' >> beam.io.ReadFromText('inputs/jsonFile')

    cast_json = read_file | 'Cast to JSON' >> beam.Map(json.loads)

    create_struct = cast_json | 'Create Structure' >> beam.Map(lambda x: (x['id'], x['salary']))

    group = create_struct | 'Create groups' >> beam.GroupByKey() 

    crate_struct = group | 'Print' >> beam.Map(print)
    