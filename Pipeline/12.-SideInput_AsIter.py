import json
import apache_beam as beam


# We create function to recognize id that not exists in side input
def get_sideinput(element, side, fields):
    if int(element['id']) not in side:
        yield dict((f,element[f]) for f in fields)


with beam.Pipeline() as pipe:

    input_side   = pipe     | 'Query Clients' >> beam.Create([1,7])

    read_file    = (pipe    | 'Read File' >> beam.io.ReadFromText('inputs/jsonFile')
                            | 'Load Json' >> beam.Map(lambda x: json.loads(x)))

    # Use data of side input as iter (list in this case)
    add_side     = read_file | 'Add fields Side Input' >> beam.ParDo(get_sideinput, 
                                                                     side = beam.pvalue.AsList(input_side), 
                                                                     fields = ['id', 'first_name', 'last_name'])

    print_data = add_side | beam.Map(print)


