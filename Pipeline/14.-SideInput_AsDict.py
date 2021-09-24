import json
import apache_beam as beam


def asign_side(element, side_input):
    
    for i in element['addresses']:
        if i['state'] in side_input:
            i['state'] = side_input[i['state']]
        else:
            i['state'] = 'SIN ESTADO'


    return element




with beam.Pipeline() as pipe:

    # When we use side input it must be a key/value item to process, if not it will give error
    input_side   = pipe     | 'Query Clients' >> beam.Create([['OR','Oregon'],
                                                              ['NY','New York']])

    read_file    = (pipe    | 'Read File' >> beam.io.ReadFromText('inputs/jsonFile')
                            | 'Load Json' >> beam.Map(lambda x: json.loads(x)))

    # The validation always will be in key  
    assign_side = read_file | 'Asign value' >> beam.Map(asign_side, side_input = beam.pvalue.AsDict(input_side))


    print_data = assign_side | 'Print' >> beam.Map(print)