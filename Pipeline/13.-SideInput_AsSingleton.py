import json
import apache_beam as beam

def get_sideinput(element, sideSingleton):
    if float(element['salary']) >= sideSingleton:
        return element


with beam.Pipeline() as pipe:

    input_side   = pipe     | 'Query Clients' >> beam.Create([6000])

    read_file    = (pipe    | 'Read File' >> beam.io.ReadFromText('inputs/jsonFile')
                            | 'Load Json' >> beam.Map(lambda x: json.loads(x)))

    # Use data of side input as Singleton with Filter function
    #filter_side  = read_file | 'Filter with Singleton' >> beam.Filter(lambda x, amount: float(x['salary']) >= amount, amount = beam.pvalue.AsSingleton(input_side))
    
    # Use data of side input as Singleton with DoFn
    filter_side   = read_file | 'Filter with Singleton' >> beam.Map(get_sideinput,sideSingleton = beam.pvalue.AsSingleton(input_side))


    print_data = filter_side | beam.Map(print)
