import apache_beam as beam


with beam.Pipeline() as pipeline:
    
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                          | 'Divide' >> beam.Map(lambda x: (x.split(',')[0],float(x.split(',')[2])))
                          | 'Count' >> beam.CombinePerKey(sum)
                )
                          

    print_data = read_file | 'Print' >> beam.Map(print)