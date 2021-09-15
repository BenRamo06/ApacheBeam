import apache_beam as beam

def valid_amount(row, max):
    amount = float(row[3])
    if amount >= max:
        return row
    

with beam.Pipeline() as pipeline:

    read_file  = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                           | 'Split' >> beam.Map(lambda x: x.split(',')))

    # We use 
    valid_rows = read_file | 'Amount grather than 0' >> beam.Filter(lambda x: float(x[3]) > 0)
    
    
    #
    valid_rows = read_file | 'Amount grather than 0' >> beam.Filter(valid_amount, max = 1469)

    print_data = valid_rows | 'Print' >> beam.Map(print)
    