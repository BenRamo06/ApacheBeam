import apache_beam as beam



with beam.Pipeline() as p:

    saijayins = p | 'Create Collection SY' >> beam.Create([['Goku','Vegeta']])

    hybrids = p | 'Create Collection HD' >> beam.Create([['Gohan','Trunks']])

    order_saijayins = saijayins | 'S' >> beam.FlatMap(lambda x: x)

    order_hybrids = hybrids | 'H' >> beam.FlatMap(lambda x: x)

    join = (order_saijayins, order_hybrids) | 'Flatten' >> beam.Flatten()

    print_data = test | 'Print' >> beam.Map(print)