import apache_beam as beam


# Create Pipeline without options
with beam.Pipeline() as pipeline:

# Create PCollection with a list
# We use function Map with print and We see the result
    (pipeline | 'Create PCollection' >> beam.Create([(1,10.3,'EUA'),
                                                     (1,10.3,'MEX')])
              | 'Print'              >> beam.Map(print))

