import apache_beam as beam

# We created a pipeline without arguments
with beam.Pipeline() as pipeline:

# Create PCollection with a list
# We use function Map with print and We see the result
    (pipeline | 'Create PCollection' >> beam.Create([[1,10.3,'EUA'],
                                                     [1,10.3,'MEX']])
              | 'Print'              >> beam.Map(print))




# PCollection are Immutable : Apply a transformation on a PCollection it gives us a new PCollection
# Element Type: PCollection must be of any type, but all must be the same type
# Operation Type: it doesn't support grained operations. it means We can't apply transformations on specific element
# Timestamp: Each element in a PColllection contains a timestamp UnBounded: Source assigns timestamp Bounded : Every element is set to same timestamp
