import apache_beam as beam

# We created a pipeline without arguments
with beam.Pipeline() as pipeline:
    
    # We create a branch called "collection" that contains the creation of a PCollection
    collection = (pipeline   | 'Create PCollection' >> beam.Create([(1,10.3,'EUA'),
                                                                    (1,10.3,'MEX')]))

    # This branch "collection" is now a PCollecion Source for our next transformation "print data"
    print_data = (collection | 'Print'  >> beam.Map(print))
