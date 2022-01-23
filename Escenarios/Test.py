import apache_beam as beam


lines = [
    "apple orange grape banana apple banana",
    "banana orange banana papaya"
]

with beam.Pipeline() as p:

  (p | beam.Create(lines)

     | beam.FlatMap(lambda sentence: sentence.split())
     | beam.combiners.Count.PerElement()
     | beam.MapTuple(lambda k, v: k + ":" + str(v))
     | beam.Map(print)

  )