import apache_beam as beam
from datetime import date,datetime


class Filter(beam.DoFn):
    
    def __init__(self, fields):
        self.fields = fields    

    def process(self, element):        
        row = element.split(',')

        for i in self.fields:
            try:
               row[i[0]] = (datetime.strptime(row[i[0]],i[1]).strftime('%Y-%m-%d'))
            except:
               row[i[0]] = ''

        yield row
    

with beam.Pipeline() as pipeline:

    read_file  =   pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)

    valid_rows = (read_file | 'Filter' >> beam.ParDo(Filter([[1,'%d-%m-%y']]))  
                            | 'Print'  >> beam.Map(print))