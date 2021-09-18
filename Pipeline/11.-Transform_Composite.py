import apache_beam as beam
from datetime import datetime


class Cast_Dates(beam.PTransform):
    def __init__(self, DatesFormat):
        self.DatesFormat = DatesFormat

    def expand(self, collection):

        def format(element, fields):        
            row = element.split(',')

            for i in fields:
                try:
                    row[i[0]] = (datetime.strptime(row[i[0]],i[1]).strftime('%Y-%m-%d'))
                except:
                    row[i[0]] = ''
            return row
        
        out = (collection | 'Cast dates' >> beam.Map(format, fields = self.DatesFormat))
            
        return out




with beam.Pipeline() as pipeline:

    read_file = (pipeline   | 'Read' >> beam.io.ReadFromText(file_pattern = 'inputs/InfoDataflow', skip_header_lines = 1)
                            #| 'Composite' >> beam.ParDo(format,fields=[[1,'%d-%m-%y']]))
                            | 'Composite' >> beam.Cast_Dates(DatesFormat = [[1,'%d-%m-%y']])
    )



    print_data = read_file | 'Print' >> beam.Map(print)

    