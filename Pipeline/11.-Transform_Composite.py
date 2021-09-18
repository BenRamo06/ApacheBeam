import apache_beam as beam
from datetime import datetime

# function to validate rows, use it function with a Map filter 
def correct_row(element):
    if len(element) == len('id,date,amount1,amount2,amount3,amount4,target'.split(',')):
        return element


# Create PTransform that cast fields to date with a specific format

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


# Create PTransform convert data to json

class Cast_To_Json(beam.PTransform):

    def __init__(self, structure):
        self.structure = structure


    def expand(self, collection):

        def to_json(element, structure):
            return dict(zip(structure, element))


        out = (collection | 'Convert to Json' >> beam.Map(to_json, structure = self.structure))

        return out



with beam.Pipeline() as pipeline:

    read_file = (pipeline   | 'Read'       >> beam.io.ReadFromText(file_pattern = 'inputs/InfoDataflow', skip_header_lines = 1)
                            | 'Composite'  >> Cast_Dates(DatesFormat = [[1,'%d-%m-%y']])
                            | 'Valid rows' >> beam.Filter(correct_row)
                            | 'To JSON'    >> Cast_To_Json(structure = 'id,date,amount1,amount2,amount3,amount4,target'.split(','))
                            | 'Export'     >> beam.io.WriteToText(file_path_prefix = 'outputs/Composite', file_name_suffix = '.json')
                )

    print_data = read_file | 'Print' >> beam.Map(print)

    