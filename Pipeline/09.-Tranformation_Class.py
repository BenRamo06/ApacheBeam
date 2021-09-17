from os import read
import apache_beam as beam

def filters(row):

    try:
        row[3] = float(row[3])        
    except:
        row[3] = float(0)
    
    if float(row[3]) > 0 :
        return row


class convert (beam.DoFn):
    def __init__(self, header):
        self.header = header

    def process (self, element):
        if len(element) == len(self.header.split(',')):
            yield beam.pvalue.TaggedOutput('valids',dict(zip(self.header.split(','), element)))
        else:
            yield beam.pvalue.TaggedOutput('fails', ",".join(str(x) for x in element))


with beam.Pipeline() as pipeline:

    read_file = pipeline | 'Read'        >> beam.io.ReadFromText(file_pattern = 'inputs/InfoDataflow', skip_header_lines = 1, )

    divide = read_file   | 'Split data'  >> beam.Map(lambda x: x.split(','))
    
    filters = divide     | 'Class'       >> beam.Filter(filters)

    valids, fails = (filters | 'Convert' >> beam.ParDo(convert(header = 'id,date,amount1,amount2,amount3,amount4,target')).with_outputs('valids', 'fails'))

    export_fails  = fails  | 'Export Fails'  >> beam.io.WriteToText(file_path_prefix = 'outputs/TextFails', file_name_suffix = '.txt' )

    export_valids = valids | 'Export Json'   >> beam.io.WriteToText(file_path_prefix = 'outputs/TextToJson', file_name_suffix = '.json' )
