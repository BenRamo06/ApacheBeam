from apache_beam import PTransform, PCollection, Map
from apache_beam.io import ReadFromText
from utils.parsers import row_to_array
from typing import Callable


class parse_files(PTransform):
    def __init__(self, path: str, delimiter: str, parser: Callable):
        self.path = path
        self.delimiter = delimiter
        self.parser = parser

    def expand(self, pcoll: PCollection):
        lines = pcoll | "ReadLines" >> ReadFromText(self.path)
        parsed_data = lines | "SplitAndParse" >> Map(lambda row: self.parser(row_to_array(row, delimiter=self.delimiter)))

        return parsed_data
