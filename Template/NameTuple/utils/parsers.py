from data.customers import Customers

def row_to_array(row: str, delimiter: str) -> list[str]:
    """
    Convert a delimited row to a list of strings.
    """
    return [s for s in row.split(delimiter)]


def customers_parser(fields: list[str]) -> Customers:
    return Customers(*fields)
