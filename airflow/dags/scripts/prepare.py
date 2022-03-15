import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq

def format_to_parquet(src_file):
    if not src_file.endswith('.tsv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file, parse_options=pv.ParseOptions(delimiter="\t"))
    pq.write_table(table, src_file.replace('.tsv', '.parquet'))