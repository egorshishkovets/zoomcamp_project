#from pydantic.dataclasses import dataclass

#@dataclass
class Imdb_uploaded_file():
    def __init__(self, link: str):
        self.link = link
        self.archive_name = self.link.split('/')[-1]
        self.file_name = self.archive_name.replace('.gz','')
        self.parquet_file = self.file_name.replace('.tsv', '.parquet')
        self.table_name = f"{self.file_name.split('.')[0]}_{self.file_name.split('.')[1]}"

    #link: str
    #def __post_init__(self):
    #    self.archive_name = self.link.split('/')[-1]
    #    self.file_name = self.archive_name.replace('.gz','')
    #    self.parquet_file = self.file_name.replace('.tsv', '.parquet')
    #    self.table_name = f"{self.file_name.split('.')[0]}_{self.file_name.split('.')[1]}"
