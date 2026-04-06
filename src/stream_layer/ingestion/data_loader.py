import csv

def csv_data_generator(data_path: str):
    with open(data_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for idx, row in enumerate(reader):
            yield idx, row