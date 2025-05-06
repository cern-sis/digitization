import os

import pandas as pd

from utils import generate_s3_url, get_s3_client, get_s3_file_path, list_s3_files


def process_row(row, box_file):
    for filetype in ['PDF', 'PDF_LATEX', 'TIFF']:
        s3_prefix_ = get_s3_file_path(filetype=filetype, 
                                      box_file=box_file, filename=row[1])
        files = list_s3_files('cern-archives', s3_prefix_, s3_client)
        for file in files:
            s3_url = generate_s3_url('cern-archives', file, s3_client=s3_client)
            #print(f"S3 URL: {s3_url}")
        if not files:
            print(f"No files found for {filetype} in {s3_prefix_}")
            continue

s3_client = get_s3_client()

data_path = os.path.join(os.getcwd(), 'data')
for box_file in os.listdir(data_path):
    print(f"Box file: {box_file}")
    data = pd.read_excel(os.path.join(data_path, box_file))
    data.apply(lambda x, box_file=box_file: process_row(x, box_file), axis=1, raw=True)
