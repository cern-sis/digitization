import os
import logging
import pandas as pd
from tqdm import tqdm
from .utils import generate_s3_url, get_s3_client, get_s3_file_path, list_s3_files, create_custom_xml



def process_row(row, box_file, s3_client):
    record_id = str(row[0])
    record_name = str(row[1])

    record_data = {
        'record_id': record_id,
        'pdf_url': None,
        'pdf_latex_url': None,
        'tiff_urls': [],
    }

    for filetype in ['PDF', 'PDF_LATEX', 'TIFF']:
        s3_prefix = get_s3_file_path(filetype=filetype, box_file=box_file, filename=record_name)
        files = list_s3_files('cern-archives', s3_prefix, s3_client)

        if not files:
            logging.info(f"[MISSING] {filetype} for record {record_name} (ID: {record_id}) in {s3_prefix}")
            continue

        for file in files:
            s3_url = generate_s3_url('cern-archives', file, s3_client=s3_client)
            if filetype == 'PDF':
                record_data['pdf_url'] = s3_url
            elif filetype == 'PDF_LATEX':
                record_data['pdf_latex_url'] = s3_url
            elif filetype == 'TIFF':
                record_data['tiff_urls'].append(s3_url)

    return record_data


def create_import_xml_files(data_path, output_path):
    logging.basicConfig(filename=os.path.join(output_path, 'missing_records.log'), level=logging.INFO,
                    format='%(asctime)s - %(message)s')
    s3_client = get_s3_client()
    xml_output_path = os.path.join(output_path, 'import_xml_files')
    os.makedirs(xml_output_path, exist_ok=True)
    for box_file in os.listdir(data_path):
        df = pd.read_excel(os.path.join(data_path, box_file), header=None)
        records_data = []
        for _, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Processing {box_file}"):
            records_data.append(process_row(row, box_file, s3_client))
        xml_filename = os.path.splitext(box_file)[0] + ".xml"
        xml_path = os.path.join(xml_output_path, xml_filename)
        create_custom_xml(records_data, xml_path)
        print(f"✅ XML written: {xml_path}")
