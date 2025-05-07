import os
import boto3
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from xml.dom import minidom
load_dotenv()


def get_s3_file_path(filetype='', box_file='', filename=''):
    box_file = box_file.split('.')[0].upper().replace('-', '_')
    if filetype == 'PDF' or filetype == 'TIFF':
        return f"raw/{filetype}/{box_file}/{filename}/"
    elif filetype == 'PDF_LATEX':
        return f"raw/{filetype}/{box_file}/{filename}.pdf"


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.environ['ACCESS_KEY'],
        aws_secret_access_key=os.environ['SECRET_KEY'],
        endpoint_url='https://s3.cern.ch',
    )

def list_s3_files(bucket_name, prefix, s3_client=None):
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter='/'
        )
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents'] if
                    not obj['Key'].endswith('/')]
        else:
            return []
    except Exception:
        return []

def generate_s3_url(bucket_name, file_key, expiration=31556952, s3_client=None):
    return f"{bucket_name}/{file_key}/{expiration}"
    return s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket_name, "Key": file_key},
            ExpiresIn=expiration,
        )


def create_custom_xml(records_data, output_file_path):
    collection = ET.Element("collection")
    for rec in records_data:
        if not rec.get('pdf_url') and not rec.get('pdf_latex_url') and not rec.get('tiff_urls'):
            continue
        record_elem = ET.SubElement(collection, "record")
        controlfield = ET.SubElement(record_elem, "controlfield", tag="001")
        controlfield.text = str(rec['record_id'])
        if rec.get('pdf_url'):
            datafield = ET.SubElement(record_elem, "datafield", tag="856", ind1="4", ind2=" ")
            ET.SubElement(datafield, "subfield", code="u").text = rec['pdf_url']
            ET.SubElement(datafield, "subfield", code="q").text = "PDF"
        if rec.get('pdf_latex_url'):
            datafield = ET.SubElement(record_elem, "datafield", tag="856", ind1="4", ind2=" ")
            ET.SubElement(datafield, "subfield", code="u").text = rec['pdf_latex_url']
            ET.SubElement(datafield, "subfield", code="q").text = "PDF_LATEX"
        for tiff_url in rec.get('tiff_urls', []):
            datafield = ET.SubElement(record_elem, "datafield", tag="856", ind1="4", ind2=" ")
            ET.SubElement(datafield, "subfield", code="u").text = tiff_url
            ET.SubElement(datafield, "subfield", code="q").text = "TIFF"
    rough_string = ET.tostring(collection, encoding="utf-8")
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="    ")
    with open(output_file_path, "w", encoding="utf-8") as f:
        f.write(pretty_xml)
