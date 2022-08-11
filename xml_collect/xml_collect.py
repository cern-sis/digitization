#!/usr/bin/env python

import logging
import os
import re
import sys
from glob import glob

import click

REGEXP = r"(?!original)([\w]+)\.(xml)"
MAX_NUMBER_OF_RECORDS_COLLECT = 500


@click.group()
def xml_collect():
    pass


@click.command()
@click.argument("input_dir", type=click.STRING)
@click.argument("output_dir", type=click.STRING)
def records_collection(input_dir, output_dir):
    records_collection_dict, dict_key = {}, 0
    records_collection = []
    logging.info("Finding XML files")
    for dir in os.walk(input_dir):
        for file_path in glob(os.path.join(dir[0], "*.xml")):
            file_name = file_path.split("/")[-1]
            if re.match(REGEXP, file_name):
                if len(records_collection) == MAX_NUMBER_OF_RECORDS_COLLECT:
                    dict_key += 1
                    records_collection_dict[dict_key] = records_collection
                    records_collection.clear()
                    logging.info("{} collection created.".format(dict_key))
                else:
                    records_collection.append(file_path)
    records_collection_dict[dict_key + 1] = records_collection
    logging.info("Searching completed.")

    for k, records_collection in records_collection_dict.items():
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        filename = "{}/{}.xml".format(output_dir, k)

        with open(filename, "w") as nf:
            nf.write("<collection>")
            for record_path in records_collection:
                with open(record_path, "r") as f:
                    logging.info("Reading {}".format(record_path))
                    data = f.read()

                # Remove collection tag to have only one per xml file
                data = data.replace("<collection>", "")
                data = data.replace("</collection>", "")

                # Write the xml in the collection
                logging.info("Writing in the collection {}".format(filename))
                nf.write(data)

            nf.write("</collection>")

        logging.info("Collection {} written successfully.".format(k))


xml_collect.add_command(records_collection)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    xml_collect()
