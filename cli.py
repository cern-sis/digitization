import xml.etree.ElementTree as ET
import click
import pysftp
import os
import shutil


URL = "https://digitization.web.cern.ch"

main_directory = (
    "/eos/project/p/psdigitization/public/CERN-Project-Files/CERN-Project-Files/www"
)
ERROR = []
MISSING_XMLS = []


def download_files_from_ftp(force=False):
    host = os.getenv("FTP_HOST")
    username = os.getenv("FTP_USERNAME")
    password = os.getenv("FTP_PASSWORD")

    main_directory = os.getenv("FTP_ROOT_PATH", "/CERN-Project-Files")
    download_directory = os.getenv("DOWNLOAD_DIR", "/tmp/")

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(
        host=host, username=username, password=password, cnopts=cnopts
    ) as sftp:
        sftp.cwd(main_directory)
        directory_structure = sftp.listdir_attr()
        for attr in directory_structure:
            if force or not os.path.isdir(
                os.path.join(download_directory, attr.filename)
            ):
                click.echo(f"Downloading `{attr.filename}`.")
                remoteFilePath = attr.filename
                localFilePath = download_directory
                sftp.get_r(remoteFilePath, localFilePath, preserve_mtime=True)
            else:
                click.echo(
                    f"Directory already exists`{attr.filename}`. Skip downloading..."
                )


def fix_xml(root, xml_path, tif_path, pdf_path):
    xml_file_name = os.path.basename(xml_path)
    xml_file_name_original = "original_{}".format(xml_file_name)
    xml_path_original = os.path.join(root, xml_file_name_original)

    if os.path.isfile(xml_path):
        if os.path.isfile(xml_path_original):
            click.echo("We have original")
            os.remove(xml_path)
            shutil.copy2(
                os.path.join(root, "original_{}".format(xml_file_name)), xml_path
            )
        else:
            click.echo("Saving the original file")
            shutil.copy2(
                xml_path, os.path.join(root, "original_{}".format(xml_file_name))
            )
    else:
        click.echo("XML files are missing completely!")
        MISSING_XMLS.append(xml_path)

    pdf_url = url_from_eos_path(pdf_path)
    tif_url = url_from_eos_path(tif_path)

    try:
        tree = ET.parse(xml_path)
        xml_root = tree.getroot()
        for x in xml_root.findall('.//datafield[@tag="FFT"]'):
            if x.find('.//subfield[@code="a"]').text.startswith("[PATH]"):
                x.find('.//subfield[@code="a"]').text = pdf_url

            elif x.find('.//subfield[@code="a"]').text.startswith("[EOS_PATH]"):
                x.attrib["tag"] = "856"
                x.attrib["ind1"] = "4"
                for child in x.getchildren():
                    if child.attrib["code"] == "d":
                        x.remove(child)
                    if child.attrib["code"] == "a":
                        child.attrib["code"] = "u"
                        child.text = tif_url
                    if child.attrib["code"] == "t":
                        child.attrib["code"] = "q"
                        child.text = "TIFF"
        tree.write(xml_path, encoding="utf-8")
    except Exception:
        ERROR.append(xml_path)


def find_all_xmls():
    os.chdir(main_directory)
    for root, dirs, files in os.walk(main_directory, topdown=False):
        try:
            xml_path = os.path.join(
                root,
                next(
                    filter(
                        lambda x: not x.startswith("original_") and x.endswith(".xml"),
                        files,
                    )
                ),
            )
            tif_path = os.path.join(
                root, next(filter(lambda x: x.endswith(".tif"), files))
            )
            pdf_path = os.path.join(
                root, next(filter(lambda x: x.endswith(".pdf"), files))
            )
        except StopIteration:
            continue
        fix_xml(root, xml_path, tif_path, pdf_path)
        click.echo(xml_path)

        test_file = os.path.join(root, "test.xml")
        if os.path.isfile(test_file):
            os.remove(test_file)
            click.echo(test_file)


@click.group()
def digitization():
    pass


@digitization.command()
@click.option("--force", default=False, show_default=True, is_flag=True)
@click.option("--fix-eos-paths", default=False, show_default=True, is_flag=True)
def download(force, fix_eos_paths):
    """Download files from ftp."""

    click.echo("Downloading new files.")
    download_files_from_ftp(force=force)

    if fix_eos_paths:
        click.echo("Fixing paths in xml.")
        find_all_xmls()


@digitization.command("fix-eos-paths")
def fix_eos_paths():
    """Fix EOS paths."""
    click.echo("Fixing paths in xml")
    find_all_xmls()


if __name__ == '__main__':
    digitization()
