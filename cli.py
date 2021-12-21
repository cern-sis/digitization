import click
import pysftp
import os


@click.command()
@click.option('--force', default=False)
def download(force):
    """Download files from ftp."""
    host = os.getenv('FTP_HOST')
    username = os.getenv('FTP_USERNAME')
    password = os.getenv('FTP_PASSWORD')

    main_directory = os.getenv('FTP_ROOT_PATH', '/CERN-Project-Files')
    download_directory = os.getenv('DOWNLOAD_DIR', '/tmp/')

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
        sftp.cwd(main_directory)
        directory_structure = sftp.listdir_attr()
        for attr in directory_structure:
            if force or not os.path.isdir(os.path.join(download_directory, attr.filename)):
                click.echo(f'Downloading `{attr.filename}`.')
                remoteFilePath = attr.filename
                localFilePath = download_directory
                sftp.get_r(remoteFilePath, localFilePath, preserve_mtime=True)
            else:
                click.echo(f'Directory already exists`{attr.filename}`. Skip downloading...')
if __name__ == '__main__':
    download()
