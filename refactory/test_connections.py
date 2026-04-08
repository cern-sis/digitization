import tempfile
import os
from storage_connection import S3Provider, CernboxProvider

def test_s3():
    print("--- Testing AWS S3 connection ---")
    try:
        s3 = S3Provider(bucket="digitization-dev")
        base_path = "cern-archives/raw/PDF/"

        folders = s3.list_folders(base_path)
        print("✅ Read: Success! Connected to S3.")
        print(f"Found {len(folders)} folders in '{base_path}'.")

    except Exception as e:
        print("❌ Failed to connect/operate on S3.")
        print(f"Details: {e}")

def test_cernbox():
    print("\n--- Testing CERNBOX connection (Hybrid Mode) ---")

    # 1. Read Variables (Public)
    public_hash = "QslvWRIPsBcDAOK"
    read_base_path = "" # Relative path inside the public link

    # 2. Write Variables (Private/Authenticated)
    cern_user = "gadesant" # CERN username
    cern_password = os.environ.get("CERNBOX_PASSWORD")
    write_base_path = "eos/user/g/gadesant/teste/"#"eos/user/{u}/{user}/teste/"

    if public_hash == "PUT_YOUR_PUBLIC_HASH_HERE":
        print("Warning: Configure the public_hash in the code before testing.")
        return

    if not cern_password:
        print("❌ The CERNBOX_PASSWORD environment variable is not set.")
        print("Run in terminal: export CERNBOX_PASSWORD='your_password'")
        return

    try:
        # Passing all three arguments
        cernbox = CernboxProvider(public_link_hash=public_hash, account=cern_user, password=cern_password)

        print("\n[Phase 1: Reading from Public Link]")
        folders = cernbox.list_folders(read_base_path)
        print("✅ Read: Success (Anonymous)!")
        print(f"Found {len(folders)} items at the root of the link.")

        print("\n[Phase 2: Writing via Authenticated WebDAV]")

        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as tmp:
            tmp.write("Authenticated upload from test_connections.py")

    except Exception as e:
        print("❌ Failed to connect/operate on CERNBOX.")
        print(f"Details: {e}")

if __name__ == "__main__":
    test_s3()
    # test_cernbox()
