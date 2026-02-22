
import shutil
import os

src = r"c:\Users\rahul\OneDrive\Desktop\ccall\_archive\csv_import_api.py"
dst = r"c:\Users\rahul\OneDrive\Desktop\ccall\csv_import_api.py"

if os.path.exists(src):
    shutil.move(src, dst)
    print(f"Moved {src} to {dst}")
else:
    print(f"Source file {src} not found!")
