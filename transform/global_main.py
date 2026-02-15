import sys
import os
import shutil
from dotenv import load_dotenv
from lib.paths import PathManager
from lib.global_processor import GlobalCSVProcessor

# Load environmental variables
load_dotenv()

class GlobalOrchestrator:
    def __init__(self, folder_name):
        self.base_dir = os.getenv("BASE_DIR")
        if not self.base_dir:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.base_dir = os.path.dirname(current_dir)
        
        # Folder is located in original/jp/folder_name
        self.pm = PathManager(self.base_dir, folder_name, sub_path="jp")
        self.cp = GlobalCSVProcessor()

    def run(self):
        print(f"=== Starting Global CSV Transformation Pipeline (JP) ===")
        print(f"Target Version: {self.pm.version_string}")

        # Prepare output directory
        if not self.pm.prepare_output_dir(): 
            return

        target = self.pm.target_dir
        
        # Prioritize .ja.csv files
        print("Phase 2: Initial cleanup (filtering languages)...")
        self.cp.initial_cleanup(target)

        # Filter columns and rows
        print("Phase 3: Filtering columns (EN/JP/KR)...")
        self.cp.filter_columns(target)

        print("Phase 4: Filtering rows & Removing empty files...")
        self.cp.remove_empty_rows(target)
        
        print("Phase 5: Renaming files...")
        self.cp.rename_files(target)

        # Finalize output directory structure
        self.finalize_directory()

        print(f"\n=== Global Pipeline Completed Successfully ===")
        print(f"Results located in: {self.pm.dst_root}")

    def finalize_directory(self):
        print("Phase 6: Finalizing directory name...")
        final_path = os.path.join(self.pm.dst_root, "rawexd")
        if os.path.exists(self.pm.target_dir):
            if os.path.exists(final_path): 
                shutil.rmtree(final_path)
            os.rename(self.pm.target_dir, final_path)
        return final_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python global_main.py <folder_name>")
    else:
        GlobalOrchestrator(sys.argv[1]).run()
