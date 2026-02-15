import sys
import os
import shutil
import json
from dotenv import load_dotenv
from lib.paths import PathManager
from lib.rsv import RSVManager
from lib.processor import CSVProcessor
from lib.uploader import S3Uploader
from lib.validator import ValidationManager

# Load environmental variables
load_dotenv()

class Orchestrator:
    def __init__(self, folder_name, sub_path=""):
        self.base_dir = os.getenv("BASE_DIR")
        if not self.base_dir:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.base_dir = os.path.dirname(current_dir)
        
        self.pm = PathManager(self.base_dir, folder_name, sub_path=sub_path)
        self.rm = RSVManager(self.pm.rsv_json_path)
        self.cp = CSVProcessor(self.rm)
        self.uploader = S3Uploader()
        self.validator = ValidationManager(self.pm.preset_json_path)

    def run(self):
        print(f"=== Starting Unified CSV Transformation Pipeline ===")
        print(f"Target Version: {self.pm.version_string}")

        # Isolate source data to output directory
        print(f"Phase 1: Isolating {self.pm.folder_name} to output/{self.pm.version_string}...")
        if not self.pm.prepare_output_dir(): 
            return

        target = self.pm.target_dir
        
        # Initial cleanup and manual filters
        self.cp.initial_cleanup(target)
        self.cp.apply_manual_filters(target, self.pm.config_path)
        
        # Filter columns and rows
        self.cp.filter_columns(target)
        self.cp.remove_empty_rows(target)
        
        
        # RSV Key validation and English mapping
        self.cp.process_rsv(target) 
        
        if self.rm.new_keys_found:
            # Sync new keys with ACT overrides
            self.rm.save()
            self.rm.sync_act_overrides()
            self.cp.process_rsv(target)
        else:
            self.rm.sync_act_overrides()
            
        # Generate data.json (Manifest) in output directory
        self.generate_manifest()

        # Filter non-Korean files and rename
        self.cp.remove_non_korean_files(target)
        self.cp.rename_files(target)

        # Package and versioning
        rawexd_path = self.finalize_directory()
        self.create_zip(rawexd_path)
        self.create_version_txt()

        # Validation
        self.run_validation()
        
        # Upload to S3 and local cleanup
        zip_base, zip_path = self.pm.get_zip_paths()
        ver_path = self.pm.get_version_txt_path()
        data_path = self.pm.data_json_path
        
        if self.uploader.upload_files([zip_path, ver_path, data_path]):
            # Local cleanup: Only delete zip, keep version.txt and data.json
            self.uploader.cleanup_local([zip_path])
        
        print(f"\n=== Pipeline Completed Successfully ===")
        print(f"Results located in: {self.pm.dst_root}")

    def generate_manifest(self):
        # Generate data.json manifest
        try:
            # Read Presets
            with open(self.pm.preset_json_path, 'r', encoding='utf-8') as f:
                preset_data = json.load(f)
            
            # Support both "Presets" and "presets"
            presets = preset_data.get("Presets") or preset_data.get("presets", [])
            
            # Get RSV List
            rsv_list = sorted(list(self.rm.rsv_files))
            
            # Create Manifest Data
            manifest = {
                "presets": presets,
                "rsv": rsv_list
            }
            
            # Write to Output Directory
            with open(self.pm.data_json_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=4, ensure_ascii=False)
            
            print(f"Manifest saved to: {self.pm.data_json_path}")
            
        except Exception as e:
            print(f"Failed to generate manifest: {e}")

    def finalize_directory(self):
        print("Phase 10: Finalizing directory name...")
        final_path = os.path.join(self.pm.dst_root, "rawexd")
        if os.path.exists(self.pm.target_dir):
            if os.path.exists(final_path): 
                shutil.rmtree(final_path)
            os.rename(self.pm.target_dir, final_path)
        return final_path

    def create_zip(self, rawexd_path):
        if not os.path.exists(rawexd_path): return
        print("Phase 11: Zipping results...")
        zip_base, _ = self.pm.get_zip_paths()
        shutil.make_archive(zip_base, 'zip', rawexd_path)

    def create_version_txt(self):
        print("Phase 12: Creating version.txt...")
        path = self.pm.get_version_txt_path()
        with open(path, 'w', encoding='utf-8') as f:
            f.write(self.pm.version_string)

    def run_validation(self):
        # Validate against version root
        target_dir = self.pm.dst_root
        
        results = self.validator.validate(target_dir)
        if results:
            self.validator.save_report(results, self.pm.validation_json_path)
        else:
            print("Validation passed: All expected files present.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <folder_name>")
    else:
        Orchestrator(sys.argv[1]).run()
