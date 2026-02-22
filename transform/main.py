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
from lib.filter_loader import FilterLoader
from lib.filter_sync import FilterSync
from lib.logging_setup import setup_logging
from lib.config import Config
from lib.discord_notifier import DiscordNotifier

# Load environmental variables
load_dotenv()

# Setup Global Logging
logger = setup_logging()

class Orchestrator:
    def __init__(self, folder_name, sub_path=""):
        self.base_dir = Config.BASE_DIR
        self.pm = PathManager(self.base_dir, folder_name, sub_path=sub_path)
        self.rm = RSVManager(self.pm.rsv_json_path)
        self.cp = CSVProcessor(self.rm)
        self.uploader = S3Uploader()
        self.validator = ValidationManager(self.pm.preset_json_path)
        self.discord = DiscordNotifier(Config.DISCORD_WEBHOOK_URL)
        
        # self.fs and self.fl initialized later
        self.config = {}

    def init_filters(self):
        logger.info("Initializing filters...")
        cdir = os.path.join(self.base_dir, "transform", "config")
        self.fs = FilterSync(cdir)
        self.fl = FilterLoader(cdir)

    def run(self):
        logger.info(f"=== Starting Unified CSV Transformation Pipeline ===")
        logger.info(f"Target Version: {self.pm.version_string}")

        try:
            self.init_filters()
            
            # Sync Filter Configuration
            logger.info(f"Phase 0: Syncing filter configuration from Google Sheets...")
            if not self.fs.update_config():
                logger.warning("Warning: Filter sync failed, using cached manual config only.")
            
            # Load Merged Config
            self.config = self.fl.load()
            logger.info("Loaded merged filter configuration.")

            # Isolate source data to output directory
            logger.info(f"Phase 1: Isolating {self.pm.folder_name} to output/{self.pm.version_string}...")
            if not self.pm.prepare_output_dir(): 
                return

            target = self.pm.target_dir
            
            logger.info(f"Phase 2: Initial cleanup and manual filters...")
            self.cp.initial_cleanup(target)
            self.cp.apply_manual_filters(target, self.config)

            logger.info(f"Phase 3: Applying column remapping from filter.json...")
            self.cp.apply_column_remapping(target, self.config)

            logger.info(f"Phase 4: Anonymizing chat quest phrases to prevent broadcast...")
            self.cp.anonymize_chat_phrases(target)
            
            logger.info(f"Phase 5: Filtering columns...")
            self.cp.filter_columns(target, config=self.config)

            logger.info(f"Phase 6: Removing rows without target language content...")
            self.cp.remove_empty_rows(target, config=self.config)
            
            logger.info(f"Phase 7: Processing RSV keys...")
            self.cp.process_rsv(target) 
            
            logger.info(f"Phase 8: Syncing ACT overrides...")
            if self.rm.new_keys_found:
                # Sync new keys with ACT overrides
                self.rm.save()
                self.rm.sync_act_overrides()
                self.cp.process_rsv(target)
            else:
                self.rm.sync_act_overrides()
                
            logger.info(f"Phase 9: Generating Manifest (data.json)...")
            self.generate_manifest()

            logger.info(f"Phase 10: Removing files without Korean content...")
            self.cp.remove_non_korean_files(target)

            logger.info(f"Phase 11: Finalizing file names (.ko.csv -> .csv)...")
            self.cp.rename_files(target)

            # Package and versioning
            rawexd_path = self.finalize_directory()
            self.create_zip(rawexd_path)
            self.create_version_txt()

            logger.info(f"Phase 12: Running validation...")
            self.run_validation()
            
        finally:
            # Cleanup Transient Config
            if hasattr(self, 'fl') and os.path.exists(self.fl.transient_path):
                try:
                    os.remove(self.fl.transient_path)
                    logger.info("Cleaned up transient filter configuration.")
                except Exception as e:
                    logger.warning(f"Failed to cleanup transient config: {e}")
        
        logger.info(f"Phase 13: Uploading to S3...")
        zip_base, zip_path = self.pm.get_zip_paths()
        ver_path = self.pm.get_version_txt_path()
        data_path = self.pm.data_json_path
        
        if self.uploader.upload_files([zip_path, ver_path, data_path]):
            # Local cleanup: Only delete zip, keep version.txt and data.json
            self.uploader.cleanup_local([zip_path])
        
        logger.info(f"\n=== Pipeline Completed Successfully ===")
        logger.info(f"Results located in: {self.pm.dst_root}")

        # Notify Success (Only on success)
        self.discord.send_notification(self.pm.version_string, self.pm.folder_name)


    def generate_manifest(self):
        # Generate data.json manifest
        try:
            # Read Presets
            with open(self.pm.preset_json_path, 'r', encoding='utf-8') as f:
                preset_data = json.load(f)
            
            # Support both "Presets" and "presets"
            presets = preset_data.get("Presets") or preset_data.get("presets", [])
            
            # Get RSV dict (filename -> count)
            rsv_counts = self.rm.rsv_files
            
            # Create Manifest Data
            manifest = {
                "presets": presets,
                "third-party": preset_data.get("third-party", []),
                "rsv": rsv_counts
            }
            
            # Write to Output Directory
            with open(self.pm.data_json_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=4, ensure_ascii=False)
            
            logger.info(f"Manifest saved to: {self.pm.data_json_path}")
            
        except Exception as e:
            logger.error(f"Failed to generate manifest: {e}")

    def finalize_directory(self):
        # Already used correctly
        logger.info("Finalizing directory name...")
        final_path = os.path.join(self.pm.dst_root, "rawexd")
        if os.path.exists(self.pm.target_dir):
            if os.path.exists(final_path): 
                shutil.rmtree(final_path)
            os.rename(self.pm.target_dir, final_path)
        return final_path

    def create_zip(self, rawexd_path):
        if not os.path.exists(rawexd_path): return
        logger.info("Zipping results...")
        zip_base, _ = self.pm.get_zip_paths()
        shutil.make_archive(zip_base, 'zip', rawexd_path)

    def create_version_txt(self):
        logger.info("Creating version.txt...")
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
            logger.info("Validation passed: All expected files present.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <folder_name>")
    else:
        Orchestrator(sys.argv[1]).run()
