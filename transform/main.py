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
from lib.resource_release import build_release, publish_release

# Load environmental variables
load_dotenv()

# Setup Global Logging
logger = setup_logging()

class Orchestrator:
    def __init__(self, folder_name, sub_path="", with_resources=None):
        self.base_dir = Config.BASE_DIR
        self.with_resources = (all(os.environ.get(key) for key in ('RESOURCE_SWAP_PROJECT', 'RESOURCE_KR_CLIENT', 'RESOURCE_GLOBAL_CLIENT'))
                               if with_resources is None else with_resources)
        if self.with_resources and os.environ.get('RESOURCE_RELEASE_CHANNEL', 'stable') != 'stable':
            raise ValueError('The integrated CSV pipeline publishes Stable; use resources.py for Staging images')
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
            logger.info(f"Phase 1: Syncing filter configuration from Google Sheets...")
            if not self.fs.update_config():
                logger.warning("Warning: Filter sync failed, using cached manual config only.")
            
            # Load Merged Config
            self.config = self.fl.load()
            logger.info("Loaded merged filter configuration.")

            # Isolate source data to output directory
            logger.info(f"Phase 2: Isolating {self.pm.folder_name} to output/{self.pm.version_string}...")
            if not self.pm.prepare_output_dir(): 
                return

            target = self.pm.target_dir
            
            logger.info(f"Phase 3: Initial cleanup and manual filters...")
            self.cp.initial_cleanup(target)
            self.cp.apply_manual_filters(target, self.config)

            logger.info(f"Phase 4: Applying column remapping from filter.json...")
            self.cp.apply_column_remapping(target, self.config)

            logger.info(f"Phase 5: Anonymizing chat quest phrases to prevent broadcast...")
            self.cp.anonymize_chat_phrases(target)
            
            logger.info(f"Phase 6: Filtering columns...")
            self.cp.filter_columns(target, config=self.config)

            logger.info(f"Phase 7: Removing rows without target language content...")
            self.cp.remove_empty_rows(target, config=self.config)

            logger.info(f"Phase 8: Normalizing EventItem offsets after filtering...")
            self.cp.normalize_event_item_offsets(target)
            
            logger.info(f"Phase 9: Processing RSV keys...")
            self.cp.process_rsv(target) 
            
            logger.info(f"Phase 10: Syncing ACT overrides...")
            if self.rm.new_keys_found:
                # Sync new keys with ACT overrides
                self.rm.save()
                self.rm.sync_act_overrides()
                self.cp.process_rsv(target)
            else:
                self.rm.sync_act_overrides()
                
            logger.info(f"Phase 11: Generating Manifest (data.json)...")
            self.generate_manifest()

            logger.info(f"Phase 12: Removing files without Korean content...")
            self.cp.remove_non_korean_files(target)

            logger.info(f"Phase 13: Finalizing file names (.ko.csv -> .csv)...")
            self.cp.rename_files(target)

            # Package and versioning
            rawexd_path = self.finalize_directory()
            self.create_zip(rawexd_path)
            self.create_version_txt()

            logger.info(f"Phase 14: Running validation...")
            self.run_validation()
            
        finally:
            # Cleanup Transient Config
            if hasattr(self, 'fl') and os.path.exists(self.fl.transient_path):
                try:
                    os.remove(self.fl.transient_path)
                    logger.info("Cleaned up transient filter configuration.")
                except Exception as e:
                    logger.warning(f"Failed to cleanup transient config: {e}")
        
        resource_release = None
        if self.with_resources:
            logger.info("Extracting and packaging Korean icons 120000-129999 and 180000-189999 (6 excluded IDs)...")
            resource_release = build_release(os.path.join(self.pm.dst_root, 'resources'), self.pm.folder_name, self.pm.version_string)

        logger.info(f"Phase 15: Uploading to S3...")
        zip_base, zip_path = self.pm.get_zip_paths()
        ver_path = self.pm.get_version_txt_path()
        data_path = self.pm.data_json_path
        
        if self.uploader.upload_files([zip_path]):
            # Local cleanup: Only delete zip, keep version.txt and data.json
            self.uploader.cleanup_local([zip_path])
        else:
            raise RuntimeError('CSV upload failed; resource release was not published')

        if resource_release:
            archive, release = resource_release
            # Publish the release descriptor only after its archive has uploaded successfully.
            publish_release(self.uploader, archive, release, data_path=data_path)
        elif not self.uploader.upload_files([data_path]):
            raise RuntimeError('CSV data.json upload failed')
        # Text and image resources share this commit marker; font version remains independent.
        if not self.uploader.upload_files([ver_path]):
            raise RuntimeError('Shared text/resource version publication failed')
        
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
        
        results = self.validator.validate(target_dir, config=self.config)
        if results:
            self.validator.save_report(results, self.pm.validation_json_path)
        else:
            logger.info("Validation passed: All expected files present.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('folder_name')
    parser.add_argument('--with-resources', action='store_true', default=None, help='Include Korean icons; automatic when resource client settings are configured')
    parser.add_argument('--without-resources', dest='with_resources', action='store_false', help='Publish text only')
    arguments = parser.parse_args()
    Orchestrator(arguments.folder_name, with_resources=arguments.with_resources).run()
