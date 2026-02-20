import csv
import json
import os
import re
import gspread
from google.oauth2.service_account import Credentials

from .config import Config
from .logging_setup import get_logger

logger = get_logger()

class FilterSync:
    def __init__(self, config_dir):
        self.config_dir = config_dir
        # Resolve credential path relative to project root or use configured path
        base_dir = Config.BASE_DIR
        self.creds_path = os.path.join(base_dir, Config.GOOGLE_CREDS_PATH)
        self.json_path = os.path.join(config_dir, 'filter.json')
        self.manual_json_path = os.path.join(config_dir, 'manual_filter.json')
        self.transient_json_path = os.path.join(config_dir, 'managed_filter.tmp.json')
        self.sheet_id = Config.GOOGLE_SHEET_ID

    @staticmethod
    def normalize_filename(filename):
        return re.sub(r'\.(ja|ko|en|de|fr)?(\.(ja|ko|en|de|fr))?\.csv$', '.csv', filename)

    def get_data(self):
        """Fetches data from Google Sheets using API."""
        logger.info(f"Fetching data from Google Sheets API...")
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_file(self.creds_path, scopes=scopes)
            client = gspread.authorize(creds)
            sh = client.open_by_key(self.sheet_id)
            worksheet = sh.get_worksheet(0)
            return worksheet.get_all_records()
        except Exception as e:
            logger.warning(f"Failed to fetch from Google Sheets API: {e}")
            return None

    def update_config(self):
        """Generates managed_filter.tmp.json from Spreadsheet data."""
        # Initialize empty collections for Sheet data
        new_delete_rows = {}
        new_remap_keys = {}
        new_remap_columns = {} # Only row-level remaps come from sheet

        data = self.get_data()
        if data is None:
            logger.error("Error: Could not retrieve data from spreadsheet.")
            return False

        for row in data:
            # Headers: idx, File, Key, Offset, Type, Global, KR, Exclude, Swap_Key, Swap_Offset
            if not row.get('File') or not row.get('Key'):
                continue
                
            filename = self.normalize_filename(str(row['File']))
            rid = str(row['Key']).strip()
            
            # 2a. Handle Exclude
            is_excluded = str(row.get('Exclude', '')).upper() == 'TRUE'
            if is_excluded:
                if filename not in new_delete_rows:
                    new_delete_rows[filename] = []
                try:
                    ival = int(rid)
                    if ival not in new_delete_rows[filename]:
                        new_delete_rows[filename].append(ival)
                except ValueError:
                    pass

            # 2b. Handle Swap_Key
            swap_target = str(row.get('Swap_Key', '')).strip()
            if swap_target:
                if filename not in new_remap_keys:
                    new_remap_keys[filename] = {}
                new_remap_keys[filename][rid] = swap_target

            # 2c. Handle Swap_Offset (Row-specific column remap)
            swap_offset = str(row.get('Swap_Offset', '')).strip()
            gl_offset = str(row.get('Offset', '')).strip()
            if swap_offset and gl_offset:
                if filename not in new_remap_columns:
                    new_remap_columns[filename] = {}
                if rid not in new_remap_columns[filename]:
                    new_remap_columns[filename][rid] = {}
                
                # If "G" is specified, fetch the literal Global value from the sheet
                if swap_offset.lower() == "g":
                    gl_val = str(row.get('Global', ''))
                    # Store as a literal String value
                    new_remap_columns[filename][rid][gl_offset] = gl_val
                else:
                    # Try to store as an Integer Offset
                    try:
                        new_remap_columns[filename][rid][gl_offset] = int(swap_offset)
                    except ValueError:
                        # Fallback to String if not a number
                        new_remap_columns[filename][rid][gl_offset] = swap_offset
                    
        sorted_delete_rows = {}
        for filename in sorted(new_delete_rows.keys()):
            sorted_delete_rows[filename] = sorted(new_delete_rows[filename])

        sorted_remap_keys = {}
        for filename in sorted(new_remap_keys.keys()):
            sorted_remap_keys[filename] = dict(sorted(new_remap_keys[filename].items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0]))

        # Sort remap_columns by filename and row ID
        sorted_remap_columns = {}
        for filename, row_mappings in new_remap_columns.items():
            if filename not in sorted_remap_columns:
                sorted_remap_columns[filename] = {}
            for rid, col_mapping in row_mappings.items():
                sorted_remap_columns[filename][rid] = col_mapping
                
        transient_config = {
            "delete_files": [], # Sheet doesn't manage file deletion
            "remap_keys": sorted_remap_keys,
            "remap_columns": dict(sorted(sorted_remap_columns.items())),
            "keep_rows": {},
            "delete_columns": {},
            "keep_columns": {},
            "delete_rows": sorted_delete_rows
        }

        # Write to TRANSIENT file, NOT filter.json
        with open(self.transient_json_path, 'w', encoding='utf-8') as f:
            json.dump(transient_config, f, indent=4, ensure_ascii=False)

        logger.info(f"Successfully generated {self.transient_json_path} from Google Sheets")
        return True
