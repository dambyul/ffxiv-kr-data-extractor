import os
import json
from .logging_setup import get_logger

logger = get_logger()

class RSVManager:
    def __init__(self, json_path):
        self.json_path = json_path
        self.rsv_data = {}
        self.rsv_files = {} # dict: filename -> unresolved_count
        self.new_keys_found = False
        self.load()

    def load(self):
        if os.path.exists(self.json_path):
            try:
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        raw = json.loads(content)
                        # Handle migration for older RSV data formats
                        for k, v in raw.items():
                            if isinstance(v, list):
                                self.rsv_data[k] = v
                            else:
                                self.rsv_data[k] = [v, ""]
            except Exception as e:
                logger.error(f"Error loading rsv.json: {e}")

    def save(self):
        try:
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(self.rsv_data, f, indent=4, ensure_ascii=False)
            logger.info(f"Updated {self.json_path} with RSV data.")
            self.new_keys_found = False
        except Exception as e:
            logger.error(f"Failed to save rsv.json: {e}")

    def add_found_file(self, rel_path, is_unresolved=False):
        """Records a relative path where an RSV key was found and tracks unresolved count."""
        clean_path = rel_path.replace('.ko.csv', '.csv').replace('\\', '/')
        if not clean_path.startswith("rawexd/"):
            clean_path = f"rawexd/{clean_path}"
        
        if clean_path not in self.rsv_files:
            self.rsv_files[clean_path] = 0
            
        if is_unresolved:
            self.rsv_files[clean_path] += 1

    def is_unresolved(self, key):
        """Checks if an RSV key has a valid Korean translation."""
        if key in self.rsv_data:
            return not self.rsv_data[key][0] # Unresolved if Korean value is empty
        return True # New keys are unresolved by default until sync/edit

    def get_value(self, key):
        if key in self.rsv_data:
            val_pair = self.rsv_data[key]
            # Priority: User definition > ACT/Fallback
            return val_pair[0] if val_pair[0] else val_pair[1]
        else:
            self.rsv_data[key] = ["", ""]
            self.new_keys_found = True
            return ""

    @staticmethod
    def transform_key(key):
        """Transform RSV key for ACT lookup compatibility."""
        parts = key.split('_')
        if len(parts) > 4 and parts[4] == '6':
            parts[4] = '1'
            return '_'.join(parts)
        return key

    def sync_act_overrides(self):
        """Automatically fetch English names from ACT repository."""
        filenames = self._fetch_file_list()
        if not filenames:
            return
            
        lookup_map = self._fetch_overrides_content(filenames)
        if not lookup_map:
            return

        updated_count = 0
        for key, val_pair in self.rsv_data.items():
            if not val_pair[1]:
                t_key = self.transform_key(key)
                if t_key in lookup_map:
                    val_pair[1] = lookup_map[t_key]
                    updated_count += 1
        
        if updated_count > 0:
            logger.info(f"Synced {updated_count} English overrides from ACT Plugin.")
            self.save()

    def _fetch_file_list(self):
        api_url = "https://api.github.com/repos/ravahn/FFXIV_ACT_Plugin/contents/Overrides"
        try:
            with urllib.request.urlopen(api_url) as response:
                data = json.loads(response.read().decode('utf-8'))
                return [f['name'] for f in data if f['name'].startswith('global_')]
        except Exception as e:
            logger.warning(f"Failed to fetch ACT file list: {e}")
            return []

    def _fetch_overrides_content(self, filenames):
        base_url = "https://raw.githubusercontent.com/ravahn/FFXIV_ACT_Plugin/master/Overrides/"
        lookup_map = {}
        logger.info("Fetching ACT overrides content...")
        for filename in filenames:
            url = base_url + filename
            try:
                with urllib.request.urlopen(url) as response:
                    content = response.read().decode('utf-8')
                    for line in content.splitlines():
                        if '|' in line:
                            k, v = line.split('|', 1)
                            lookup_map[k.strip()] = v.strip()
            except:
                pass
        return lookup_map
