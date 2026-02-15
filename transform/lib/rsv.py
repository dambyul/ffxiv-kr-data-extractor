import os
import json
import urllib.request

class RSVManager:
    def __init__(self, json_path):
        self.json_path = json_path
        self.rsv_data = {}
        self.rsv_files = set()
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
                print(f"Error loading rsv.json: {e}")

    def save(self):
        try:
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(self.rsv_data, f, indent=4, ensure_ascii=False)
            print(f"Updated {self.json_path} with RSV data.")
            self.new_keys_found = False
        except Exception as e:
            print(f"Failed to save rsv.json: {e}")

    def add_found_file(self, rel_path):
        """Records a relative path where an RSV key was found, removing .ko suffix."""
        clean_path = rel_path.replace('.ko.csv', '.csv').replace('\\', '/')
        if not clean_path.startswith("rawexd/"):
            clean_path = f"rawexd/{clean_path}"
        self.rsv_files.add(clean_path)

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
            print(f"Synced {updated_count} English overrides from ACT Plugin.")
            self.save()

    def _fetch_file_list(self):
        api_url = "https://api.github.com/repos/ravahn/FFXIV_ACT_Plugin/contents/Overrides"
        try:
            with urllib.request.urlopen(api_url) as response:
                data = json.loads(response.read().decode('utf-8'))
                return [f['name'] for f in data if f['name'].startswith('global_')]
        except Exception as e:
            print(f"Warning: Failed to fetch ACT file list: {e}")
            return []

    def _fetch_overrides_content(self, filenames):
        base_url = "https://raw.githubusercontent.com/ravahn/FFXIV_ACT_Plugin/master/Overrides/"
        lookup_map = {}
        print("Fetching ACT overrides content...")
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
