import json
import os
from .config import Config
from .logging_setup import get_logger

logger = get_logger()

class FilterLoader:
    def __init__(self, config_dir=None):
        if config_dir:
            self.config_dir = config_dir
        else:
            self.config_dir = os.path.join(Config.BASE_DIR, 'transform', 'config')
            
        self.manual_path = os.path.join(self.config_dir, 'filter.json')
        self.transient_path = os.path.join(self.config_dir, 'managed_filter.tmp.json')

    def load(self):
        """Loads and merges manual filter and transient sheet data."""
        base_config = self._load_json(self.manual_path)
        transient_config = self._load_json(self.transient_path)
        
        return self._merge_configs(transient_config, base_config)

    def _load_json(self, path):
        if not os.path.exists(path):
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load {path}: {e}")
            return {}

    def _merge_configs(self, base, override):
        """
        Merges override config (Manual) ON TOP OF base config (Sheet).
        
        Args:
            base (dict): The base configuration (usually from Sheet/Transient).
            override (dict): The overriding configuration (Manual).
            
        Returns:
            dict: The merged configuration.
        """
        merged = base.copy()
        
        # 1. Lists (Union)
        for key in ["delete_files"]:
            base_list = base.get(key, [])
            override_list = override.get(key, [])
            # Union of unique items
            merged[key] = list(set(base_list) | set(override_list))
        
        # 2. Dict of Lists (Append/Union)
        for key in ["keep_rows", "delete_columns", "keep_columns", "delete_rows"]:
            base_dict = base.get(key, {})
            override_dict = override.get(key, {})
            merged[key] = base_dict.copy()
            
            for file, items in override_dict.items():
                if file not in merged[key]:
                    merged[key][file] = []
                
                if not isinstance(items, list): items = [items]
                
                current_set = set(str(x) for x in merged[key][file])
                for item in items:
                    if str(item) not in current_set:
                        merged[key][file].append(item)
                        current_set.add(str(item))

        # 3. Dict of Dicts (Recursive Update)
        for key in ["remap_keys", "remap_columns"]:
            base_dict = base.get(key, {})
            override_dict = override.get(key, {})
            merged[key] = base_dict.copy()
            
            for file, mappings in override_dict.items():
                if file not in merged[key]:
                    merged[key][file] = mappings
                else:
                    # If both are dicts, update. If one is not (legacy file-level), override.
                    if isinstance(merged[key][file], dict) and isinstance(mappings, dict):
                        merged[key][file].update(mappings)
                    else:
                         merged[key][file] = mappings
                         
        return merged
