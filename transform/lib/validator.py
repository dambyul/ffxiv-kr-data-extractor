import os
import json
from .logging_setup import get_logger

logger = get_logger()

class ValidationManager:
    def __init__(self, preset_path):
        self.preset_path = preset_path
        self.expected_files = set()
        self.expected_dirs = []
        self.load_presets()

    def load_presets(self):
        if not os.path.exists(self.preset_path):
            logger.warning(f"Warning: Preset file not found at {self.preset_path}")
            return
            
        try:
            with open(self.preset_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Load presets and handle case variations
            presets = data.get("Presets", []) or data.get("presets", [])
                
            for p in presets:
                # Skip '폰트' validation
                if p.get("name") == "폰트":
                    continue
                    
                entries = p.get("Entries", []) or p.get("entries", [])
                for entry in entries:
                    # Normalize path and type
                    path = entry.get("Path") or entry.get("path")
                    entry_type = entry.get("Type") or entry.get("type")
                    
                    if not path: continue
                    
                    # Normalize slashes
                    norm_path = path.replace('\\', '/')
                    
                    if entry_type == "File":
                        self.expected_files.add(norm_path)
                    elif entry_type == "Directory":
                        self.expected_dirs.append(norm_path)
                        
        except Exception as e:
            logger.error(f"Error loading presets: {e}")

    def validate(self, target_dir):
        """Validate actual files against expected presets."""
        results = {
            "not_found": [],
            "unknown": []
        }
        
        # Ignore versioning and manifest files
        ignored_patterns = ["rawexd.zip", "version.txt", "data.json"]

        # Check for missing files in presets
        for f_path in self.expected_files:
            if any(f_path == p or f_path.startswith(p + "/") for p in ignored_patterns):
                continue
                
            full_path = os.path.join(target_dir, f_path)
            if not os.path.exists(full_path):
                results["not_found"].append({
                    "path": f_path,
                    "type": "File"
                })
        
        # Check for missing directories in presets
        for d_path in self.expected_dirs:
            if any(d_path == p or d_path.startswith(p + "/") for p in ignored_patterns):
                continue

            full_path = os.path.join(target_dir, d_path)
            if not os.path.exists(full_path) or not os.path.isdir(full_path):
                results["not_found"].append({
                    "path": d_path,
                    "type": "Directory"
                })

        # Check for output files not defined in presets
        for root, dirs, files in os.walk(target_dir):
            for f in files:
                full_path = os.path.join(root, f)
                rel_path = os.path.relpath(full_path, target_dir).replace('\\', '/')
                
                if any(rel_path == p or rel_path.startswith(p + "/") for p in ignored_patterns):
                    continue

                is_expected = False
                if rel_path in self.expected_files:
                    is_expected = True
                else:
                    for d_path in self.expected_dirs:
                        if rel_path.startswith(d_path + "/") or rel_path == d_path:
                             is_expected = True
                             break
                
                if not is_expected:
                    results["unknown"].append({
                        "path": rel_path,
                        "type": "File"
                    })
            
        return results

    def save_report(self, results, output_path):
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=4, ensure_ascii=False)
            logger.info(f"Validation report saved to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save validation report: {e}")
