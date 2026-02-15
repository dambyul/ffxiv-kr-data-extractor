import os
import shutil
import datetime

class PathManager:
    def __init__(self, base_dir, folder_name, sub_path=""):
        self.base_dir = base_dir
        self.folder_name = folder_name
        self.timestamp = datetime.datetime.now().strftime("%m%d.%H%M")
        self.version_string = folder_name.replace(".0000.0000", f".{self.timestamp}")
        
        # Language-specific paths
        self.src_root = os.path.join(base_dir, "transform", "original", sub_path, folder_name)
        self.dst_root = os.path.join(base_dir, "transform", "output", sub_path, self.version_string)
        self.target_dir = os.path.join(self.dst_root, "raw-exd-all")
        
        self.config_path = os.path.join(base_dir, "transform", "config", "filter.json")
        self.rsv_json_path = os.path.join(base_dir, "transform", "config", "rsv.json")
        self.preset_json_path = os.path.join(base_dir, "transform", "config", "preset.json")
        self.validation_json_path = os.path.join(base_dir, "transform", "validation.json")
        
    @property
    def data_json_path(self):
        return os.path.join(self.dst_root, "data.json")

    def prepare_output_dir(self):
        if not os.path.exists(self.src_root):
            print(f"Error: Source directory not found: {self.src_root}")
            return False
            
        print(f"Isolating {self.folder_name} to output/{self.version_string}...")
        try:
            if os.path.exists(self.dst_root):
                shutil.rmtree(self.dst_root)
            shutil.copytree(self.src_root, self.dst_root)
            return True
        except Exception as e:
            print(f"Failed to copy directory: {e}")
            return False

    def get_version_txt_path(self):
        return os.path.join(self.dst_root, "version.txt")

    def get_zip_paths(self):
        # Zip location alongside extracted files
        zip_base = os.path.join(self.dst_root, "rawexd")
        return zip_base, f"{zip_base}.zip"
