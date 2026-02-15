import os
import csv
import re
import stat
import time
import json
import sys

# Setup CSV field limits
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

class CSVProcessor:
    def __init__(self, rsv_manager):
        self.rsv_manager = rsv_manager

    @staticmethod
    def make_writable(path):
        if os.path.exists(path):
            os.chmod(path, stat.S_IWRITE)

    @staticmethod
    def has_korean(text):
        if not text: return False
        if text.startswith("_rsv_"): return True
        return bool(re.search(r'[\uac00-\ud7af]', text))

    def safe_replace(self, src, dst):
        self.make_writable(dst)
        for _ in range(3):
            try:
                if os.path.exists(dst): os.remove(dst)
                os.rename(src, dst)
                return True
            except:
                time.sleep(0.1)
        return False

    def initial_cleanup(self, target_dir):
        # Remove all files except those ending in .ko.csv
        for root, _, files in os.walk(target_dir):
            for file in files:
                if not file.endswith(".ko.csv"):
                    path = os.path.join(root, file)
                    self.make_writable(path)
                    os.remove(path)

    def apply_manual_filters(self, target_dir, config_path):
        # Apply manual deletions from config
        if not os.path.exists(config_path): return
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        del_files = config.get("delete_files", [])
        del_rows_conf = config.get("delete_rows", {})

        for root, _, files in os.walk(target_dir):
            for f in files:
                # File deletion
                base = f.replace(".ko.csv", ".csv")
                if base in del_files or f in del_files:
                    path = os.path.join(root, f)
                    self.make_writable(path)
                    os.remove(path)
                    continue

                # Row deletion
                matched_conf = None
                for k in del_rows_conf:
                    if k == f or k.replace(".csv", ".ko.csv") == f:
                        matched_conf = del_rows_conf[k]
                        break
                
                if matched_conf:
                    self._filter_rows(os.path.join(root, f), matched_conf)

    def _filter_rows(self, path, keys_to_del):
        # Convert all keys to strings for comparison (JSON has ints, CSV has strings)
        target_keys = set(str(k) for k in keys_to_del)
        
        temp = path + ".tmp"
        modified = False
        rows = []
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                # Preserve 4-line header
                if i < 4:
                    rows.append(row)
                    continue
                
                if row and row[0] in target_keys:
                    modified = True
                    continue
                rows.append(row)
        if modified:
            with open(temp, 'w', encoding='utf-8', newline='') as f:
                csv.writer(f).writerows(rows)
            self.safe_replace(temp, path)

    def filter_columns(self, target_dir):
        # Keep columns containing Korean text or specific keywords
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith(".csv"):
                    path = os.path.join(root, file)
                    temp = path + ".tmp"
                    
                    rows = []
                    with open(path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        rows = list(reader)
                    
                    if len(rows) < 4: continue
                    
                    # Identify which columns to keep
                    col_indices = {0} # Always keep Key column
                    
                    # Scan header for keywords or Korean text
                    for i, col in enumerate(rows[0]):
                        if i == 0: continue
                        if self.has_korean(col) or 'Name' in col or 'Description' in col:
                            col_indices.add(i)
                    
                    # Scan rows for Korean text
                    for row in rows:
                        for i, cell in enumerate(row):
                            if i in col_indices: continue
                            if self.has_korean(cell):
                                col_indices.add(i)
                    
                    # Sort indices to maintain order
                    sorted_indices = sorted(list(col_indices))
                    
                    # Write cleaned rows (preserving all 4 header lines)
                    new_rows = [[row[i] for i in sorted_indices if i < len(row)] for row in rows]
                    
                    with open(temp, 'w', encoding='utf-8', newline='') as f:
                        csv.writer(f).writerows(new_rows)
                    self.safe_replace(temp, path)

    def remove_empty_rows(self, target_dir):
        # Remove rows without Korean text and delete empty files
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith(".csv"):
                    path = os.path.join(root, file)
                    temp = path + ".tmp"
                    rows = []
                    with open(path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        # Append 4 header lines
                        for _ in range(4):
                            header = next(reader, None)
                            if header: rows.append(header)
                        
                        # Filter data rows
                        for row in reader:
                            if any(self.has_korean(cell) for cell in row[1:]):
                                rows.append(row)
                    
                    with open(temp, 'w', encoding='utf-8', newline='') as f:
                        csv.writer(f).writerows(rows)
                    self.safe_replace(temp, path)

    def process_rsv(self, target_dir):
        # Replace RSV keys with English or user-defined values
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith(".csv"):
                    path = os.path.join(root, file)
                    temp = path + ".tmp"
                    modified = False
                    rows = []
                    with open(path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        rel_path = os.path.relpath(path, target_dir)
                        for i, row in enumerate(reader):
                            # Skip RSV processing for 4 header lines
                            if i < 4:
                                rows.append(row)
                                continue
                            
                            new_row = []
                            for cell in row:
                                if cell.startswith("_rsv_"):
                                    self.rsv_manager.add_found_file(rel_path)
                                    val = self.rsv_manager.get_value(cell)
                                    new_row.append(val)
                                    if val != cell: modified = True
                                else:
                                    new_row.append(cell)
                            rows.append(new_row)
                    if modified:
                        with open(temp, 'w', encoding='utf-8', newline='') as f:
                            csv.writer(f).writerows(rows)
                        self.safe_replace(temp, path)

    def remove_non_korean_files(self, target_dir):
        # Delete files containing no Korean content except RSV-referenced ones
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith(".csv"):
                    path = os.path.join(root, file)
                    rel_path = os.path.relpath(path, target_dir).replace('\\', '/')
                    
                    # Check if this file is an RSV file
                    rsv_key = f"rawexd/{rel_path}"
                    if rsv_key in self.rsv_manager.rsv_files:
                        continue

                    has_ko = False
                    with open(path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        # Skip 4 header lines before checking for Korean content
                        for _ in range(4): next(reader, None)
                        
                        # Check remaining rows for Korean
                        for row in reader:
                            if any(self.has_korean(cell) for cell in row):
                                has_ko = True
                                break
                    
                    if not has_ko:
                        self.make_writable(path)
                        os.remove(path)

    def rename_files(self, target_dir):
        # Rename .ko.csv to .csv and cleanup empty folders
        for root, dirs, files in os.walk(target_dir, topdown=False):
            for f in files:
                if f.endswith(".ko.csv"):
                    old = os.path.join(root, f)
                    new = os.path.join(root, f.replace(".ko.csv", ".csv"))
                    self.safe_replace(old, new)
            
            # Remove empty folders
            if not os.listdir(root) and root != target_dir:
                try:
                    os.rmdir(root)
                except:
                    pass
