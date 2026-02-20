import os
import csv
import re
import stat
import time
import json
import sys

from .common import CommonUtils
from .logging_setup import get_logger

logger = get_logger()

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
        return CommonUtils.is_kr(text)

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

    def apply_manual_filters(self, target_dir, config):
        # Apply manual deletions and remappings from config
        if not config: return

        
        del_files = config.get("delete_files", [])
        del_rows_conf = config.get("delete_rows", {})
        remap_keys_conf = config.get("remap_keys", {})

        for root, _, files in os.walk(target_dir):
            for f in files:
                # File deletion
                base = f.replace(".ko.csv", ".csv")
                if base in del_files or f in del_files:
                    path = os.path.join(root, f)
                    self.make_writable(path)
                    os.remove(path)
                    continue

                # Row operations
                rows_to_del = None
                keys_to_remap = None
                
                for k in del_rows_conf:
                    if k == f or k.replace(".csv", ".ko.csv") == f:
                        rows_to_del = del_rows_conf[k]
                        break
                
                for k in remap_keys_conf:
                    if k == f or k.replace(".csv", ".ko.csv") == f:
                        keys_to_remap = remap_keys_conf[k]
                        break
                
                if rows_to_del or keys_to_remap:
                    self._apply_row_operations(os.path.join(root, f), rows_to_del, keys_to_remap)

    def apply_column_remapping(self, target_dir, config):
        # Apply row-specific column value swaps or literal injections
        if not config: return

        
        remap_cols_conf = config.get("remap_columns", {})
        if not remap_cols_conf: return

        for root, _, files in os.walk(target_dir):
            for f in files:
                if not f.endswith(".ko.csv"): continue
                
                base = f.replace(".ko.csv", ".csv")
                file_remaps = None
                for k in remap_cols_conf:
                    if k == f or k == base:
                        file_remaps = remap_cols_conf[k]
                        break
                
                if file_remaps and isinstance(file_remaps, dict):
                    # Check if it has row-specific mappings (dict vs string handle)
                    has_row_remaps = any(isinstance(v, dict) for v in file_remaps.values())
                    if has_row_remaps:
                        self._apply_col_remaps(os.path.join(root, f), file_remaps)

    def _apply_col_remaps(self, path, file_remaps):
        temp = path + ".tmp"
        rows = []
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        if len(rows) < 4: return
        
        offsets = rows[2]
        # Map offset string to column index
        offset_to_idx = {str(off): i for i, off in enumerate(offsets)}
        
        modified = False
        for r_idx in range(4, len(rows)):
            row = rows[r_idx]
            if not row: continue
            rid = row[0]
            
            row_remap = file_remaps.get(str(rid))
            if not row_remap or not isinstance(row_remap, dict):
                continue
            
            # row_remap maps { Global_Column_Offset: Target_Value_or_Offset }
            # We need to match the Global Offset to the current row's column index.
            
            new_row = list(row)
            for gl_off, mapped_val in row_remap.items():
                if gl_off not in offset_to_idx: continue
                target_idx = offset_to_idx[gl_off]
                
                if isinstance(mapped_val, str):
                    # Literal injection
                    new_row[target_idx] = mapped_val
                    modified = True
                elif isinstance(mapped_val, int):
                    # Column data swap
                    src_off = str(mapped_val)
                    if src_off in offset_to_idx:
                        src_idx = offset_to_idx[src_off]
                        new_row[target_idx] = row[src_idx]
                        modified = True
            
            rows[r_idx] = new_row

        if modified:
            with open(temp, 'w', encoding='utf-8', newline='') as f:
                csv.writer(f).writerows(rows)
            self.safe_replace(temp, path)
            logger.info(f"Applied column remapping to: {path}")

    def _apply_row_operations(self, path, rows_to_del, keys_to_remap):
        # Convert config to strings for comparison
        delete_set = set(str(k) for k in (rows_to_del or []))
        remap_dict = {str(k): str(v) for k, v in (keys_to_remap or {}).items()} # Target: Source
        
        # Build reverse map: Source -> [Targets]
        source_to_targets = {}
        for target, source in remap_dict.items():
            if source not in source_to_targets:
                source_to_targets[source] = []
            source_to_targets[source].append(target)

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
                
                if not row: continue
                current_key = row[0]

                # 1. If this ID is a source for any targets, generate the new rows
                if current_key in source_to_targets:
                    for target_key in source_to_targets[current_key]:
                        new_row = list(row)
                        new_row[0] = target_key
                        rows.append(new_row)
                    modified = True


                # 2. If this ID is explicitly a target of a remap, it will be handled when its source is processed
                if current_key in remap_dict:
                    modified = True
                    continue

                # 3. If key is in delete set, skip
                if current_key in delete_set:
                    modified = True
                    continue
                
                rows.append(row)

        if modified:
            with open(temp, 'w', encoding='utf-8', newline='') as f:
                csv.writer(f).writerows(rows)
            self.safe_replace(temp, path)

    def filter_columns(self, target_dir, config=None):
        # Load explicit configs
        delete_cols_conf = {}
        keep_cols_conf = {}
        if config:
            delete_cols_conf = config.get("delete_columns", {})
            keep_cols_conf = config.get("keep_columns", {})

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
                    
                    remap_cols_conf = config.get("remap_columns", {})
                    
                    # Determine rules for this file
                    base = file.replace(".ko.csv", ".csv")
                    explicit_deletes = set()
                    explicit_keeps = set()
                    
                    for k in delete_cols_conf:
                        if k == file or k == base:
                            explicit_deletes = set(str(c) for c in delete_cols_conf[k])
                            break
                    
                    for k in keep_cols_conf:
                        if k == file or k == base:
                            explicit_keeps = set(str(c) for c in keep_cols_conf[k])
                            break
                    


                    # Identify which columns to keep
                    col_indices = {0} # Always keep Key column (usually # or key)
                    
                    # Scan headers
                    field_names = rows[0] # First line (Field names)
                    offsets = rows[2]    # Third line (Offsets)
                    
                    for i in range(len(field_names)):
                        if i == 0: continue
                        
                        field_val = field_names[i]
                        offset_val = offsets[i]
                        
                        # Rule 1: Always keep if explicitly in keep_columns
                        if offset_val in explicit_keeps or field_val in explicit_keeps or "ALL" in explicit_keeps:
                            col_indices.add(i)
                            continue
                        
                        # Rule 2: Skip if offset or field name is explicitly deleted
                        if offset_val in explicit_deletes or field_val in explicit_deletes:
                            continue
                            
                        # Rule 3: Keep if header has Korean or specific keywords
                        if self.has_korean(field_val) or 'Name' in field_val or 'Description' in field_val:
                            col_indices.add(i)
                    
                    # Scan rows for Korean text (further identification)
                    for row in rows:
                        for i, cell in enumerate(row):
                            if i in col_indices: continue
                            
                            # Skip if explicitly deleted
                            field_val = field_names[i] if i < len(field_names) else ""
                            offset_val = offsets[i] if i < len(offsets) else ""
                            if offset_val in explicit_deletes or field_val in explicit_deletes:
                                continue

                            if self.has_korean(cell):
                                col_indices.add(i)
                    
                    # Sort indices to maintain order
                    sorted_indices = sorted(list(col_indices))
                    
                    # Write cleaned rows
                    new_rows = [[row[i] for i in sorted_indices if i < len(row)] for row in rows]
                    
                    with open(temp, 'w', encoding='utf-8', newline='') as f:
                        csv.writer(f).writerows(new_rows)
                    self.safe_replace(temp, path)

    def remove_empty_rows(self, target_dir, config=None):
        # Remove rows without Korean text and delete empty files
        keep_rows_conf = {}
        keep_cols_conf = {}
        if config:
            keep_rows_conf = config.get("keep_rows", {})
            keep_cols_conf = config.get("keep_columns", {})

        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith(".csv"):
                    path = os.path.join(root, file)
                    
                    # Determine rules
                    base = file.replace(".ko.csv", ".csv")
                    file_keep_rows = set()
                    keep_all_rows = False
                    explicit_keep_cols = set()
                    
                    for k in keep_rows_conf:
                        if k == file or k == base:
                            conf_val = keep_rows_conf[k]
                            if isinstance(conf_val, list) and "ALL" in conf_val:
                                keep_all_rows = True
                            else:
                                file_keep_rows = set(str(rid) for rid in conf_val)
                            break
                    
                    for k in keep_cols_conf:
                        if k == file or k == base:
                            explicit_keep_cols = set(str(c) for c in keep_cols_conf[k])
                            break

                    temp = path + ".tmp"
                    rows = []
                    with open(path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        # Read 4 header lines
                        header_all = []
                        for _ in range(4):
                            h = next(reader, None)
                            if h: header_all.append(h)
                        
                        if not header_all: continue
                        rows.extend(header_all)
                        
                        # Identify indices of columns that should trigger row preservation
                        content_indices = set()
                        field_names = header_all[0]
                        offsets = header_all[2]
                        
                        for i in range(len(field_names)):
                            f_val = field_names[i]
                            o_val = offsets[i]
                            if o_val in explicit_keep_cols or f_val in explicit_keep_cols or "ALL" in explicit_keep_cols:
                                content_indices.add(i)

                        # Filter data rows
                        for row in reader:
                            # Keep row if:
                            # 1. keep_all_rows is True
                            # 2. contains Korean text
                            # 3. is in explicit keep_rows list
                            # 4. has non-empty content in an explicitly preserved column (keep_columns)
                            has_ko = any(self.has_korean(cell) for cell in row[1:])
                            is_kept_id = row and row[0] in file_keep_rows
                            has_preserved_content = any(row[i] for i in content_indices if i < len(row))
                            
                            if keep_all_rows or has_ko or is_kept_id or has_preserved_content:
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
                                    is_unres = self.rsv_manager.is_unresolved(cell)
                                    self.rsv_manager.add_found_file(rel_path, is_unres)
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
