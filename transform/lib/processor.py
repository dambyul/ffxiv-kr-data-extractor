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
        self.anonymized_ids = {} # {rel_path: set(row_ids)}

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
                path = os.path.join(root, f)
                rel_path = os.path.relpath(path, target_dir).replace('\\', '/')
                base_rel_path = rel_path.replace(".ko.csv", ".csv")

                # File deletion
                is_deleted = False
                if rel_path in del_files or base_rel_path in del_files:
                    is_deleted = True
                else:
                    # Support folder-level deletion (e.g., "transport/")
                    for d in del_files:
                        if d.endswith('/') and (rel_path.startswith(d) or base_rel_path.startswith(d)):
                            is_deleted = True
                            break
                
                if is_deleted:
                    path = os.path.join(root, f)
                    self.make_writable(path)
                    os.remove(path)
                    continue

                # Row operations
                rows_to_del = del_rows_conf.get(rel_path) or del_rows_conf.get(base_rel_path)
                keys_to_remap = remap_keys_conf.get(rel_path) or remap_keys_conf.get(base_rel_path)
                
                # Try folder-level row config if needed (though rarer)
                if not rows_to_del or not keys_to_remap:
                    for k in sorted(del_rows_conf.keys(), key=len, reverse=True):
                        if k.endswith('/') and (rel_path.startswith(k) or base_rel_path.startswith(k)):
                            rows_to_del = del_rows_conf[k]
                            break
                    for k in sorted(remap_keys_conf.keys(), key=len, reverse=True):
                        if k.endswith('/') and (rel_path.startswith(k) or base_rel_path.startswith(k)):
                            keys_to_remap = remap_keys_conf.get(k)
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
                
                path = os.path.join(root, f)
                rel_path = os.path.relpath(path, target_dir).replace('\\', '/')
                base_rel_path = rel_path.replace(".ko.csv", ".csv")

                file_remaps = remap_cols_conf.get(rel_path) or remap_cols_conf.get(base_rel_path)
                
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
        # Handle global remapping if "*" is present
        global_remap = file_remaps.get("*")

        for r_idx in range(4, len(rows)):
            row = rows[r_idx]
            if not row: continue
            rid = row[0]
            
            row_remap = file_remaps.get(str(rid))
            # Merge with global remap if exists (specific row remap takes priority)
            effective_remap = dict(global_remap) if global_remap and isinstance(global_remap, dict) else {}
            if row_remap and isinstance(row_remap, dict):
                effective_remap.update(row_remap)
            
            if not effective_remap:
                continue
            
            new_row = list(row)
            for gl_off, mapped_val in effective_remap.items():
                if gl_off not in offset_to_idx: continue
                target_idx = offset_to_idx[gl_off]
                
                if isinstance(mapped_val, str):
                    # Literal injection or placeholder substitution
                    if "{" in mapped_val and "}" in mapped_val:
                        # Substitute {offset} with actual column value
                        updated_val = mapped_val
                        placeholders = re.findall(r'\{(\d+)\}', mapped_val)
                        for ph_off in placeholders:
                            if ph_off in offset_to_idx:
                                val_idx = offset_to_idx[ph_off]
                                updated_val = updated_val.replace(f"{{{ph_off}}}", row[val_idx])
                        new_row[target_idx] = updated_val
                    else:
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

    def anonymize_chat_phrases(self, target_dir):
        # Automatically find and anonymize chat quest phrases to "/"
        quest_dir = os.path.join(target_dir, "quest")
        if not os.path.exists(quest_dir): return

        quote_regex = re.compile(r'["\']([^"\']+)["\']')
        hex_regex = re.compile(r'<hex:[A-F0-9]+>')

        for root, _, files in os.walk(quest_dir):
            for f in files:
                if not f.endswith(".ko.csv"): continue
                path = os.path.join(root, f)
                rel_path = os.path.relpath(path, target_dir).replace('\\', '/')
                
                # Robust content reading (UTF8/UTF16)
                content = None
                try:
                    with open(path, 'r', encoding='utf-8-sig') as file:
                        content = file.read()
                except UnicodeDecodeError:
                    try:
                        with open(path, 'r', encoding='utf-16') as file:
                            content = file.read()
                    except: continue
                
                if not content or "말하기" not in content: continue

                import io
                f_io = io.StringIO(content)
                reader = csv.reader(f_io)
                try:
                    rows = list(reader)
                except: continue
                
                if len(rows) < 5: continue
                
                def clean_for_match(s):
                    # Remove hex tags, quotes, and common punctuation at ends
                    s = hex_regex.sub('', s).strip()
                    s = s.strip('"\'').strip()
                    # Strip common trailing punctuation often found in instructions but not target rows
                    s = s.rstrip('.?!,').strip()
                    return s

                file_anonymized_ids = set()
                modified = False
                
                # PHASE 1: Collect ALL standalone phrases in the file (potential targets)
                # A phrase is a candidate if it's longer than 1 char and not "말하기"
                candidates = {} # clean_text -> list of row indices
                for j, crow in enumerate(rows):
                    if j < 4 or len(crow) < 3: continue
                    ctext = crow[2]
                    if not ctext: continue
                    clean_t = clean_for_match(ctext)
                    if len(clean_t) > 1 and clean_t != "말하기":
                        if clean_t not in candidates:
                            candidates[clean_t] = []
                        candidates[clean_t].append(j)

                # PHASE 2: Process "Say" instructions using hints verified by candidates
                if candidates:
                    # Regex for finding quoted strings
                    quote_regex = re.compile(r'["\'](.*?)["\']')
                    
                    for i, row in enumerate(rows):
                        if i < 4 or len(row) < 3: continue
                        text = row[2]
                        if "대화창" in text and "'말하기'" in text:
                            # 1. Find the earliest anchor to define the suffix
                            split_idx = -1
                            for anchor in ["키보드로", "가상 키보드로", "방식으로"]:
                                idx = text.find(anchor)
                                if idx != -1:
                                    split_idx = idx + len(anchor)
                                    break 
                            
                            prefix = text[:split_idx] if split_idx != -1 else ""
                            suffix = text[split_idx:] if split_idx != -1 else text
                            
                            file_already_modified = False
                            collected_originals = []
                            
                            # 2. Extract potential hints (quoted strings) from the suffix
                            found_hints = quote_regex.findall(suffix)
                            if not found_hints: continue
                            
                            for hint in set(found_hints):
                                if hint == "말하기": continue
                                cleaned_hint = clean_for_match(hint)
                                
                                if cleaned_hint in candidates:
                                    # SUCCESS: The hint in the instruction matches a standalone row
                                    # Scrub instruction suffix (preserve quotes style) - Now using 'r'
                                    new_suffix = suffix.replace(f'"{hint}"', '"r"').replace(f"'{hint}'", '"r"')
                                    if new_suffix == suffix: # Fallback if no quotes found around it
                                         new_suffix = suffix.replace(hint, "r")
                                    
                                    if new_suffix != suffix:
                                        suffix = new_suffix
                                        collected_originals.append(hint)
                                        # Scrub all matching standalone target rows to 'r'
                                        for idx in candidates[cleaned_hint]:
                                            rows[idx][2] = "r"
                                            file_anonymized_ids.add(str(rows[idx][0]))
                                        file_already_modified = True
                                        modified = True

                            if file_already_modified:
                                final_text = prefix + suffix
                                if collected_originals:
                                    # Append original text reference in (phrase) format
                                    ref_text = "".join([f"({h})" for h in collected_originals])
                                    final_text += ref_text
                                rows[i][2] = final_text.strip()
                                file_anonymized_ids.add(str(row[0]))

                if modified:
                    if file_anonymized_ids:
                        self.anonymized_ids[rel_path] = file_anonymized_ids
                        self.anonymized_ids[rel_path.replace(".ko.csv", ".csv")] = file_anonymized_ids
                    temp = path + ".tmp"
                    with open(temp, 'w', encoding='utf-8', newline='') as file:
                        csv.writer(file).writerows(rows)
                    self.safe_replace(temp, path)
                    logger.info(f"Anonymized chat phrases in: {os.path.relpath(path, target_dir)}")

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
                    rel_path = os.path.relpath(path, target_dir).replace('\\', '/')
                    base_rel_path = rel_path.replace(".ko.csv", ".csv")

                    explicit_deletes = set(str(c) for c in (delete_cols_conf.get(rel_path) or delete_cols_conf.get(base_rel_path) or []))
                    explicit_keeps = set(str(c) for c in (keep_cols_conf.get(rel_path) or keep_cols_conf.get(base_rel_path) or []))
                    


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
                    rel_path = os.path.relpath(path, target_dir).replace('\\', '/')
                    base_rel_path = rel_path.replace(".ko.csv", ".csv")

                    file_keep_rows = set()
                    keep_all_rows = False
                    
                    conf_val = keep_rows_conf.get(rel_path) or keep_rows_conf.get(base_rel_path)
                    if conf_val:
                        if isinstance(conf_val, list) and "ALL" in conf_val:
                            keep_all_rows = True
                        else:
                            file_keep_rows = set(str(rid) for rid in conf_val)

                    explicit_keep_cols = set(str(c) for c in (keep_cols_conf.get(rel_path) or keep_cols_conf.get(base_rel_path) or []))

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
                        file_anon_ids = self.anonymized_ids.get(rel_path) or self.anonymized_ids.get(base_rel_path) or set()
                        for row in reader:
                            # Keep row if:
                            # 1. keep_all_rows is True
                            # 2. contains Korean text
                            # 3. is in explicit keep_rows list or was anonymized
                            # 4. has non-empty content in an explicitly preserved column (keep_columns)
                            has_ko = any(self.has_korean(cell) for cell in row[1:])
                            is_kept_id = row and (row[0] in file_keep_rows or row[0] in file_anon_ids)
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
