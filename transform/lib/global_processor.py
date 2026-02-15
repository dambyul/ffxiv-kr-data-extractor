import os
import csv
import re
import stat
from lib.processor import CSVProcessor

class GlobalCSVProcessor(CSVProcessor):
    def __init__(self):
        super().__init__(None) # No RSV for global

    @staticmethod
    def has_target_lang(text):
        if not text: return False
        
        # Strip hex tags to avoid false positives on metadata
        clean_text = re.sub(r'<hex:[^>]+>', '', text)
        
        # Exclude Korean
        if bool(re.search(r'[\uac00-\ud7af]', clean_text)): return False
        
        # Filter technical strings
        lower_text = clean_text.lower().strip()
        if not lower_text: return False
        if lower_text in ['true', 'false']: return False
        if lower_text.startswith('bit&'): return False
        
        # Detect Japanese and English
        if bool(re.search(r'[\u3040-\u30ff\u4e00-\u9faf]', clean_text)): return True
        if bool(re.search(r'[a-zA-Z]', clean_text)): return True
        return False

    def initial_cleanup(self, target_dir):
        # Filter files to prioritize .ja.csv over other languages
        
        for root, _, files in os.walk(target_dir):
            sheets = {}
            for f in files:
                if not f.endswith(".csv"): continue
                
                parts = f.split('.')
                if len(parts) >= 3: # Action.ja.csv -> ['Action', 'ja', 'csv']
                    base = ".".join(parts[:-2])
                    lang = parts[-2]
                else:
                    base = parts[0]
                    lang = None
                
                if base not in sheets: sheets[base] = set()
                sheets[base].add(f)

            for base, file_set in sheets.items():
                ja_file = f"{base}.ja.csv"
                if ja_file in file_set:
                    # Keep ja_file, remove others in this set
                    for f in file_set:
                        if f != ja_file:
                            path = os.path.join(root, f)
                            self.make_writable(path)
                            os.remove(path)
                else:
                    # If no .ja.csv, only keep the base .csv if it exists
                    base_file = f"{base}.csv"
                    for f in file_set:
                        if f != base_file: # Remove other languages (en, de, fr)
                            path = os.path.join(root, f)
                            self.make_writable(path)
                            os.remove(path)

    def filter_columns(self, target_dir):
        # Keep columns containing target languages or specific keywords
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
                    
                    col_indices = {0} # Always keep Key
                    
                    str_col_indices = set()
                    for i, col_type in enumerate(rows[3]):
                        if col_type == 'str':
                            str_col_indices.add(i)

                    # Kepp columns with Name, Description, or Text keywords in headers
                    # Check both row 0 (index markers) and row 1 (labels)
                    for header_row in [rows[0], rows[1]]:
                        for i, col in enumerate(header_row):
                            if i == 0: continue
                            if i not in str_col_indices: continue
                            if 'Name' in col or 'Description' in col or 'Text' in col:
                                col_indices.add(i)
                    
                    # Keep columns containing EN or JP text
                    for row in rows[4:]:
                        for i, cell in enumerate(row):
                            if i in col_indices: continue
                            if self.has_target_lang(cell):
                                col_indices.add(i)
                    
                    sorted_indices = sorted(list(col_indices))
                    new_rows = [[row[i] for i in sorted_indices if i < len(row)] for row in rows]
                    
                    with open(temp, 'w', encoding='utf-8', newline='') as f:
                        csv.writer(f).writerows(new_rows)
                    self.safe_replace(temp, path)

    def remove_empty_rows(self, target_dir):
        # Remove rows without target languages and delete files with only headers
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith(".csv"):
                    path = os.path.join(root, file)
                    temp = path + ".tmp"
                    rows = []
                    with open(path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        for _ in range(4):
                            header = next(reader, None)
                            if header: rows.append(header)
                        
                        for row in reader:
                            # Keep row if it contains target languages
                            if any(self.has_target_lang(cell) for cell in row[1:]):
                                rows.append(row)
                    
                    if len(rows) <= 4: # Delete if only headers remain
                        self.make_writable(path)
                        os.remove(path)
                    else:
                        with open(temp, 'w', encoding='utf-8', newline='') as f:
                            csv.writer(f).writerows(rows)
                        self.safe_replace(temp, path)

    def rename_files(self, target_dir):
        # Rename .ja.csv to .csv
        for root, dirs, files in os.walk(target_dir, topdown=False):
            for f in files:
                if f.endswith(".ja.csv"):
                    old = os.path.join(root, f)
                    new = os.path.join(root, f.replace(".ja.csv", ".csv"))
                    self.safe_replace(old, new)
            
            if not os.listdir(root) and root != target_dir:
                try: os.rmdir(root)
                except: pass
