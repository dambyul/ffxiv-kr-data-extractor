import re
import os

class CommonUtils:
    @staticmethod
    def normalize_filename(filename):
        """Removes language suffixes like .ja.csv from filenames."""
    @staticmethod
    def is_kr(text):
        if not text: return False
        if text.startswith("_rsv_"): return True
        # Hangul
        return bool(re.search(r'[\uac00-\ud7af]', text))


