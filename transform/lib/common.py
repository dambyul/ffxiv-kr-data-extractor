import re
import os

class CommonUtils:
    # Pre-compiled regex patterns for performance
    _RE_LANG_SUFFIX = re.compile(r'\.(ja|ko|en|de|fr)\.csv$')
    _RE_HANGUL = re.compile(r'[\uac00-\ud7af]')
    _RE_JA = re.compile(r'[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9faf]')
    _RE_OTHER = re.compile(r'[a-zA-Z0-9\U00010000-\U0010ffff]')

    @staticmethod
    def normalize_filename(filename):
        """Removes language suffixes like .ja.csv from filenames."""
        return CommonUtils._RE_LANG_SUFFIX.sub('.csv', filename)

    @staticmethod
    def is_kr(text):
        if not text: return False
        if text.startswith("_rsv_"): return True
        return bool(CommonUtils._RE_HANGUL.search(text))

    @staticmethod
    def is_ja(text):
        if not text: return False
        return bool(CommonUtils._RE_JA.search(text))

    @staticmethod
    def is_empty(text):
        if not text: return True
        return text.strip() == ""

    @staticmethod
    def is_other(text):
        """Checks if text contains non-Japanese/non-Korean meaningful characters (EN, Emoji, etc.)"""
        if not text: return False
        if CommonUtils.is_kr(text) or CommonUtils.is_ja(text): return False
        return bool(CommonUtils._RE_OTHER.search(text))


