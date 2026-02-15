import os
import shutil
import sys
import urllib.request
import zipfile
import argparse
import glob

# Constants
SCHEMA_URL = "https://github.com/xivapi/SaintCoinach/archive/refs/heads/master.zip"
EXTRACT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEF_DIR = os.path.join(EXTRACT_ROOT, "SaintCoinach", "Definitions")
BAK_DIR = os.path.join(EXTRACT_ROOT, "SaintCoinach", "Definitions_Bak")
TEMP_DIR = os.path.join(EXTRACT_ROOT, "temp_stcoinach")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--game-path", required=True)
    args = parser.parse_args()

    print("Updating SaintCoinach Definitions (Full Replace)...")

    # Clear existing definitions
    if os.path.exists(DEF_DIR):
        print(f"Clearing existing definitions: {DEF_DIR}")
        for item in os.listdir(DEF_DIR):
            path = os.path.join(DEF_DIR, item)
            try:
                if os.path.isfile(path): os.unlink(path)
                else: shutil.rmtree(path, ignore_errors=True)
            except: pass
    else:
        os.makedirs(DEF_DIR)

    # Download and extract latest global definitions
    if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR, ignore_errors=True)
    os.makedirs(TEMP_DIR)
    
    zip_path = os.path.join(TEMP_DIR, "st.zip")
    print(f"Downloading latest global definitions...")
    try:
        urllib.request.urlretrieve(SCHEMA_URL, zip_path)
        with zipfile.ZipFile(zip_path, 'r') as z: z.extractall(TEMP_DIR)
    except Exception as e:
        print(f"Fetch failed: {e}")
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
        sys.exit(1)

    # Copy extracted definitions
    ext_dirs = glob.glob(os.path.join(TEMP_DIR, "SaintCoinach-*"))
    if ext_dirs:
        glob_dir = os.path.join(ext_dirs[0], "SaintCoinach", "Definitions")
        if os.path.exists(glob_dir):
            count = 0
            for g_file in glob.glob(os.path.join(glob_dir, "*.json")):
                shutil.copy2(g_file, os.path.join(DEF_DIR, os.path.basename(g_file)))
                count += 1
            print(f"Updated {count} definitions from Global.")

    # Update game.ver file
    v_path = os.path.join(args.game_path, "game", "ffxivgame.ver")
    ver = "0000.00.00.0000.0000"
    if os.path.exists(v_path):
        with open(v_path, "r") as f: ver = f.read().strip()
        print(f"Game version found: {ver}")
    else:
        print(f"Warning: ffxivgame.ver not found at {v_path}. Using fallback version.")
    
    with open(os.path.join(DEF_DIR, "game.ver"), "w") as f:
        f.write(ver)

    # Sync to runtime directory
    rt_dir = os.path.join(EXTRACT_ROOT, "SaintCoinach.Cmd", "bin", "Debug", "net7.0", "Definitions")
    if os.path.exists(os.path.dirname(rt_dir)):
        shutil.rmtree(rt_dir, ignore_errors=True)
        try: shutil.copytree(DEF_DIR, rt_dir, dirs_exist_ok=True)
        except: pass

    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print("Update complete.")

if __name__ == "__main__":
    main()
