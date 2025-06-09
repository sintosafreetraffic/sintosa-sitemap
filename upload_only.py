import subprocess
import os
import shutil
import sys

# Edit these paths if needed:
REPO_PATH = '/Users/saschavanwell/Documents/google_seo/sintosa-sitemap'
NEW_SITEMAP = '/Users/saschavanwell/Documents/google_seo/sitemap.xml'

def safe_run(cmd, **kwargs):
    """Run subprocess command and print output."""
    print(f"→ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    return result

def main():
    # Check if both files/folders exist
    if not os.path.exists(REPO_PATH):
        print(f"❌ ERROR: Repo folder not found: {REPO_PATH}")
        sys.exit(1)
    if not os.path.exists(NEW_SITEMAP):
        print(f"❌ ERROR: New sitemap not found: {NEW_SITEMAP}")
        sys.exit(1)

    # Copy sitemap.xml into repo (overwrite if exists)
    dest_path = os.path.join(REPO_PATH, 'sitemap.xml')
    shutil.copyfile(NEW_SITEMAP, dest_path)
    print(f"✅ Copied new sitemap to repo: {dest_path}")

    # Change to repo directory
    os.chdir(REPO_PATH)

    # Git add
    safe_run(['git', 'add', 'sitemap.xml'])

    # Git commit (handle 'nothing to commit' gracefully)
    commit_result = safe_run(['git', 'commit', '-m', 'Automated sitemap update'])
    if commit_result.returncode == 0:
        print("✅ Git commit succeeded.")
    elif "nothing to commit" in commit_result.stdout or "nothing to commit" in commit_result.stderr:
        print("ℹ️ No changes to commit (sitemap unchanged). Skipping push.")
        return
    else:
        print(f"⚠️ Git commit failed! See above.")
        sys.exit(1)

    # Git push
    push_result = safe_run(['git', 'push'])
    if push_result.returncode == 0:
        print("🚀 Sitemap.xml updated on GitHub Pages!")
    else:
        print(f"❌ Git push failed! See above.")

if __name__ == '__main__':
    main()
