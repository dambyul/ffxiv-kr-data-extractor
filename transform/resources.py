"""Prepare a resource release locally, or explicitly publish it to S3."""
import argparse
from pathlib import Path
from dotenv import load_dotenv
from lib.resource_release import build_release, package_release, publish_release


if __name__ == '__main__':
    load_dotenv(Path(__file__).resolve().parents[1] / '.env')
    parser = argparse.ArgumentParser()
    parser.add_argument('--package', help='Validate/package an existing exported resource directory')
    parser.add_argument('--output', default='transform/output/resource-release')
    parser.add_argument('--upload', action='store_true', help='Overwrite root resources.zip and update existing data.json')
    parser.add_argument('--text-version', help='Shared CSV version; required when uploading a standalone image release')
    args = parser.parse_args()
    if args.upload and not args.text_version:
        parser.error('--upload requires --text-version matching the published CSV version')
    archive, release = (package_release(args.package, args.output, args.text_version) if args.package
                                else build_release(args.output, text_version=args.text_version))
    print('Archive:', archive)
    print('Release:', release)
    if args.upload:
        from lib.uploader import S3Uploader
        uploader = S3Uploader()
        publish_release(uploader, archive, release, verify_shared_version=True)
