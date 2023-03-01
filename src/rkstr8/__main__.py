from argparse import ArgumentParser
from rkstr8 import launch, ConfigError, PlatformError


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-d', '--pipeline-spec-dir', type=str, required=True, help='Absolute path to pipeline dir')
    parser.add_argument('--demo', action='store_true', default=False, help='Run the packaged demo pipeline to test your environment')
    parser.add_argument('--dry-run', action='store_true', default=False, help='Parse user data, do not create resources')
    parser.add_argument('--availability-zone', '--az', type=str, required=False, help='The availability zone in which to create resources')
    parser.add_argument('--key-pair', '--kp', type=str, required=False, help='The EC2 keypair name to associate with AWS Batch instances')
    parser.add_argument('--bucket', '--s3', type=str, required=False, help='The s3 bucket to store artifacts')
    parser.add_argument('--repo-root', '--rt', type=str, required=False, help='The root of the repo, containing src and build.sh')
    return parser.parse_args()


def cli():
    # this function should not take any arguments in order to be used as an entry point for the setuptools cli
    args = parse_args()
    try:
        launch(args.pipeline_spec_dir, demo=args.demo, dry_run=args.dry_run, availability_zone=args.availability_zone,
               key_pair=args.key_pair, bucket=args.bucket, repo_root=args.repo_root)
    except Exception as e:
        print('Error launching')
        raise e


if __name__ == '__main__':
    cli()