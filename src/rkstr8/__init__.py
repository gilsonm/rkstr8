from rkstr8.domain.platform_initialization import Platform
from rkstr8.domain.workers import Rkstr8PackageResourceCopy
from rkstr8.conf import Config
import os
import tempfile
import functools
import importlib.resources as importlib_resources
import shutil
from pathlib import Path

class ConfigError(Exception): pass
class PlatformError(Exception): pass

# TODO let caller handle config and launch separately
def launch(pipeline_spec_dir: str='', demo: bool=False, dry_run: bool=False, availability_zone: str='',
           key_pair: str='', bucket: str='', repo_root: str=''):
    ''' this function can be imported and used as a library entrypoint

    :param pipeline_spec_dir: the directory containing the pipeline definition
    :param demo: flag to run the packaged demo pipeline to test your environment
    :param dry_run: flag for mostly-side-effect free run helpful to debug config
    :param availability_zone: the availability zone in which to create resources, defaults to pipeline config value
    :param key_pair: the EC2 keypair name to associate with AWS Batch instances, defaults to pipeline config value
    :param bucket: the s3 bucket to store artifacts, defaults to pipeline config value
    '''

    # Copy package data to a tempdir in order to access it via filesystem API as opposed to importlib API
    pkg_data_tmp_dir, cleanup_pkg_data = copy_package_resources(
        prefix=Config.TMP_BUILDDIR_PREFIX,
        pkg_name=Config.RKSTR8_DATA_PKG_NAME,
        to_dir=os.getcwd() if demo else pipeline_spec_dir)

    pipeline_config_file, pipeline_spec_file, pipeline_spec_dir_abs = find_input_files(
        os.path.join(pkg_data_tmp_dir,Config.DEMO_PIPELINE_SPEC_DIR) if demo else pipeline_spec_dir)

    user_args = { 'availability_zone': availability_zone, 'key_pair': key_pair, 'bucket': bucket, 'demo': demo,
                  'dry_run': dry_run, 'repo_root': repo_root}
    config = Config(pipeline_config_file, pipeline_spec_file, pipeline_spec_dir_abs, pkg_data_tmp_dir, user_args)
    try:
        config.merge_sources()
    except Exception as e:
        raise ConfigError('Could not merge config sources') from e

    platform = Platform(config)
    try:
        platform.initialize()
        platform.run()
    except Exception as e:
        raise PlatformError('Error initializing platform or launching pipeline') from e
    finally:
        platform.cleanup() # cleans up artifacts created by the platform
        cleanup_pkg_data() # removes the package_data tempdir


def find_input_files(pipeline_spec_dir):
    pipeline_spec_dir = Path(pipeline_spec_dir).resolve().absolute()
    try:
        # ensure 1 or more files ending in .config.yaml and .spec.yaml, use the first match from glob for each
        pipeline_config_file = next(Path(pipeline_spec_dir).glob('*.config.yaml'))
        pipeline_spec_file = next(Path(pipeline_spec_dir).glob('*.spec.yaml'))
    except StopIteration as e:
        raise ValueError('Could not locate expected config or spec file in pipeline definition dir') from e
    return pipeline_config_file, pipeline_spec_file, pipeline_spec_dir


def copy_package_resources(prefix, pkg_name, to_dir):
    copier = Rkstr8PackageResourceCopy(prefix, pkg_name, to_dir)
    tempdir, cleanup_callback = copier.make_temp_dir()
    copier.copy(tempdir)
    return tempdir, cleanup_callback