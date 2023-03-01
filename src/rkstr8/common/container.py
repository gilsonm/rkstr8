from pathlib import Path
from urllib.parse import urlparse, urlunparse


class S3UriManager:

    def __init__(self, s3_base_uri):
        self.s3_base_uri = s3_base_uri

    def __extend_base_uri(self, path_components):
        base = Path(urlparse(self.s3_base_uri).path.strip('/'))

        result = base
        for p in path_components:
            result = result / p # this is no-op if p == ''

        bucket = urlparse(self.s3_base_uri).netloc
        result_uri = urlunparse(('s3', bucket, result.as_posix(), None, None, None))
        return result_uri

    def uri_for_asset(self, file_name):
        return self.__extend_base_uri(['assets', file_name])

    def uri_for_metadata(self):
        return self.__extend_base_uri(['metadata', 'metadata.yaml'])

    def uri_for_log(self, stage_name, attempt_idx, array_idx=None):
        array_marker = 'single' if not array_idx else str(array_idx)
        return self.__extend_base_uri(['logs', '{stage}.{array}.{attempt}.log'.format(stage=stage_name, array=array_marker, attempt=attempt_idx)])

    def uri_for_event(self, label):
        return self.__extend_base_uri(['metadata', 'event.{label}.yaml'.format(label=label)])

    def uri_for_result(self, stage_name, result_name, worker_id, local_path):
        worker_id_or_empty = worker_id if worker_id else ''
        file_name = Path(local_path).name
        return self.__extend_base_uri(['results', stage_name, result_name, worker_id_or_empty, file_name])


class S3Transport:

    def __init__(self, s3_resource):
        self.s3_resource = s3_resource

    def __bucket_for_uri(self, s3_uri):
        return self.s3_resource.Bucket(urlparse(s3_uri).netloc)

    def __key_for_uri(self, s3_uri):
        return urlparse(s3_uri).path.strip('/')

    def __bucket_and_key(self, s3_uri):
        bucket = self.__bucket_for_uri(s3_uri)
        key = self.__key_for_uri(s3_uri)
        return bucket, key

    def __ensure_local_path_dirs(self, local_path):
        parent_path = Path(local_path).parent
        parent_path.mkdir(parents=True, exist_ok=True)

    def upload(self, local_path, s3_uri):
        bucket, key = self.__bucket_and_key(s3_uri)
        bucket.upload_file(local_path, key)

    def download(self, s3_uri, local_path):
        self.__ensure_local_path_dirs(local_path)
        bucket, key = self.__bucket_and_key(s3_uri)
        bucket.download_file(key, local_path)
