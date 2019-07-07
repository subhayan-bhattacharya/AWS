from __future__ import annotations

import json
import mimetypes
import os
from pathlib import Path
from pprint import pprint
from hashlib import md5

import boto3
import click
import utils
from functools import reduce

session = None
resource = None
manifest = {}
CHUNK_SIZE = 8388608
transfer_config = None


def load_manifest(bucket):
    paginator = resource.meta.client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            manifest[obj['Key']] = obj['ETag']
    pprint(manifest)


def hash_data(data):
    hash = md5()
    hash.update(data)
    return hash


def gen_etag(file):
    hashes = []
    with open(file, 'rb') as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            hashes.append(hash_data(data))
    if not hashes:
        print("Does it get here for anyone??")
        return
    elif len(hashes) == 1:
        return f'"{hashes[0].hexdigest()}"'
    else:
        hash = hash_data(reduce(lambda x, y: x + y, (h.digest() for h in hashes)))
        return f'"{hash.hexdigest()}-{len(hashes)}"'



def _upload_object_to_s3(object: str, bucket: str, object_type: str, *args, **kwargs) -> None:
    try:
        if object_type == "file":
            resource.Bucket(bucket).upload_file(
                Filename=object, Key=os.path.basename(object), *args, **kwargs)
        else:
            for dirs, subdir, files in os.walk(object):
                for file in files:
                    full_file_name = os.path.join(dirs, file)
                    s3_file_name = os.path.join(os.path.basename(dirs), file)
                    resource.Bucket(bucket).upload_file(
                        Filename=full_file_name, Key=s3_file_name)
    except Exception as e:
        raise e


def _upload_object_when_key_available(bucket: str, object: str, key: str) -> None:
    print(f"Trying to upload {object} and key value is {key}")
    try:
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'
        etag = gen_etag(object)
        print(f"The etag generated fpr the {key} is {etag}")
        if manifest.get(key, "") == etag:
            print(f"Skipping the key {key} since the etags match")
            return
        resource.Bucket(bucket).upload_file(Filename=object, Key=key,
                                            ExtraArgs={"ContentType": content_type},
                                            Config=transfer_config)
    except Exception as e:
        raise e


def get_region_name(bucket):
    bucket_location =  resource.meta.client.get_bucket_location(Bucket=resource.Bucket(bucket).name)
    return bucket_location["LocationConstraint"] or 'us-east-1'


def get_bucket_url(bucket):
    """Get the website URL for this bucket."""
    return f"http://{resource.Bucket(bucket).name}.{utils.get_endpoint(get_region_name(bucket))}"


@click.group()
@click.option('--profile', default=None)
def cli(profile):
    "Uploads websites to AWS"
    global resource, session
    session_cfg = {}
    if profile:
        session_cfg['profile_name'] = profile
    session = boto3.Session(**session_cfg)
    resource = session.resource("s3")
    transfer_config = boto3.s3.transfer.TransferConfig(
        multipart_chunksize = CHUNK_SIZE,
        multipart_threshold = CHUNK_SIZE
    )


@cli.command('enable-website-on-bucket')
@click.argument('bucket')
def enable_website_on_bucket(bucket: str) -> None:
    "Enable web site hosting on the bucket"
    configuration = {
        'ErrorDocument': {
            'Key': 'error.html'
        },
        'IndexDocument': {
            'Suffix': 'index.html'
        }
    }
    try:
        website_config = resource.Bucket(bucket).Website()
        website_config.put(WebsiteConfiguration=configuration)
    except Exception as e:
        print("Could not enable website configuration in the bucket:", e)


@cli.command('make-bucket-public')
@click.argument('bucket')
def make_bucket_public(bucket: str) -> None:
    "Makes a bucket public by attaching a policy object to it"
    try:
        policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket}/*"
                             ]
            }
            ]
        }
        policy_obj = resource.Bucket(bucket).Policy()
        policy_obj.put(Policy=json.dumps(policy))
    except Exception as e:
        print("Something went wrong while updating the bucket policy: ", e)


@cli.command('create-s3-bucket')
@click.argument('bucket')
def create_s3_bucket(bucket: str) -> None:
    "Creates an s3 bucket"
    try:
        resource.create_bucket(Bucket=bucket, CreateBucketConfiguration={
            'LocationConstraint': session.region_name})
    except Exception as e:
        print("Could not create bucket: ", e)


@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket: str) -> None:
    "List the objects in an s3 bucket"
    try:
        for object in resource.Bucket(bucket).objects.all():
            print(object)
    except Exception as e:
        print("Something went wrong with listing the bucket:", e)


@cli.command('upload-object-to-bucket')
@click.argument('bucket')
@click.argument('object')
@click.argument('object_type')
def upload_object_to_bucket(bucket: str, object: str, object_type: str):
    "Upload an object or a dictionary to s3 bucket"
    if object_type not in ["dir", "file"]:
        raise ValueError(f"The object type {object_type} is not recognized")
    if object_type == "file":
        if not os.path.isfile(object):
            raise FileNotFoundError(f"The object {object} cannot be found")
    else:
        if not os.path.isdir(object):
            raise NotADirectoryError(f"The object {object} is not a directory")
    try:
        if object_type == "file":
            _upload_object_to_s3(object, bucket, object_type, ExtraArgs={
                                 "ContentType": 'text/html'})
        else:
            _upload_object_to_s3(object, bucket, object_type)
    except Exception as e:
        print("Something went wrong in uploading objects to s3:", e)


@cli.command('list-buckets')
def list_buckets() -> None:
    "Lists all bucket names in an S3 bucket"

    try:
        buckets = [bucket.name for bucket in resource.buckets.all()]
        print(buckets)
    except Exception as e:
        print(e)


@cli.command('sync-dir')
@click.argument('bucket')
@click.argument('pathname', type=click.Path(exists=True))
def sync_dir(bucket: str, pathname: str) -> None:
    "Sync contents of pathname to bucket... very similar to the other command which uploads a file/dir to s3"
    load_manifest(bucket)
    root = Path(pathname).expanduser().resolve()

    def handle_dir(target):
        for p in target.iterdir():
            if p.is_dir():
                handle_dir(p)
            if p.is_file():
                try:
                    object = str(p.resolve())
                    key = str(p.relative_to(root))
                    _upload_object_when_key_available(bucket, object, key)
                except Exception as e:
                    print(f"Could not upload {p}:", e)
    handle_dir(root)
    print(get_bucket_url(bucket))

if __name__ == "__main__":
    cli()
