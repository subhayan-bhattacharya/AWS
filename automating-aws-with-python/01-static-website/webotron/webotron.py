from __future__ import annotations
import boto3
import click
import os
import json
from pathlib import Path
import mimetypes

session = boto3.Session(profile_name="subhayan_aws")
resource = session.resource("s3")



def _upload_object_to_s3(object: str, bucket: str, object_type: str, *args, **kwargs ) -> None:
    try:
        if object_type == "file":
            resource.Bucket(bucket).upload_file(Filename=object, Key=os.path.basename(object), *args, **kwargs)
        else:
            for dirs, subdir, files in os.walk(object):
                for file in files:
                    full_file_name = os.path.join(dirs, file)
                    s3_file_name = os.path.join(os.path.basename(dirs), file)
                    resource.Bucket(bucket).upload_file(Filename=full_file_name, Key=s3_file_name)
    except Exception as e:
        raise e


def _upload_object_when_key_available(bucket: str, object: str, key: str) -> None:
    try:
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'
        resource.Bucket(bucket).upload_file(Filename=object, Key=key,
                                            ExtraArgs={"ContentType": content_type})
    except Exception as e:
        raise e


@click.group()
def cli():
    "Uploads websites to AWS"
    pass

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
            _upload_object_to_s3(object, bucket, object_type, ExtraArgs={"ContentType": 'text/html'})
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



if __name__ == "__main__":
    cli()
