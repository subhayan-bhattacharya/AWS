from __future__ import annotations
import boto3
import click

session = boto3.Session(profile_name="subhayan_aws")
resource = session.resource("s3")


@click.group()
def cli():
    "Uploads websites to AWS"
    pass

@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket: str) -> None:
    "List the objects in an s3 bucket"
    try:
        for object in resource.Bucket(bucket).objects.all():
            print(object)
    except Exception as e:
        print(e)

@cli.command('list-buckets')
def list_buckets() -> None:

    "Lists all bucket names in an S3 bucket"

    try:
        buckets = [bucket.name for bucket in resource.buckets.all()]
        print(buckets)
    except Exception as e:
        print(e)



if __name__ == "__main__":
    cli()
