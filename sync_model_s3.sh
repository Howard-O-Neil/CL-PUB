# Sync from local
aws s3 cp model/ gs://clpub/model/ --recursive

# Download from s3
aws s3 cp gs://clpub/model/ model/ --recursive
