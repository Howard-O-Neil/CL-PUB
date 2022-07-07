# Sync from local
aws s3 cp model/ s3://recsys-bucket-1/model/ --recursive

# Download from s3
aws s3 cp s3://recsys-bucket-1/model/ model/ --recursive
