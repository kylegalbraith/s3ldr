# s3ldr
Command-line tool for uploading and downloading content from an Amazon S3 bucket, with features that make working with static hosting website happier.
### Usage
Default behaviour is to upload from the current directory to the destination bucket location

s3ldr *[options][source]* destination_bucket

### Examples
To upload the contents of the current directory to the bucket “www.example.com”:
```
C:\web>s3ldr www.example.com
```
