This is a project which contains an AWS lambda function for generating an AWS EMR\
environment when data files are uploaded to a certain directory in an AWS S3 bucket.

You can modify the code for your own needs, including the config_settings.py file\
which contains node creation information.

You need to provide your own creds.py file in the root directory.\
This file must contain your AWS access and secret keys like so:

```angular2html
ACCESS_KEY="actual_access_key"
SECRET_KEY="actual_secret_key"
```

This project also has an AWS CloudWatch configuration and shell scripts to setup\
node monitoring.