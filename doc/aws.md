## Deploying to AWS

The jar executable generated from this project can be deployed to AWS. Follow these steps:
1. Generate jar file for this project: follow [these](./README.md#build-and-run-configuration) steps.
2. Log in to the AWS console.
3. Go to the EC2 web interface and add a key-pair.
4. Go to EMR platform and create a cluster. Add the key-pair created in the previous step
5. Go to S3 storage and create 3 directories:
   1. `src`: upload the created jar here
   2. `input`: upload the data log file here
   3. `output`: empty base directory where EMR will write the output
6. Go back to EMR -> add step.
   1. Add jar S3 URI for the jar
   2. Add arguments as mentioned [here](./README.md#build-and-run-configuration). Input and output paths are the S3 URIs here.

Here is a demo of the above steps:
[youtube video](https://youtu.be/ngMowSsGUvE)