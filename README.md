# Dashbase-sink

This repo contains aws lambda function and google cloud function. It can help you to use dashbase to monitor your logs in stackdriver or cloudwatch.

## CloudTrail To Dashbase

We can use lambda function to automatically export cloudTrail logs to dashbase. This is the document to introduce the whole process.

### Architecture

#### CloudTrail

*When activity occurs in your AWS account, that activity is recorded in a CloudTrail event. When you create a trail, you enable ongoing delivery of events as log files to an Amazon S3 bucket that you specify.*

If you wonder about how to create a trail, you can refer to [Creating a Trail](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-create-and-update-a-trail.html).

Services supported: [AWS Service Topics for CloudTrail](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/monitor-cloudtrail-log-files-with-cloudwatch-logs.html)

Services unsupported: [CloudTrail Unsupported Services](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-unsupported-aws-services.html)

#### Lambda function

This function will be triggered when there are new cloudwatch logs. It is mainly used for processing raw logs and send them to dashbase. This function will be detaily described in the following part.

#### Dashbase

It is exactly a database that can store all the logs and index them. We send logs to dashbase using “bulk” api.

#### CloudWatch Logs

If the trigger of lambda function is CloudWatch, we should configure cloudwatch group and log streams before. Then every log that pass the filter will be sent to lambda function.

![img](https://lh3.googleusercontent.com/U7Dp89o-TlNLZNU_e1Jdhx49p4U5EcLNBeeIZmygE8bm4LkmltNOMgtifBnCkStRi9Hb04KegfB5QcmP2d1qbcIXuqklCTexahOmUP0H6TcG8hLzEm_rbbpHPi7qcMG__VFVo1tX)

Here is the diagram of the whole process. We configure trail to export logs to CloudWatch groups. And it will trigger lambda functions and then be sent to dashbase.

### Data flow

Cloudtrail events(or EC2,whatever)->CloudWatch Logs->lambda function->dashbase

The life of a cloudtrail event:

1. **Created** by the aws account activity.
2. Cloudtrail catched them and export events to CloudWatch group.
3. CloudWatch will filter the logs and send them to lambda function.
4. Lambda function will reformat the logs and send them to dashbase using “bulk” api.
5. Data can be seen on dashbase cluster now.

### Lambda function

Two arguments are provided to function which are **event** and **context**. 

**Context**: It provides methods and properties that provide information about the invocation, function, and execution environment. We didn’t use this parameter in this function.

**Event**: It is used to pass event data to the handler. This parameter is usually of the Python dict type.

The **event** type is decided by the trigger. When it is triggered by cloudwatch logs, the event data that is forwarded to function look like:

```
{"awslogs":{"data":"BASE64ENCODED_GZIP_COMPRESSED_DATA"}}
```

So the function will do the following things:

1. Prepare the environment variables(dashbase cluster, index, table, etc).
2. Get the template from dashbase_utils files, and update the remote dashbase cluster template
3. Decode the **event** and get all the logs.
4. Format data for “bulk” request. I send 100 logs per request because sending all logs at one time may cause error.

### Configuration

1. Dashbase cluster configuration

   - First you need to create a dashbase cluster and create a table.
   - As our lambda function is running on aws, we need to expose dashbase port to outside. You can use nodeport to map 7888 port to outside which is used to calling api.

2. CloudWatch log configuration
   - If you already have some active CloudWatch logs groups configured, you can skip this step simply.
   - Click into “CloudTrail”. Create a trail and export  logs to CloudWatch logs group. You can find specific steps [here](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/send-cloudtrail-events-to-cloudwatch-logs.html).

3. Lambda creation
   - Press create lambda
   - Enter your function name, e.g “cloudtrailToDashbase”.
   - Select python3.7 as the programming language.
   - Create a role to let  the function have the basic permissions. You can edit the policy later when you configure your trigger.
   - Edit handler name to “cloudwatchToDashbase.cloudwatchToDashbase” (File name and function name). It specifies the entrypoint in lambda function.
   - Set environment variables
     - ES_HOST => Your elasticsearch cluster host.(format “http://1.1.1.1:1234”)
     - ES_INDEX => Your index in es cluster.
     - ES_SUBTABLE => Your subtable in index.

4. Trigger configuration
   - Click into the lambda function, select “CloudWatch Logs” and add it as your trigger.
   - Select a CloudWatch log group as the input. And create a filter for it. Only logs that pass the filter will be sent to lambda function.

5. Template settings
   - Templates are stored in /PROJECT_ROOT/dashbase_utils/templates/*.py
   - You can add/edit/remove templates as you want. There’re some sample templates and you can modify or edit them. The lambda function will update template to remote cluster automatically when it is called.

6. Deployment
   - Deploy by zip file

     Click into the lambda configuration, and click “upload”. Then you can simply upload the zip file to deploy. Remember the following things:
       1.  Set handler
       2.  Set envrironment variables

     And maybe you will need to modify the templates in **dashbase_utils/templates/**.
   - Deploy by command line
     1. Install virtual environment on your computer. 

       ```shell
       pip install virtualenv
       cd /path/to/project
       virtualenv env
       ```

       Make sure the python version is 3.7

     2. Then you need to make sure that aws-cli has been successfully configured on your computer. Then clone the repo: https://github.com/dashbase/dashbase-sink.git

     3. Run command:

       ```
       sh aws-lambda-deploy.sh
       ```

        This script mainly do the following things:
       - Activate the virtual environment
       - Install all the requirements from requirements.txt
       - Compress all the site-packages and source code into one zip file
       - Upload the whole zip file to aws lambda. You should **change function name and region to your own.**
     4. Back to lambda configuration page, you can set the test event now. Choose event type to “cloudwatch logs” and click test to send test event to lambda.
     5. You can see logs on dashbase website now.



## GoogleCloud Sink To Dashbase

We can not directly invoke google cloud function from a log, but we can send logs to gcs, and then invoke the function(According to [this question](https://stackoverflow.com/questions/50571259/how-to-trigger-google-function-from-stackdriver-logs-similar-to-how-we-do-in-aw)).

### Architecture

#### Sink

Sink is used to filter log entry. Each sink whose filter matches the log entry writes a copy of the log entry to the sink's export destination. You can click [here](https://cloud.google.com/logging/docs/export/configure_export_v2#dest-create) to see how to create a sink and set the properties.

The log entry will be exported to google cloud storage every hour in “json” format. If there are too many logs, they will be separated into small json files(the size can be customized)

![img](https://lh4.googleusercontent.com/Ca9mIGoTs2dv0fLFflBKdfcQFnGEkWlimWT3OIRG_gt3FahXckm7PrEzJkcBov-3Mx3bpFudcPWn9mRY840u0fSe4qfinuz0PTzPloHUpHFDI4DWrciG0RR4V_ZFunh_h7wxvFmM)

#### Google cloud storage

There’s a bucket used to store logs. It will invoke cloud function whenever there’s a new object created in the bucket.

#### Google cloud function

This is a function running on cloud (beta runtime python3.7). When there’s a object created in google cloud storage, the trigger will call this function. 

We do two things in this function:

1. Getting raw data from the storage
2. Send them to dashbase

You can refer to [here](https://cloud.google.com/functions/docs/deploying/console) to see how to create a gcloud function and deploy it on cloud.

#### Dashbase

It is exactly a database that can store all the logs and index them. We send logs to dashbase using “bulk” api.

### Data Flow

The life of a logentry:

1. **Created** by vm, api request or something else.
2. **Filtered** by sink, and if it matches, it will be exported into google cloud storage. The trigger will be called because there’s a new object created in gcs, and then it call gcloud function. The logs will be stored like my-gcs-bucket/syslog/YYYY/MM/DD/ and you can refer to [here](https://cloud.google.com/logging/docs/export/using_exported_logs#gcs_organization) to see more about the log organization.
3. Gcloud function will get the [event](https://cloud.google.com/storage/docs/json_api/v1/objects) object. Then it will fetch raw data from bucket by the file name.
4. Function will reformat the logs and send them to dashbase using “bulk” api.
5. Data can be seen on dashbase cluster now.

### Configuration

1. Bucket configuration

   - You should create a bucket at first which is used to store exported logs. Click *Navigation menu->Storage->Browser->CREATE BUCKET*
   - Then choose the settings you like and create it.

2. Sink configuration

   - Create sink

     1. The filter and destination are held in an object called a **sink.**  You can  create  a sink with rest API. Make sure that you have an ‘Owner’ role or ‘Logging Admin’ role to create sinks. *Refs:*[*https://cloud.google.com/logging/docs/reference/v2/rest/v2/projects.sinks/create*](https://cloud.google.com/logging/docs/reference/v2/rest/v2/projects.sinks/create)

   - Add permission

     1. The sink that we just created may don’t have permission to write objects into the bucket. You should find one who have the **Owner** access to destination, and then add the sink's writer identity to the bucket and give it the [Storage Object Creator](https://cloud.google.com/iam/docs/understanding-roles#cloud_storage_roles) role.

*Refs:* [*https://cloud.google.com/logging/docs/export/configure_export_v2#dest-auth*](https://cloud.google.com/logging/docs/export/configure_export_v2#dest-auth)
3. Dashbase cluster configuration
   - First you need to create a dashbase cluster and create a table.
   - As our lambda function is running on aws, we need to expose dashbase port to outside. You can use nodeport to map 7888 port to outside which is used to calling api.

4. Template settings
   - Templates are stored in /PROJECT_ROOT/dashbase_utils/templates/*.py
   - You can add/edit/remove templates as you want. There’re some sample templates and you can modify or edit them. The lambda function will update template to remote cluster automatically when it is called.

5. Deployment

   - Deploy by zip file

      - Click into the google cloud function configuration, and click “create”. Then you can simply upload the provided zip file to deploy. Remember the following things:

         1. Set handler 
         2. Set environment variables
         

      And if you want to change templates, maybe you should need to modify the templates in dashbase_utils/templates/.
   - Deploy by command line

      - You need to make sure that gcloud and gcloud beta have been successfully configured on your computer. Then clone the repo: https://github.com/dashbase/dashbase-sink.git

      - Set environment variables
        All environment variables are set in file “.env.yaml”. You can edit it manually.

      - You can see there’s a script called “gcloud-funcition-deploy.sh”. You should set the function name、trigger resource to your own names.
        Then simply run command:
        ```
         sh aws-lambda-deploy.sh
        ```
        Back to google cloud console, you can see a new function being created. Then you can test the function by passing the event to function like:

        ```
        {"name":"PATH/TO/BUCKET/FILES"}
        ```

        You can see logs on dashbase website now.



​        

### References

Google cloud storage trigger: https://cloud.google.com/functions/docs/calling/storage?authuser=1

The event object format: https://cloud.google.com/storage/docs/json_api/v1/objects

The log entry format: https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry