# TPC-DS: Spark SQL Implementation using Databricks AWS
* * *
## Running the pipeline
### Requirements and installation

To use this repo we assume you have at least:
- Python3.6 installed on your computer.
- A Databricks account with:
  - A workspace working with AWS (https://www.databricks.com/product/aws).
  - A Databricks token (https://docs.databricks.com/dev-tools/api/latest/authentication.html).
- An AWS account with:
  - IAM Role set with ses:SendRawEmail policy.
  - A verified SES email receiver.

Please be aware that if you're building the pipeline on your own, you'll have to modify several of the fields in the 
run_tpcds_notebook_on_databricks_job.json accordingly, more specifically the fields pertaining the AWS arns, 

If not, we provide a sample user and credentials on the written report of this project (only available for academic use).

Please install the databricks client on a clean python env in your computer:

```
pip install databricks-cli
```
or
```
sudo -H pip3 install databricks-cli
```


Once installed, assuming you have a databricks user with access data 
(we provide a sample user on the written report of this project), run the command:

```
databricks configure
```

This will create a .cfg file on your root folder containing your access data. Please input the data to configure. As said before,
the access credentials to configure a test user are included in the written report of this project (Anexes section). 
You can also create your own databricks workspace with your account and later on attach the script and notebook to 
run the pipeline.

### Running the pipeline

To run the pipeline, let's first create a job with databricks. 
First, edit the scripts/run_tpcds_notebook_on_databricks.json file to contain your mail in the email_notifications section:

```
  "email_notifications": {
    "on_success": [
      "<YOUR-MAIL-HERE>"
    ],
    "on_failure": [
      "<YOUR-MAIL-HERE>"
    ]
  }
```

```
 "aws_attributes": {
            "first_on_demand": 0,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1f",
            "instance_profile_arn": "<YOUR-INSTANCE-ARN-HERE>"",
            "spot_bid_price_percent": 100,
            "ebs_volume_count": 0
          },

```

Then, also modify the base_parameters attached data to send the report, this includes the email of whoever you want to 
send the report to (possibly a stakeholder or a senior data engineer of your organization wanting to review the results
of your tests), and also the AWS credentials to use SES. Again, we provide these credentials on the written report of this project.
Also consider that this email has to be previously whitelisted on AWS SES.
```
      "base_parameters": {
        "mail_to_send_report": <STAKEHOLDER-EMAIL>,
        "aws_key": <AWS-KEY-TO-USE-SES>,
        "aws_secret": <AWS-SECRET-KEY-TO-USE-SES>
      }
```

```
databricks jobs configure --version=2.1     
databricks jobs create --json-file scripts/run_tpcds_notebook_on_databricks_job.json
```
Please take into account that this json file considers the creation of a single-node cluster since this is for testing purposes.
Feel free to create a bigger cluster if you wish to have the computations working faster (also consider this will be much more costly).
This json file will create a databricks job that will, when triggered, execute the jupyter notebook
with the tpcds pipeline that we have generated. You can also check the notebooks manually at the databricks web UI.  
Upon creation, the CLI will return a "job-id" that we can use to call the job.
You can always check the job-id of your created jobs by doing:

```
databricks jobs list 
```

This json file will create a databricks job that will, when triggered, execute the jupyter notebook
with the tpcds pipeline that we have generated. You can also check the notebooks manually at the databricks web UI.

To trigger the job, run this command:

```
databricks jobs run-now --job-id <JOB-ID>
```

This will trigger the job that will first execute the notebook that performs the tpcds on 4 scale factors: 1G, 2G, 3G, and 4G.
Then another notebook will be executed. This second notebook generates the visualizations of the experiments that were performed on the
first notebook and sends the results to the stakeholder. If correctly executed, the CLI will return a run_id to track the job.
Once completed, databricks will automatically send a notification of completion/failure to you and the report to the stakeholder. 
The report might be considered as "spam" by your email provider so please check that section to look for the report.
You can also check the results by checking the output of the run by:

```
databricks runs get-output --run-id <RUN-ID>
```

Click on the URL, login with your credentials (also provided with the written report), and you will be prompted to the 
notebooks and the results.

