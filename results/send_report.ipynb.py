# Databricks notebook source
# MAGIC %md
# MAGIC # Send Report to Stakeholders
# MAGIC 
# MAGIC This notebook retrieves the results of the last run of tpc-ds, generates interesting visualizations, and sends them to the stakeholder to take decisions.
# MAGIC 
# MAGIC ### Visualization and Stats retrieving Functions

# COMMAND ----------

import pandas as pd
from matplotlib import pyplot as plt
import math
from pyspark.sql import types
from pyspark.sql.functions import col

def get_visualization_tables_per_scale(data_sizes = ["1G", "2G", "3G", "4G"]):
    dfs = []
    for i, data_size in enumerate(data_sizes):
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        schema = types.StructType([types.StructField("run_id", types.IntegerType(), True), 
                           types.StructField("query_id", types.IntegerType(), True), 
                           types.StructField("start_time", types.DoubleType(), True),
                           types.StructField("end_time", types.DoubleType(), True),
                           types.StructField("elapsed_time", types.DoubleType(), True),
                           types.StructField("row_count", types.IntegerType(), True),
                           types.StructField("error", types.BooleanType(), True)
                           ])
        df_s = spark.read.option("header", "true").csv(stats_path, schema)
        df_s = df_s.orderBy('query_id')
        df= df_s.toPandas()
        df.drop(columns=['start_time', 'end_time'], inplace=True)
        dfs.append(df)
    return dfs

def get_visualization_tables_per_scale_for_all(    data_sizes = ["1G", "2G", "3G","4G"]):
    dfs=[]
    for i, data_size in enumerate(data_sizes):
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        schema = types.StructType([types.StructField("run_id", types.IntegerType(), True), 
                           types.StructField("query_id", types.IntegerType(), True), 
                           types.StructField("start_time", types.DoubleType(), True),
                           types.StructField("end_time", types.DoubleType(), True),
                           types.StructField("elapsed_time", types.DoubleType(), True),
                           types.StructField("row_count", types.IntegerType(), True),
                           types.StructField("error", types.BooleanType(), True)
                           ])
        df_s = spark.read.option("header", "true").csv(stats_path, schema)
        df_s = df_s.orderBy('query_id')
        df = df_s.toPandas()
        df['scale'] = data_size
        df.drop(columns=['start_time', 'end_time'], inplace=True)
        dfs.append(df)

    df_final=pd.concat(dfs, ignore_index=True)
    return df_final

def runtime_per_query_per_scale(df_final):
    #General plot
    df_final.pivot(index='query_id', columns='scale', values='elapsed_time')
    #Plots per queries
    grouped = df_final.groupby('query_id')
    grouped_names = [(df, name) for (name, df) in grouped]
    return grouped_names
  
def get_visualization_for_overall_stats(data_sizes = ["1G", "2G", "3G","4G"]):
    dfs=[]
    for i, data_size in enumerate(data_sizes):
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        schema = types.StructType([types.StructField("batch_id", types.IntegerType(), True), 
                           types.StructField("create_db_time", types.DoubleType(), True),
                           types.StructField("run_query_time", types.DoubleType(), True)
                           ])
        df_s = spark.read.option("header", "true").csv(stats_path, schema)
        df_s = df_s.orderBy('batch_id')
        df = df_s.toPandas()
        df['scale'] = data_size
        dfs.append(df)

    df_final=pd.concat(dfs, ignore_index=True)

    #General plot
    df_final.pivot(index='batch_id', columns='scale', values='run_query_time')
    #Plots per queries
    grouped = df_final.groupby('batch_id')
    grouped_names = [(df, name) for (name, df) in grouped]
    return grouped_names

# COMMAND ----------

from pyspark.sql import types
from pyspark.sql.functions import col

def retrieve_overall_stats(data_sizes = ["1G", "2G", "3G", "4G"]):
    dfs=[]
    for i, data_size in enumerate(data_sizes):
        stats_path = "s3a://tpcds-spark/results/{size}/overall_stats_csv".format(size=data_size)
        schema = types.StructType([
                               types.StructField("batch_id", types.IntegerType(), True), 
                               types.StructField("create_db_time", types.DoubleType(), True),
                               types.StructField("run_query_time", types.DoubleType(), True)
                               ])
        df_s = spark.read.option("header", "true").csv(stats_path, schema)
        df = df_s.toPandas()
        df['scale'] = data_size
        dfs.append(df)
    final_df = pd.concat(dfs)
    return final_df
  
def retrieve_stats(data_sizes = ["1G", "2G", "3G", "4G"]):
    dfs=[]
    for i, data_size in enumerate(data_sizes):
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        schema = types.StructType([types.StructField("run_id", types.IntegerType(), True), 
                               types.StructField("query_id", types.IntegerType(), True), 
                               types.StructField("start_time", types.DoubleType(), True),
                               types.StructField("end_time", types.DoubleType(), True),
                               types.StructField("elapsed_time", types.DoubleType(), True),
                               types.StructField("row_count", types.IntegerType(), True),
                               types.StructField("error", types.BooleanType(), True)
                               ])
        df_s = spark.read.option("header", "true").csv(stats_path, schema)
        df_s = df_s.orderBy('query_id')
        df = df_s.toPandas()
        df.drop(columns=['start_time', 'end_time'], inplace=True)
        df['scale'] = data_size
        dfs.append(df)
    return dfs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Methods to parse the images and send an email

# COMMAND ----------

# Code snippet extracted from https://docs.databricks.com/_static/notebooks/kb/notebooks/send-email-aws.html
import boto3

import matplotlib.pyplot as plt
from email import encoders
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from uuid import uuid4
import datetime
import base64
import glob

def send_email(from_email, to_email, subject, body_html, attachments=[], cc=[], bcc=[], access_key=None, secret_key=None):
    attachment_ready_html = []
    img_id = 0
    mime_images = []
    # iterate over raw HTML
    for l in body_html:
        # For each image in the body_html, convert each to a base64 encoded inline image
        if l.startswith("<img"):
            image_data = l[len("<img src='data:image/png;base64,"):-2]
            mime_img = MIMEImage(base64.standard_b64decode(image_data))
            mime_img.add_header('Content-ID', '<img-%d>' % img_id)
            attachment_ready_html.append("<center><img src='cid:img-%d'></center>" % img_id)
            img_id += 1
            mime_images.append(mime_img)
        else:
            attachment_ready_html.append(l)
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    body = MIMEText('\n'.join(attachment_ready_html), 'html')
  
    for i in mime_images:
        msg.attach(i)
    msg.attach(body)
  
    for raw_attachment in attachments:
        attachment = MIMEApplication(open(raw_attachment, 'rb').read())
        attachment.add_header('Content-Disposition', 'attachment', filename=raw_attachment)
        msg.attach(attachment)
  
    ses = boto3.client('ses', 
                       region_name='us-east-1', 
                       aws_access_key_id=access_key,
                       aws_secret_access_key=secret_key
                      )
    ses.send_raw_email(
        Source=msg['FROM'],
        Destinations=[to_email],
        RawMessage={'Data': msg.as_string()})    
    print("Sending Email.")

    
def makeCompatibleImage(image, withLabel=False):
    files = glob.glob('/tmp/*.png')
    for f in files:
        os.remove(f)
    if withLabel:
        image.figure.suptitle("Generated {}".format(datetime.datetime.today().strftime("%Y-%m-%d")))
    imageName = "/tmp/{}.png".format(uuid4())
    image.figure.savefig(imageName)
    plt.close(image.figure)
    val = None
    with open(imageName, 'rb') as png:
        encoded = base64.b64encode(png.read())
        contents = encoded.decode('utf-8')
        val = "<img src='data:image/png;base64,%s'>" % (contents)
    displayHTML(val)
    return val

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parsing the visualizations and generating the email

# COMMAND ----------

import io
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from uuid import uuid4
import datetime
import base64

mail_to_send_results_to = "luis.leon.villapun@ulb.be"#dbutils.widgets.get("mail_to_send_report")

def create_mail_images(data_sizes = ["1G", "2G", "3G", "4G"]):
    images = {}
    images['per_scale_factor'] = []
    images['all_scale_factors'] = None
    images['per_query'] = []
    img_format = 'png'
    dfs_per_scale_factor = get_visualization_tables_per_scale()
    for df, data_size in zip(dfs_per_scale_factor, data_sizes):
        df_plt = df.plot(x="query_id", y="elapsed_time", kind="bar", figsize=(15,7), title=f"Query runtime for scale factor {data_size}")
        images['per_scale_factor'].append(makeCompatibleImage(df_plt))
    df_all = get_visualization_tables_per_scale_for_all()
    df_all_plt = df_all.pivot(
        index='query_id', columns='scale', values='elapsed_time').plot(kind='bar', rot=0, figsize=(15,7), title="Query runtimes across scale factors")
    images['all_scale_factors'] = makeCompatibleImage(df_all_plt)
    runtimes_per_query_axs_names = runtime_per_query_per_scale(df_all)
    for run_time_df, name in runtimes_per_query_axs_names:
        run_time_plot = run_time_df.plot(x='scale',y='elapsed_time', title=str(name), figsize=(5, 2))
        images['per_query'].append(makeCompatibleImage(run_time_plot))
    return images

# COMMAND ----------

import numpy as np
def create_mail_content():
    html = [
      "<center><h1>TPC-DS Report</h1></center>",
      """
      <h1>This report was generated after user request.</h1>
      <h3>Runtimes by scale factor</h3>
      """
    ]
    imgs = create_mail_images()
    html.extend([
        img for img in imgs['per_scale_factor']
    ])
    html.append("""
        <h3>Comparing the overall runtimes</h3>
    """)
    html.append(imgs['all_scale_factors'])
    html.append("""
        <h3>Query by query runtime</h3>
        <table class="tg">
        <tbody>
    """)
    imgs['per_query'] = np.reshape(imgs['per_query'], (-1, 3))
    for i, _ in enumerate(imgs['per_query']):
        html.append("<tr>")
        for j, _ in enumerate(imgs['per_query'][i]):
            html.append("<td>")
            html.append(imgs['per_query'][i][j])
            html.append("</td>")
        html.append("</tr>")
    html.append("</tbody></table>")
    html.append("<h3>Overall Stats</h3>")
    overall_stats = retrieve_overall_stats()
    html.append("""
    <table>
        <thead>
            <tr>
                <th>batch_id</th>
                <th>create_db_time</th>
                <th>run_query_time</th>
                <th>scale</th>
            </tr>
        </thead>
        <tbody>
    """)
    for idx, row in overall_stats.iterrows():
        html.append("<tr>")
        for column in overall_stats.columns:
            html.append(f"<td>{row[column]}</td>")
        html.append("</tr>")
    html.append("</tbody></table>")
    html.append("<h3>All stats displayed</h3>")
    stats = retrieve_stats()
    for stat in stats:
        html.append("""
        <table>
            <thead>
              <tr>
                <th>query_id</th>
                <th>elapsed_time</th>
                <th>row_count</th>
                <th>error</th>
                <th>run_id</th>
                <th>scale</th>
              </tr>
            </thead>
            <tbody>
        """)
        for idx, row in stat.iterrows():
            html.append("<tr>")
            for column in stat.columns:
                html.append(f"<td>{row[column]}</td>")
            html.append("</tr>")
        html.append("</tbody></table>")
    return html
#send_email("luis.leon.villapun@ulb.be", "luis.leon.villapun@ulb.be", "Test", body_html=html)

# COMMAND ----------

def previewHTMLEmail(html):
    displayHTML("\n".join([x for x in html if type(x) != type(type)]))
html = create_mail_content()

# COMMAND ----------

previewHTMLEmail(html)  # Display a preview of the html content that will be sent as an email.
try:
    to_mail = dbutils.widgets.get("mail_to_send_report")
    aws_secret = dbutils.widgets.get("aws_secret")
    aws_key = dbutils.widgets.get("aws_key")
except Exception as e:
    to_mail = "satria.wicaksono@ulb.be"
    aws_secret = None
    aws_key = None
send_email("luis.leon.villapun@ulb.be", to_mail, "TPC-DS-Report", body_html=html, access_key=aws_key, secret_key=aws_secret)

# COMMAND ----------


