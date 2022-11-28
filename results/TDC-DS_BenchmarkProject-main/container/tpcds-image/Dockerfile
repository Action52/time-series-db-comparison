# Driver image that implements tpcds tools.
# It also implements spark to be able to submit jobs from this image.
# The image will be able to execute the tpcds-kit commands if accessed.

FROM centos as tpc_builder
# Install required dependencies
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
RUN yum update -y
RUN yum install gcc make flex bison byacc git wget tar -y
RUN yum -y remove java
RUN yum install -y \
       java-1.8.0-openjdk \
       java-1.8.0-openjdk-devel
RUN yum install -y maven
RUN yum install -y curl
RUN yum install -y unzip
# Install spark
RUN wget http://apache.mirror.anlx.net/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
RUN tar -xzf spark-3.3.0-bin-hadoop3.tgz && \
    mv spark-3.3.0-bin-hadoop3 /spark && \
    rm spark-3.3.0-bin-hadoop3.tgz
# Download the tpcds tools / Maybe change for local COPY only.
RUN git clone https://github.com/databricks/tpcds-kit.git
WORKDIR /tpcds-kit/tools
# Run the makefile
RUN make OS=LINUX
# TODO: AT THE END THE CONTAINER SHOULD POINT THE USER TO A
# SCRIPT (separathe bash script) OR NOTEBOOK THAT IS ABLE TO
# A) CREATE THE TABLES
# B) GENERATE THE DATA
RUN yum install -y nano
RUN yum install -y python3
RUN pip3 install pyspark
RUN pip3 install tqdm
WORKDIR /scripts
#COPY tpcds-image/* .
COPY tpcds-image/sparksql.txt /tpcds-kit/query_templates/sparksql.tpl

#RUN mkdir /queries_vol_shared
#COPY tpcds-image/queries_1G_Fixed.sql /queries_vol_shared

#RUN git clone https://github.com/Action52/TDC-DS_BenchmarkProject.git
# WORKDIR TDC-DS_BenchmarkProject
### RUN git fetch && git checkout isa
# RUN python3 script/app_menu.py
# C) INGEST THE DATA
# D) CREATE THE QUERIES
# E) EXECUTE THE QUERIES
    # E.1) INDIVIDUALLY
    # E.2) IN GROUP
# F) RETRIEVE TPC RESULTS
CMD ["/bin/bash"]