
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
RUN yum install -y nano

RUN yum install -y python3
COPY ./fintime/findata/* /home/benchmark
