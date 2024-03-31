FROM mageai/mageai:latest
ARG PIP=pip3

# Add Debian Bullseye repository
RUN echo 'deb http://deb.debian.org/debian bullseye main' > /etc/apt/sources.list.d/bullseye.list

# Install OpenJDK 11
RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk

# Install wget
RUN apt-get install -y wget

# Download PostgreSQL JDBC driver
RUN wget -P /usr/share/java/ https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

# Remove Debian Bullseye repository
RUN rm /etc/apt/sources.list.d/bullseye.list

# Install PySpark
RUN ${PIP} install pyspark

ENV MAGE_DATA_DIR=/path/to/data/directory
