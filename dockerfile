# parent image
FROM python:3.9-slim

LABEL PRD="PRD"
WORKDIR /usr/src/app

# install Microsoft SQL Server requirements.
ENV ACCEPT_EULA=Y
RUN apt-get update -y && apt-get update \
  && apt-get install -y --no-install-recommends curl gcc g++ gnupg unixodbc-dev python3-dev unixodbc \
    tzdata

# Add SQL Server ODBC Driver 17 for Ubuntu 18.04
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends --allow-unauthenticated msodbcsql17 mssql-tools \
  && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile \
  && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc


# Set the timzone to EST
RUN cp /usr/share/zoneinfo/US/Eastern /etc/localtime

#Install required python dependencies
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

#Copy application files
COPY ./app .
