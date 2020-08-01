# adaptive_connector
ETL job that pulls data from Adaptive to DW

## Dependencies:
* Python3.7
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)
* [Docker](https://www.docker.com/)

## Setup Environment

### Clone this repo
```
$ git clone https://github.com/kipp-bayarea/deanslist_connector.git
```

### Create .env file with project secrets
```
# Database connection
DB_SERVER=
DB=
DB_USER=
DB_PWD=
DB_SCHEMA=

# API connection
API_URL=
API_USER=
API_PWD=
CALLER_NAME=
TOP_LEVEL=

# Data filter
START_YEAR=
END_YEAR=

# Notification email settings
GMAIL_USER=
GMAIL_PWD=
SLACK_EMAIL=
```

## Running the job

### Build the docker image
```
docker build -t adaptive_connector .
```

### Run the job
```
docker run --rm -it adaptive_connector --version "version name"
```

### Dev command
```
docker run --rm -it --network host adaptive_connector --version "Data Team Sandbox"
```