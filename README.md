# adaptive
ETL job that pulls data from Adaptive to DW

## Dependencies:
* Python3.7
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)
* [Docker](https://www.docker.com/)

## Setup Environment

### Clone this repo
```
$ git clone https://github.com/kippnorcal/adaptive.git
```

### Create .env file with project secrets
```
# Database connection
DB_SERVER=
DB=
DB_USER=
DB_PWD=
DB_SCHEMA=

# School year settings (MM/DD/YYYY)
# SCHOOL_YEAR_START=

# Account data range (YYYY)
ACCOUNTS_START=
ACCOUNTS_END=

# Personnel data range (Mmm-YYYY)
PERSONNEL_START=
PERSONNEL_END=

# API connection
API_URL=
API_USER=
API_PWD=
CALLER_NAME=
TOP_LEVEL=
VERSION=

# Notification email settings
SENDER_EMAIL=
SENDER_PWD=
RECIPIENT_EMAIL=
EMAIL_SERVER=
EMAIL_PORT= 

```

### Create Adaptive_Levels table with names of levels (taken from Adaptive) and flag to export them or not
```
CREATE TABLE custom.Adaptive_Levels (
    LevelName VARCHAR(50),
    ExportData BIT,
    ExportPersonnel BIT
)
```
ExportData: Export the account data for this level

ExportPersonnel: Export the personnel data for this level


## Running the job

### Build the docker image
```
docker build -t adaptive .
```

### Run the job
```
docker run --rm -it adaptive
```
