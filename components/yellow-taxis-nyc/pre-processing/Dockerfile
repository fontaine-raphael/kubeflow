FROM google/cloud-sdk:latest

RUN apt-get update -y && apt-get install --no-install-recommends -y -q ca-certificates python3-dev python3-setuptools python3-pip

COPY requirements.txt ./
COPY ./src /src

RUN python3 -m pip install -r requirements.txt
