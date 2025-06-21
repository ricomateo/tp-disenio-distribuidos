FROM python:3.9.7-slim
COPY /leader_election /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]
