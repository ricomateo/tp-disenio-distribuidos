FROM python:3.9.7-slim
RUN pip install --no-cache-dir pika==1.3.2 pandas
COPY /sentiment /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]
RUN pip install transformers torch
