FROM python:3.9.7-slim
RUN pip install --no-cache-dir pika==1.3.2 orjson
COPY /gateway /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]