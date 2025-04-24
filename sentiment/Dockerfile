FROM python:3.9.7-slim-bullseye

RUN pip install --no-cache-dir \
<<<<<<< HEAD
    torch \ 
    orjson \
    huggingface_hub \
    transformers \
    pika==1.3.2 && \
=======
      torch \
      huggingface_hub \
      transformers \
      pika==1.3.2 && \
>>>>>>> main
    rm -rf /root/.cache /root/.local && \
    find /usr/local/lib/python3.9/site-packages -name "*.pyc" -delete && \
    find /usr/local/lib/python3.9/site-packages -type d -name "tests" -exec rm -rf {} + && \
    find /usr/local/lib/python3.9/site-packages -type d -name "__pycache__" -exec rm -rf {} +

<<<<<<< HEAD

=======
>>>>>>> main
COPY /sentiment /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]
