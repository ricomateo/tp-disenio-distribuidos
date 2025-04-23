FROM python:3.9.7-slim
RUN pip install --no-cache-dir pika==1.3.2 pandas
RUN pip install transformers torch
RUN pip install hf_xet
RUN python -c "from transformers import pipeline; pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')"
COPY /sentiment /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]

