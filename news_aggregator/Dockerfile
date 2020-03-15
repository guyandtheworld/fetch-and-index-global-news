FROM python:3.7
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY news_aggregator app/
COPY .keys app/.keys
WORKDIR /app

ENTRYPOINT ["python", "pubsub/subscriber.py"]
