FROM python:3.7
RUN pip install lxml Cython psycopg2-binary numpy
COPY requirements.txt ./requirements.txt
RUN pip install dragnet
RUN pip install -r requirements.txt

COPY body_extraction app/
COPY .keys app/.keys
WORKDIR /app

ENTRYPOINT ["python", "subscriber.py"]
