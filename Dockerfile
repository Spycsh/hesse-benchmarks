FROM python:3.8-alpine

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY main.py /app

RUN mkdir -p /app/results

CMD ["python3", "/app/main.py"]