FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pymongo faker

COPY main.py .
COPY load_test.py .

CMD ["python", "main.py"]