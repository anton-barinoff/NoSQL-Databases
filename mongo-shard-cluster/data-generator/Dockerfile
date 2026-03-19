FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pymongo

COPY generate_mongo_data.py .

CMD ["python", "generate_mongo_data.py"]