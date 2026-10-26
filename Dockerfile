
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY extraction/ ./extraction/

WORKDIR /app/extraction

CMD ["python", "extract_mysql.py"]
