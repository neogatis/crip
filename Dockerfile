FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY engine.py adapters.py scanner.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "scanner.py"]
