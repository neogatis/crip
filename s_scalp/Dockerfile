FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py dashboard.html ./
COPY tests/ ./tests/
RUN python -m unittest discover -s tests -v
EXPOSE 8080
CMD ["python","app.py"]
