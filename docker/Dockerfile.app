FROM python:3.11-slim

COPY requirements-app.txt /tmp/requirements-app.txt
RUN pip install --no-cache-dir -r /tmp/requirements-app.txt

ENV PYTHONPATH=/opt/app
WORKDIR /opt/app

CMD ["uvicorn", "serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
