FROM python:3.11-slim

WORKDIR /app

COPY newsroom-api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY newsroom-api/app.py ./app.py
COPY newsroom-api/static ./static
COPY spark-jobs/common ./common

EXPOSE 8085

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8085"]
