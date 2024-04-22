FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
ENV DATA_DIR="/mnt/disks/share/"

COPY . .

ENTRYPOINT ["python", "main.py"]