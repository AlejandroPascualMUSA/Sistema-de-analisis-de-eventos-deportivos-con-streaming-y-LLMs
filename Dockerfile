FROM python:3.11-slim

WORKDIR /opt/app

ENV PYTHONPATH=/opt/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir -r /opt/app/requirements.txt

COPY . /opt/app

CMD ["bash"]
