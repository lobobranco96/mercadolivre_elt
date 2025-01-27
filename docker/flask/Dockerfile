FROM python:3.9-slim

WORKDIR /opt/flask

COPY requirements_flask.txt /opt/flask/requirements_flask.txt
RUN pip install --no-cache-dir -r /opt/flask/requirements_flask.txt

COPY . .

CMD ["python", "app.py"]