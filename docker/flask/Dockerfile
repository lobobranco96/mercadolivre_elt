FROM python:3.9-slim

WORKDIR /opt/flask

COPY flask_app/api.py /opt/flask/

COPY requirements_flask.txt /opt/flask/requirements_flask.txt
RUN pip install --no-cache-dir -r /opt/flask/requirements_flask.txt

CMD ["python", "api.py"]
