FROM python:3.9-slim

WORKDIR /app

COPY ./requirements_flask.txt /app/requirements_flask.txt

# Instala as dependências do Flask
RUN pip install --no-cache-dir -r /app/requirements_flask.txt

COPY . .

# Comando para rodar o Flask
CMD ["python", "app.py"]