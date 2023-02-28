FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN  apt-get update && apt-get install -y netcat && pip install --upgrade pip  \
     && apt-get install -y postgresql-client --no-install-recommends

WORKDIR /code
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
COPY . .

RUN pip3 install poetry==1.2.2 --no-cache-dir
RUN poetry config virtualenvs.create false && poetry install --no-root

ENTRYPOINT ["sh", "entrypoint.sh"]
