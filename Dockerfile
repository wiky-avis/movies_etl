FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /code
COPY . .

RUN pip3 install poetry
RUN poetry config virtualenvs.create false && poetry install --no-root

CMD python3 manage.py run
