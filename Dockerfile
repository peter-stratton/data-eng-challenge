FROM python:3.8-slim as base

FROM base as builder
COPY requirements/base.txt /base.txt
COPY requirements/prod.txt /requirements.txt
RUN pip install -U pip
RUN pip install --user -r /requirements.txt

FROM base
COPY --from=builder /root/.local /root/.local

ENV PYTHONUNBUFFERED 1
ENV PATH=/root/.local/bin:$PATH

COPY ./nhldata /app/nhldata
COPY ./setup.py /app/setup.py


WORKDIR app
RUN pip install -U pip
RUN pip install --user .

# 1 day
ENTRYPOINT ["nhldata", "games", "--from-date=2020-09-01"]
# Full Season with Playoffs
#ENTRYPOINT ["nhldata", "games", "--from-date=2019-10-02", "--to-date=2020-09-21"]
# Null currentAge
#ENTRYPOINT ["nhldata", "games", "--from-date=2020-02-22", "--to-date=2020-02-22"]
