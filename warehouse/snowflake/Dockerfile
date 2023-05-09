FROM ghcr.io/dbt-labs/dbt-snowflake:1.3.latest

LABEL maintainer="Eric James ericbradleyj@gmail.com"

WORKDIR /snowflake

COPY /games_elt .

RUN pip install -r requirements.txt 

ENV PYTHONPATH=/snowflake/games_elt

ENTRYPOINT [ "/bin/bash", "dbt_run.sh" ] 