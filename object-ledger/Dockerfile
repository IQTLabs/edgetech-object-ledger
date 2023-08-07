FROM iqtlabs/edgetech-core:latest

COPY pyproject.toml .
COPY poetry.lock .
RUN pip3 install poetry==1.5.1
RUN poetry config virtualenvs.create false
RUN poetry install --without dev

COPY ./object_ledger.py .

ENTRYPOINT ["python3", "object_ledger.py"]
