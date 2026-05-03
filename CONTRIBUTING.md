# Contributing

Thanks for your interest. chatvault is intentionally small and focused, so
contributions that align with the existing scope are welcome.

## Development setup

```sh
git clone https://github.com/chatvault/chatvault
cd chatvault
uv venv && source .venv/bin/activate
uv pip install -e '.[decrypt,dev]'
pre-commit install        # optional but recommended
```

Run the suite before submitting:

```sh
ruff check .
ruff format --check .
mypy
pytest
```

`pre-commit` will run ruff, format, mypy and a small set of safety hooks
(detect-private-key, large-file guard) before each commit.

## Style

- Type hints on every public function. `mypy --strict` passes.
- Docstrings on modules and public functions; one short line is fine, longer
  forms only when the why is non-obvious.
- Follow the existing extractor pattern: each module defines `extract(conn, db, ...)`
  and is self-contained. New domains go into `src/chatvault/extractors/`.
- New schema columns require a new migration file (`002_*.sql`, etc.) — never
  edit a released migration.

## Tests

Fixtures live in `tests/fixtures/`. The recommended approach for extractor
tests: build a tiny in-memory msgstore-like DB in a fixture, run the extractor,
assert against the chatvault tables.

## Reporting issues

Personal data leaks are a non-trivial risk in this domain. Please **never**
attach a real msgstore.db, decrypted database, or message excerpt to an issue.
A reduced reproduction with synthetic data is required.

For security issues see [SECURITY.md](SECURITY.md).
