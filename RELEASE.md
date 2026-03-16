# Release `hashserver` to PyPI

## One-time setup

- Ensure you have PyPI maintainer access for `hashserver`.
- Make sure your TestPyPI and PyPI accounts match
- Install build tooling:

```bash
python -m pip install --upgrade build twine
```

## Build

```bash
python -m build
```

## Validate

```bash
python -m twine check dist/*
```

## Upload

Test PyPI:

```bash
python -m twine upload --repository testpypi dist/*
```

Production PyPI:

```bash
python -m twine upload dist/*
```
