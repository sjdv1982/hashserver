# Hash server

ASGI file server that serves Seamless buffers.

Buffer file names are identical to their SHA3-256 checksums (hashes).

The server handles HTTP GET requests, and optionally HTTP PUT requests.

The directory containing the buffers may be organized as a Seamless vault directory, containing subdirectories for (in)dependent and big/small buffers.

The same buffer directory can be simultaneously accessed by a one or more hashservers and/or a Seamless `ctx.save_vault` operation, since lock files are used to detect/indicate files that are being written.

## Launching a service

### Using conda

`mamba env create --file environment.yml`

`conda activate hashserver`

In order to run tests, also install `requests`: 
`pip install requests`

Run `python hashserver.py -h` for an overview of all hash server parameters.

The hash server can be run as a command-line tool. In that case, the hash server will read its parameters as command line arguments, and then launch itself under `uvicorn`. Example: `python hashserver.py buffer_dir`.

Alternatively, the hash server can run under a ASGI runner such as `uvicorn`.
In that case, the hash server parameters must be first defined as environment variables. These variables are: HASHSERVER_DIRECTORY and (optionally) HASHSERVER_LOCK_TIMEOUT and HASHSERVER_WRITEABLE. The hash server is then launched by the ASGI runner, e.g. `uvicorn hashserver:app --port 1234`

The hash server has an access point "/has" where a list of checksums can be provided as the "checksums" parameter, e.g. `import requests; requests.get("http://localhost:8000/has", json=checksums)` in a Python client. The server returns a JSON list of booleans of the same length, indicating for each checksum if it is present or not.

### Using Docker compose

```bash
 export HASHSERVER_PORT=8000
 export HASHSERVER_HOST=0.0.0.0
 export HASHSERVER_DIRECTORY=mybufferdir
 export HASHSERVER_WRITABLE=1
 docker compose up
```

## API

GET /\<checksum\> : checksum must be a SHA3-256 hash in hex form.

Responses:

- Status: 200 => Content: binary, containing the requested buffer
- Status: 404 => Content: binary, containing "Not found"
- Status: 400 => Content: JSON, indicating the error message

PUT /\<checksum\> : checksum must be a SHA3-256 hash in hex form. 
The corresponding buffer must be sent as raw data in the request body.

Responses:

- Status: 200 => Content: binary, containing "OK"
- Status: 400 => Content: JSON, indicating the error message

GET /has : The request body must contain a list of checksums.

Responses:

- Status: 200 => Content: JSON, containing a list of booleans,
 one for each checksum.
- Status: 404 => Content: binary, containing "Not found"
- Status: 400 => Content: JSON, indicating the error message

## TODO

- Automatic continuous integration
- More formal description of API. Harmonize with /docs entrypoint (autogenerated by FastAPI)
