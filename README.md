# Hash server

ASGI file server that serves Seamless buffers.

Buffer file names are identical to their SHA3-256 checksums (hashes).

The server handles HTTP GET requests, and optionally HTTP PUT requests.

The directory containing the buffers may be organized as a Seamless vault directory, containing subdirectories for (in)dependent and big/small buffers.

The same buffer directory can be simultaneously accessed by a GET server, a PUT server and/or a Seamless `ctx.save_vault` operation, since lock files are used to detect/indicate files that are being written.

## Usage

### Using conda

`mamba env create --file environment.yml`

`conda activate hashserver`

In order to run tests, also install `requests`: 
`pip install requests`

Run `python hashserver.py -h` for an overview of all hash server parameters.

The hash server can be run as a command-line tool. In that case, the hash server will read its parameters as command line arguments, and then launch itself under `uvicorn`. Example: `python hashserver.py buffer_dir`.

Alternatively, the hash server can run under a ASGI runner such as `uvicorn`.
In that case, the hash server parameters must be first defined as environment variables. These variables are: HASHSERVER_DIRECTORY and (optionally) HASHSERVER_LOCK_TIMEOUT and HASHSERVER_WRITEABLE. The hash server is then launched by the ASGI runner, e.g. `uvicorn hashserver:app --port 1234`

### Using Docker

TODO

## TODO

- PUT server
- Docker compose file
- Automatic continuous integration

