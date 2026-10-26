FROM astrocrpublic.azurecr.io/runtime:3.3-2

# PyPI downloads (duckdb/scipy wheels) can be slow; avoid uv giving up too early.
ENV UV_HTTP_TIMEOUT=600
ENV UV_HTTP_RETRIES=10
