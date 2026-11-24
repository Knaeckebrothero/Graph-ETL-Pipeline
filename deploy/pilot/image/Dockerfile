# The database tool of the harness with Thesis B's validator, and the MCP wrapper, as one image.
# Built by deploy/pilot/pilot.py from a staged copy (deploy/pilot/run/build), so a running pilot
# holds the code of the moment it was built. BUILD_INFO.json in the image names commits and hashes:
#   <engine> run --rm <image> info
ARG PYTHON_IMAGE=docker.io/library/python:3.12-slim
FROM ${PYTHON_IMAGE}
COPY requirements.txt /opt/harness/requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r /opt/harness/requirements.txt
COPY dbtool /opt/harness/dbtool
COPY mcp /opt/harness/mcp
COPY validator-root /opt/validator-root
COPY kit-root /opt/kit-root
COPY entrypoint.py BUILD_INFO.json /opt/harness/
ENV PYTHONPATH=/opt/validator-root \
    KG_VALIDATOR_ROOT=/opt/kit-root \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /opt/harness
ENTRYPOINT ["python3", "/opt/harness/entrypoint.py"]
