# Use Python 3.13 Alpine image
FROM python:3.13-alpine

# Install build dependencies and Python development headers
RUN apk add --no-cache \
    build-base \
    python3-dev \
    libffi-dev \
    gcc \
    musl-dev \
    libressl-dev  # Changed from libssl-dev to libressl-dev

# Copy the necessary files from the 'uv' image (assuming 'uv' is from a previous build stage)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Add the current directory contents to /app in the container
ADD . /app

# Set the working directory to /app
WORKDIR /app

# Create a virtual environment
RUN uv venv

# Set environment variable for the project
ENV UV_PROJECT_ENVIRONMENT=/env

# Install project dependencies without cache
RUN uv sync --frozen --no-cache

CMD ["uv", "run", "main.py"]
