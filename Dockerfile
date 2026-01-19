# Baliza CLI Docker Image
# Usage: docker run ghcr.io/franklinbaldo/baliza:latest extract --help

FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/franklinbaldo/baliza"
LABEL org.opencontainers.image.description="Baliza CLI - Extract PNCP procurement data"
LABEL org.opencontainers.image.licenses="MIT"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# Create working directory
WORKDIR /workspace

# Copy project files
COPY . /tmp/baliza

# Install baliza in a virtual environment
RUN cd /tmp/baliza && \
    uv venv /opt/venv && \
    /opt/venv/bin/pip install . && \
    rm -rf /tmp/baliza

# Add virtual environment to PATH
ENV PATH="/opt/venv/bin:$PATH"

# Verify installation
RUN baliza --version

# Set default working directory for data
WORKDIR /data

# Default entrypoint
ENTRYPOINT ["baliza"]
CMD ["--help"]

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD baliza --version || exit 1
