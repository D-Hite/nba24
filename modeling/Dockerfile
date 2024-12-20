# Use a Python 3.7.9 base image
FROM python:3.7.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONIOENCODING=UTF-8

# Set the working directory
WORKDIR /app

# Install system dependencies and libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    libomp-dev \
    libffi-dev \
    curl \
    git \
    python3-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install compatible PyTorch and PyTorch Geometric versions
RUN pip install --upgrade pip \
    && pip install torch==1.8.0 \
    && pip install torchvision==0.10.0 \
    && pip install torchaudio==0.10.0 \
    && pip install torch-scatter==2.0.2 \
    && pip install torch-sparse==0.6.9 \
    && pip install torch-geometric==2.0.3 \
    && pip install pandas

# Copy the application code into the container
COPY . /app

# Set the default command to run your application
CMD ["python", "app.py"]
