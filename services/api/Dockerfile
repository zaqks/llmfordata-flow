FROM python:3.12-slim

WORKDIR /app

# Upgrade pip first
RUN python -m pip install --upgrade pip

# Copy requirements first (better layer caching)
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

EXPOSE 8000
EXPOSE 8080

RUN chmod +x run.sh
CMD ["./run.sh"]
