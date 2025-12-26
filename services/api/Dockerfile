FROM python:3.12-slim

WORKDIR /app

# Copy requirements first
COPY requirements.txt ./

# Create venv and install dependencies there
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

EXPOSE 8000
EXPOSE 8080

RUN chmod +x run.sh
CMD ["./run.sh"]
