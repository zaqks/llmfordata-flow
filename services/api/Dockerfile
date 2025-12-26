FROM python:3.12-slim

WORKDIR /app

# Copy only requirements first to leverage Docker layer caching
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Remaining build steps to be completed by the user

COPY . .

RUN chmod +x run.sh
CMD ["./run.sh"]
