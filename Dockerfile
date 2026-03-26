FROM python:3.9-slim

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project files
COPY . .

# Run the script and always print metrics.json
CMD sh -c "python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log; EXIT_CODE=\$?; echo '\n--- METRICS OUT ---'; cat metrics.json; echo ''; exit \$EXIT_CODE"
