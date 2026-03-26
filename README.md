# PrimetradeAI MLOps Project

<div align="center">
  <h3>A deterministic, observable, and Dockerized batch pipeline for financial trading signals.</h3>
</div>

---

## 🚀 Overview

The **PrimetradeAI MLOps Project** is a production-ready data pipeline designed to ingest OHLCV (Open, High, Low, Close, Volume) financial data, calculate a moving average on the closing price, and output a binary trading signal. 

Built with **Python**, **Pandas**, and **Docker**, it embraces MLOps best practices including strict configuration validation, deterministic outputs (via seed control), and comprehensive JSON metric generation.

## ✨ Features

- **Algorithmic Signal Generation**: Calculates technical indicators (rolling mean) and generates binary `buy`/`sell` (1/0) signals.
- **Robust Validation**: Ensures inputs and settings match expected schemas before executing expensive logic.
- **Observable Logging**: Verbose application logging capturing latency, rows processed, and success rate.
- **JSON Metrics**: Clean `metrics.json` outputs built for ingestion into dashboards or model registries.
- **Containerized**: Full `Dockerfile` built on `python:3.9-slim` for hardware and OS agnostic execution.

## 📁 Project Architecture

```bash
.
├── config.yaml          # Pipeline configuration (window size, seed, version)
├── data.csv             # Sample OHLCV financial dataset
├── Dockerfile           # Containerization configuration
├── metrics.json         # Auto-generated pipeline execution metrics
├── README.md            # Documentation
├── requirements.txt     # Python dependencies
├── run.log              # Auto-generated application logs
└── run.py               # Core MLOps pipeline script
```

## 💻 Local Quickstart

**1. Install Dependencies**
Ensure you have Python 3.9+ installed.
```bash
pip install -r requirements.txt
```

**2. Execute the Pipeline**
Run the core pipeline using the CLI arguments.
```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

**3. Inspect the Output**
Check `metrics.json` for performance and signal results.

## 🐳 Docker Deployment

To build and run the pipeline inside an isolated Docker container:

```bash
# Build the image
docker build -t primetradeai-pipeline .

# Run the container
docker run primetradeai-pipeline
```

*The Docker container is configured to securely run the pipeline and automatically stream the generated metrics to standard output.*

---

**Author:** [Balamurugan200508](https://github.com/Balamurugan200508)
