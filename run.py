import argparse
import logging
import json
import time
import os
import sys
import yaml
import numpy as np
import pandas as pd

def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def load_and_validate_config(config_path, logger):
    logger.info(f"Loading configuration from {config_path}")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in config file: {e}")
            
    required_fields = ['seed', 'window', 'version']
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required config field: '{field}'")
            
    if not isinstance(config['seed'], int):
        raise ValueError("Config field 'seed' must be an integer")
    if not isinstance(config['window'], int) or config['window'] <= 0:
        raise ValueError("Config field 'window' must be a positive integer")
        
    logger.info("Config validation successful")
    return config

def load_and_validate_data(data_path, logger):
    logger.info(f"Loading data from {data_path}")
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}")
        
    try:
        df = pd.read_csv(data_path)
    except Exception as e:
        raise ValueError(f"Failed to read CSV file: {e}")
        
    if df.empty:
        raise ValueError("Data file is empty")
        
    if 'close' not in df.columns:
        raise ValueError("Data file missing required column: 'close'")
        
    logger.info(f"Successfully loaded {len(df)} rows")
    return df

def write_metrics(output_path, payload):
    with open(output_path, 'w') as f:
        json.dump(payload, f, indent=2)

def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description="MLOps Batch Pipeline")
    parser.add_argument('--input', required=True, help="Input CSV data file")
    parser.add_argument('--config', required=True, help="YAML configuration file")
    parser.add_argument('--output', required=True, help="Output JSON metrics file")
    parser.add_argument('--log-file', required=True, help="Log file path")
    args = parser.parse_args()

    logger = setup_logging(args.log_file)
    logger.info("Job start time: %s", time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(start_time)))
    
    config_version = "unknown"
    
    try:
        # Load config
        config = load_and_validate_config(args.config, logger)
        config_version = config['version']
        
        # Set random seed determinism
        np.random.seed(config['seed'])
        logger.info(f"Random seed set to {config['seed']}")
        
        # Load data
        df = load_and_validate_data(args.input, logger)
        
        # Processing steps
        logger.info("Starting processing steps")
        window = config['window']
        
        # Compute rolling mean (allows NaN for first window-1 rows natively)
        logger.info(f"Computing rolling mean with window={window}")
        rolling_mean = df['close'].rolling(window=window).mean()
        
        # Generate binary signal
        logger.info("Generating trading signals")
        df['signal'] = (df['close'] > rolling_mean).astype(int)
        
        # Compute metrics
        rows_processed = len(df)
        signal_rate = df['signal'].mean()
        latency_ms = int((time.time() - start_time) * 1000)
        
        logger.info("Processing complete. Computing metrics summary.")
        logger.info(f"Metrics: rows_processed={rows_processed}, signal_rate={signal_rate:.4f}, latency_ms={latency_ms}")
        
        # Generate success payload
        payload = {
            "version": config_version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": float(signal_rate),
            "latency_ms": latency_ms,
            "seed": config['seed'],
            "status": "success"
        }
        
        write_metrics(args.output, payload)
        logger.info(f"Successfully wrote metrics to {args.output}")
        logger.info("Job completion status: SUCCESS")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        
        # Ensure fallback latency computation
        latency_ms = int((time.time() - start_time) * 1000)
        error_payload = {
            "version": config_version,
            "status": "error",
            "error_message": str(e)
        }
        
        try:
            write_metrics(args.output, error_payload)
            logger.info(f"Wrote error metrics to {args.output}")
        except Exception as write_err:
            logger.error(f"Failed to write error metrics: {write_err}")
            
        logger.info("Job completion status: FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()
