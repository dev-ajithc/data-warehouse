# Real-Time Data Warehouse

A scalable and secure real-time data warehouse system built with Python, featuring data ingestion, storage, and processing capabilities.

## Features

- Real-time data ingestion using Apache Kafka
- Secure data storage with MongoDB
- Type-safe data handling with Pydantic
- Robust error handling and logging
- Rate limiting and retry mechanisms
- Configurable batch processing
- Secure hash functions for event IDs

## Requirements

- Python 3.8+
- MongoDB 4.6+
- Apache Kafka 2.8+
- Apache Airflow 2.8+ (for scheduling)

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Copy `.env.example` to `.env` and configure your environment variables
4. Ensure MongoDB and Kafka are running

## Project Structure

```
data_warehouse/
├── src/
│   ├── ingestion.py    # Data ingestion service
│   └── storage.py      # Data storage service
├── tests/             # Unit and integration tests
├── config/            # Configuration files
├── logs/              # Application logs
├── requirements.txt   # Project dependencies
├── setup.cfg         # Development tool configurations
└── .env.example      # Environment variable template
```

## Security Features

- Secure hash functions (SHA-256) for event IDs
- Input validation using Pydantic
- Rate limiting for data ingestion
- Proper error handling and logging
- Environment variables for sensitive data
- Latest secure package versions
- Comprehensive logging

## Usage

1. Start the data ingestion service:
```python
from src.ingestion import DataIngestionService, DataEvent

service = DataIngestionService(
    bootstrap_servers='localhost:9092',
    topic_prefix='dw_'
)

# Ingest an event
event = DataEvent(
    source='my_source',
    event_type='user_action',
    payload={'action': 'login'}
)
service.ingest_event(event)
```

2. Store data in the warehouse:
```python
from src.storage import DataWarehouseStorage

storage = DataWarehouseStorage(
    mongodb_uri='mongodb://localhost:27017',
    database_name='data_warehouse'
)

# Store a single event
storage.store_event('user_actions', event.dict())
```

## Development

- Follow PEP 8 standards
- Run tests: `pytest`
- Check code style: `flake8`
- Sort imports: `isort .`
- Type checking: `mypy .`

## License

MIT License
