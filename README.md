# TradNG - Financial Data ETL Pipeline

A robust ETL (Extract, Transform, Load) pipeline for processing financial market data, featuring AI-enhanced retry mechanisms and comprehensive error handling.

## Features

- AI-enhanced retry mechanism using LLAM 3.2
- Comprehensive error handling and logging
- Email notifications for critical failures
- Environment-specific configuration
- Database connection with intelligent retry logic
- Unit test coverage for all components

## Architecture

The project follows a layered architecture:

- **Data Layer**: Handles database operations and data persistence
- **Logic Layer**: Contains business logic and data processing
- **Presentation Layer**: Manages logging and notifications
- **AI Layer**: Enhances retry mechanisms and decision making

## Setup

1. Clone the repository:
```bash
git clone [your-repository-url]
cd TradNG
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Copy `.env.example` to `.env` and configure your environment variables:
```bash
cp .env.example .env
```

## Running Tests

To run the test suite:
```bash
python -m unittest discover AgenticIntraDay/tests
```

## Project Structure

```
TradNG/
├── AgenticIntraDay/
│   ├── Main_etl.py
│   ├── data_layer.py
│   ├── logic_layer.py
│   ├── presentation_layer.py
│   ├── config.py
│   └── tests/
│       ├── test_data_layer.py
│       ├── test_logic_layer.py
│       ├── test_presentation_layer.py
│       └── test_main_etl.py
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your chosen license] 