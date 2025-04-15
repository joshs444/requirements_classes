# Requirements Classes

A Python project for managing purchase requirements, inventory, sales, and other business operations.

## Project Structure
- `analysis/`: Analytics and reporting modules
- `bom/`: Bill of Materials functionality
- `data_access/`: Data access layer for database operations
- `inventory/`: Inventory management
- `item/`: Item/product definitions
- `ledger/`: Financial records
- `purchase/`: Purchase management
- `sales/`: Sales management
- `utils/`: Utility functions
- `vendor/`: Vendor management

## Setup

1. Create a virtual environment:
```
python -m venv .venv
```

2. Activate the virtual environment:
```
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

3. Install dependencies:
```
pip install -r requirements.txt
```

## Testing

Run tests using pytest:
```
pytest
``` 