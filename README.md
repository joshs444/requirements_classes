# Requirements Classes

A Python project for managing purchase requirements, inventory, sales, and other business operations. This system provides a comprehensive set of classes for tracking and managing data related to business operations including inventory management, purchasing, sales, vendor relationships, and financial tracking.

## Overview

This system helps businesses manage their operational data with a modular architecture that separates concerns across different domains. The project uses a repository pattern for data access with singleton instances, and provides analytical capabilities for business reporting.

## Project Structure

- `analysis/`: Analytics and reporting modules for business intelligence
- `bom/`: Bill of Materials functionality for manufacturing and assembly tracking
- `data_access/`: Data access layer including SQL Server connectivity via SQLAlchemy
- `inventory/`: Inventory management including stock levels and aggregation functionality
- `item/`: Item/product definitions and metadata management
- `ledger/`: Financial records and accounting operations
- `purchase/`: Purchase management including requisitions, receipts, and order tracking
- `sales/`: Sales management including orders, quotes, and customer interactions
- `utils/`: Utility functions and helper classes
- `vendor/`: Vendor management and supplier data
- `tests/`: Automated tests ensuring code quality and functionality
- `output/` and `outputs/`: Generated reports and data exports
- `sql/`: SQL scripts and database schemas

## Prerequisites

- Python 3.8+
- Microsoft SQL Server
- ODBC Driver 17 for SQL Server

## Dependencies

The project relies on several Python packages:
- `pandas`: For data manipulation and analysis
- `pyodbc`: For database connectivity to SQL Server
- `sqlalchemy`: For ORM and database operations
- `openpyxl`: For Excel file reading/writing

