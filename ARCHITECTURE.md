# Architecture Documentation

## Entity Relationship Diagram (ERD)

```mermaid
erDiagram
    CUSTOMERS ||--o{ ORDERS : places
    ORDERS ||--|{ LINE_ITEMS : contains
    PRODUCTS ||--o{ LINE_ITEMS : "ordered in"

    CUSTOMERS {
        string customer_id PK
        string first_name
        string last_name
        string email
        timestamp created_at
    }

    PRODUCTS {
        string product_id PK
        string product_name
        string category
        decimal price
        timestamp created_at
    }

    ORDERS {
        string order_id PK
        string customer_id FK
        timestamp order_date
        string status
        decimal total_amount
    }

    LINE_ITEMS {
        string line_item_id PK
        string order_id FK
        string product_id FK
        int quantity
        decimal unit_price
        string item_status
        timestamp delivered_at
    }
```

## Logical Architecture Diagram

```mermaid
graph TB
    subgraph "Data Sources"
        A[Raw JSON Files<br/>Volume: /order_management/raw]
    end

    subgraph "Bronze Layer - Raw Ingestion"
        B1[bronze_customers]
        B2[bronze_products]
        B3[bronze_orders]
        B4[bronze_line_items]
    end

    subgraph "Silver Layer - Cleaned & Validated"
        C1[silver_customers<br/>Data Quality Checks]
        C2[silver_products<br/>Price Validation]
        C3[silver_orders<br/>FK Validation]
        C4[silver_line_items<br/>Quantity Checks]
    end

    subgraph "Gold Layer - Business Analytics"
        D1[gold_order_details<br/>Enriched Order View]
        D2[gold_product_sales_monthly<br/>Sales Aggregation]
        D3[gold_order_completion_status<br/>Completion Tracking]
    end

    subgraph "Consumption Layer"
        E1[SQL Analytics<br/>Most Sold Product Query]
        E2[BI Dashboards]
        E3[ML Models]
    end

    A --> B1
    A --> B2
    A --> B3
    A --> B4

    B1 --> C1
    B2 --> C2
    B3 --> C3
    B4 --> C4

    C1 --> D1
    C2 --> D1
    C3 --> D1
    C4 --> D1

    D1 --> D2
    C3 --> D3
    C4 --> D3

    D1 --> E1
    D2 --> E1
    D3 --> E2
    D2 --> E2
    D1 --> E3
```

## Architecture Patterns

### Medallion Architecture
The pipeline implements the Medallion Architecture pattern with three layers:

1. **Bronze Layer**: Raw data ingestion
   - Auto Loader (cloudFiles) for scalable file ingestion
   - Streaming data from JSON files
   - Minimal transformations

2. **Silver Layer**: Cleaned and validated data
   - Data quality expectations (expect_or_drop)
   - Schema enforcement with explicit casting
   - Invalid records dropped automatically

3. **Gold Layer**: Business-level aggregates
   - Enriched views joining multiple entities
   - Pre-computed aggregations for analytics
   - Optimized for consumption

### Data Quality
- Expectation checks at Silver layer
- NULL validation for primary and foreign keys
- Business rule validation (price > 0, quantity > 0, valid email format)
- Dropped records logged for auditing

### Order Completion Logic
Orders are marked as "Completed" when ALL line items have `item_status = 'delivered'`. This is implemented in the `gold_order_completion_status` table which:
- Counts total items per order
- Counts delivered items per order
- Sets `all_items_delivered = True` when counts match
- Provides completion status: "Completed", "Partially Delivered", or "Pending"

## Technology Stack
- **Databricks Lakeflow (DLT)**: Declarative pipeline framework
- **Delta Lake**: Storage layer with ACID transactions
- **Apache Spark**: Distributed processing engine
- **Auto Loader**: Incremental file ingestion
- **Databricks Asset Bundles**: CI/CD deployment
