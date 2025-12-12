# Databricks Interview Task: Lakeflow Declarative Pipeline

## Overview

Deploy a Lakeflow Declarative Pipeline using Databricks Assets Bundle for an order management system.

## Scenario

You are building a data pipeline for an e-commerce order management system with the following characteristics:

- Customers place orders containing multiple line items
- Each line item includes:
  - Product information
  - Quantity
  - Individual item status
- Orders are marked as "Completed" when all items are delivered
- Status tracking occurs at both order and item levels

## Deliverables

### 1. Data Model Design

- Create an entity-relationship diagram (ERD) for the system
- Define relationships between:
  - Customers
  - Orders
  - Line Items
  - Products
- Include a logical architecture diagram

### 2. Data Generation

- Create scripts to generate fake/synthetic data for all entities
- Ensure data represents realistic order scenarios

### 3. SQL Query

- Provide a SQL query to identify the most sold product in the last month
- Query should be optimized and production-ready

## Constraints

- Do NOT create additional documentation beyond what is requested
- Focus on practical implementation using Databricks Assets Bundle
- Ensure the solution follows Lakeflow Declarative Pipeline best practices
