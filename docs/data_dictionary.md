# Card Transaction Data Dictionary

## Source Dataset

`transactions_sample.csv`

| Column | Description | Data Type |
|---|---|---|
| customer_id | Unique customer identifier | String |
| transaction_ts | Date and time of transaction | Timestamp |
| amount | Transaction amount | Double |
| is_fraud | Fraud indicator: 0 = No, 1 = Yes | Integer |
| city | City associated with transaction | String |

## Derived Columns

| Column | Description | Data Type |
|---|---|---|
| year | Transaction year | Integer |
| month | Transaction month | Integer |
| amount_category | Low, Medium, or High | String |

## Aggregated Dataset: customer_monthly_spend

| Column | Description |
|---|---|
| customer_id | Customer identifier |
| year | Transaction year |
| month | Transaction month |
| total_spent | Total amount spent |
| transaction_count | Number of transactions |

## Aggregated Dataset: fraud_by_city

| Column | Description |
|---|---|
| city | Transaction city |
| total_txn | Total transactions |
| fraud_count | Number of fraudulent transactions |
| fraud_rate | Fraud transactions / total transactions |