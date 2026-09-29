# Handling Data Skew in PySpark Joins

![PySpark](https://img.shields.io/badge/PySpark-Data%20Engineering-E25A1C?logo=apachespark&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Distributed%20Processing-E25A1C?logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Notebook-FF3621?logo=databricks&logoColor=white)

A practical PySpark demonstration of identifying and handling **data skew** during joins using **Broadcast Join** and **Salting**.

---

## Table of Contents

- [Problem](#problem)
- [What This Project Demonstrates](#what-this-project-demonstrates)
- [Approach](#approach)
  - [1. Baseline Join](#1-baseline-join)
  - [2. Broadcast Join](#2-broadcast-join)
  - [3. Dynamic Skew Detection](#3-dynamic-skew-detection)
  - [4. Salting](#4-salting)
- [Choosing the Right Technique](#choosing-the-right-technique)
- [Key Takeaway](#key-takeaway)
- [Tech Stack](#tech-stack)

---

## Problem

When a join key is highly skewed, a small number of keys can hold a very large share of the data.

For example:

| Customer | Records |
|----------|---------|
| `C1` | ~9 million |
| All other customers | Relatively few |

During a shuffle join, all rows sharing the same key are routed to the same partition. A single hot key like `C1` therefore overloads one task while the rest sit idle, creating an **uneven workload across Spark partitions**, slower stages, and a risk of memory pressure or spills.

---

## What This Project Demonstrates

1. Creating a skewed dataset
2. Measuring key distribution
3. Performing a baseline join
4. Using Broadcast Join when one side is small
5. Dynamically detecting skewed keys
6. Applying salting to skewed keys
7. Replicating matching dimension keys across salt buckets
8. Joining using `(customer_id, salt)`
9. Comparing the approaches

---

## Approach

### 1. Baseline Join

The fact and customer datasets are joined directly on:

```
customer_id
```

This establishes the baseline before any optimization is applied.

### 2. Broadcast Join

When the customer dataset is small enough to fit safely in executor memory, it can be broadcast to every executor, avoiding a large shuffle of the fact table.

```python
from pyspark.sql import functions as F

result_df = fact_df.join(
    F.broadcast(customer_df),
    "customer_id",
    "inner"
)
```

### 3. Dynamic Skew Detection

The pipeline calculates key frequencies and identifies keys whose record count is significantly higher than the average.

> The skewed key is **not hard-coded**, so the approach adapts to the data.

### 4. Salting

For the detected skewed keys:

1. A temporary **salt** value is added to the fact records.
2. The corresponding customer record is **replicated across the same salt buckets**.
3. The final join uses `customer_id + salt`.

This spreads a previously concentrated key across multiple join keys, and therefore across multiple partitions.

```python
# Illustrative pattern
result_df = salted_fact_df.join(
    salted_customer_df,
    on=["customer_id", "salt"],
    how="inner"
)
```

---

## Choosing the Right Technique

| Situation | Approach |
|-----------|----------|
| Data is reasonably balanced | Normal Join |
| One side is small | Broadcast Join |
| Severe skew and broadcasting is not practical | Salting |

---

## Key Takeaway

Join optimization should start with **understanding the data distribution**.

The appropriate technique depends on the characteristics of the datasets, rather than applying the same optimization to every join.

---

## Tech Stack

- PySpark
- Apache Spark
- Databricks
