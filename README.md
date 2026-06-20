# Transport Analytics using Apache Spark (Scala)

## Overview

This project demonstrates the use of Apache Spark with Scala for analyzing transportation trip data. The objective is to perform data ingestion, transformation, aggregation, window operations, business analytics, and Parquet storage using Spark DataFrame APIs.

---

## Technologies Used

* Scala
* Apache Spark
* Spark SQL
* DataFrames
* Window Functions
* Parquet Storage
* IntelliJ IDEA

---

## Key Spark Concepts Demonstrated

### DataFrames

Efficient distributed data processing using Spark DataFrame APIs.

### Window Functions

Used for:

* Ranking
* Running totals
* Historical comparisons

### Partitioning

Explored partitioning concepts for scalable storage and processing.

### Caching

Studied how Spark stores intermediate results in memory to avoid recomputation.

### Broadcast Joins

Learned how Spark can distribute small datasets across executors to optimize joins.

---

## How to Run

### Prerequisites

* Java 17+
* Scala
* Apache Spark
* IntelliJ IDEA

### Hadoop Configuration (Windows)

Configure:

```text
HADOOP_HOME=C:\hadoop
```

Add:

```text
C:\hadoop\bin
```

to the system PATH.

Place:

```text
winutils.exe
hadoop.dll
```

inside:

```text
C:\hadoop\bin
```

### Execute

Run:

```scala
Main.scala
```

from IntelliJ IDEA.

---

## Learning Outcomes

This project helped in understanding:

* Spark DataFrame APIs
* Aggregations and grouping
* Spark SQL
* Window functions
* Parquet storage
* Data partitioning
* Performance optimization concepts such as caching and broadcast joins

---
