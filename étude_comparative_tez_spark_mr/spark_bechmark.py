#!/usr/bin/env python3
# ~/bench/spark_benchmark.py - VERSION OPTIMISÉE POUR COMPARAISON ÉQUITABLE

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, min as spark_min, max as spark_max

if len(sys.argv) != 2:
    print("Usage: spark_benchmark.py <input_orc_hdfs_path>")
    sys.exit(1)

input_path = sys.argv[1]

# Configuration Spark optimale
spark = SparkSession.builder \
    .appName("spark_benchmark") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("=" * 70)
print("=== SPARK BENCHMARK ===")
print("=" * 70)

# Temps total début
benchmark_start = time.time()

# Lecture ORC
print("\n[LOADING] Reading ORC data from HDFS...")
load_start = time.time()
df = spark.read.orc(input_path)
load_time = time.time() - load_start
print(f"✓ Data loaded in {load_time:.3f}s")

# Info sur le dataset (sans cache pour être équitable avec Tez)
row_count = df.count()
col_count = len(df.columns)
print(f"✓ Dataset: {row_count:,} rows, {col_count} columns")

# Afficher schéma important
print("\n[SCHEMA] Important columns:")
for col_name in ["filter_flag", "loan_amount", "funded_amount", "grade", "branch_id"]:
    if col_name in df.columns:
        col_type = dict(df.dtypes)[col_name]
        print(f"  • {col_name}: {col_type}")

# =========================================================================
# BENCHMARKS (sans cache pour comparaison équitable)
# =========================================================================

results = {}

# 1) FILTER
print("\n" + "-" * 70)
print("[QUERY 1] FILTER (filter_flag=1)")
t0 = time.time()
if "filter_flag" in df.columns:
    cnt_filtered = df.filter(col("filter_flag") == 1).count()
    results['filter'] = time.time() - t0
    print(f"✓ Filtered rows: {cnt_filtered:,}")
    print(f"✓ Time: {results['filter']:.3f}s")
else:
    print("✗ Skipped (column not found)")

# 2) GROUP BY + AGGREGATION
print("\n" + "-" * 70)
print("[QUERY 2] GROUP BY grade + AGGREGATIONS")
t0 = time.time()
if "grade" in df.columns:
    gb_df = df.groupBy("grade").agg(
        spark_sum("funded_amount").alias("total_funded"),
        avg("loan_amount").alias("avg_loan"),
        count("*").alias("count_loans"),
        spark_min("loan_amount").alias("min_loan"),
        spark_max("loan_amount").alias("max_loan")
    ).orderBy("grade")
    
    gb_results = gb_df.collect()
    results['groupby'] = time.time() - t0
    
    print(f"✓ Groups: {len(gb_results)}")
    print(f"✓ Time: {results['groupby']:.3f}s")
    print("\n  Top 3 grades:")
    for row in gb_results[:3]:
        total = row['total_funded'] if row['total_funded'] is not None else 0
        avg_val = row['avg_loan'] if row['avg_loan'] is not None else 0
        count_val = row['count_loans'] if row['count_loans'] is not None else 0
        print(f"    {row['grade']}: count={count_val:,}, "
              f"total=${total:,.0f}, avg=${avg_val:,.0f}")
    if len(gb_results) > 3:
        print(f"    ... and {len(gb_results)-3} more grades")
else:
    print("✗ Skipped (column not found)")

# 3) AGGREGATION GLOBALE
print("\n" + "-" * 70)
print("[QUERY 3] GLOBAL AGGREGATIONS")
t0 = time.time()
if "funded_amount" in df.columns and "loan_amount" in df.columns:
    agg_result = df.agg(
        spark_sum("funded_amount").alias("total_funded"),
        avg("loan_amount").alias("avg_loan"),
        spark_min("loan_amount").alias("min_loan"),
        spark_max("loan_amount").alias("max_loan"),
        count("*").alias("total_count")
    ).collect()[0]
    
    results['aggregate'] = time.time() - t0
    print(f"✓ Time: {results['aggregate']:.3f}s")
    total_funded = agg_result['total_funded'] if agg_result['total_funded'] is not None else 0
    avg_loan = agg_result['avg_loan'] if agg_result['avg_loan'] is not None else 0
    min_loan = agg_result['min_loan'] if agg_result['min_loan'] is not None else 0
    max_loan = agg_result['max_loan'] if agg_result['max_loan'] is not None else 0
    print(f"  • Total funded: ${total_funded:,.0f}")
    print(f"  • Avg loan: ${avg_loan:,.2f}")
    print(f"  • Min loan: ${min_loan:,.2f}")
    print(f"  • Max loan: ${max_loan:,.2f}")
else:
    print("✗ Skipped (columns not found)")

# 4) JOIN + GROUP BY
print("\n" + "-" * 70)
print("[QUERY 4] JOIN + GROUP BY region")
branches = spark.createDataFrame(
    [(1, "North"), (2, "South"), (3, "East"), (4, "West"), (5, "Central")],
    ["branch_id", "region"]
)

t0 = time.time()
if "branch_id" in df.columns and "funded_amount" in df.columns:
    joined_df = df.join(branches, "branch_id", "inner") \
                  .groupBy("region") \
                  .agg(
                      spark_sum("funded_amount").alias("total"),
                      count("*").alias("count_loans"),
                      avg("loan_amount").alias("avg_loan")
                  ) \
                  .orderBy("region")
    
    join_results = joined_df.collect()
    results['join'] = time.time() - t0
    
    print(f"✓ Time: {results['join']:.3f}s")
    print("  Results by region:")
    for row in join_results:
        total = row['total'] if row['total'] is not None else 0
        count_val = row['count_loans'] if row['count_loans'] is not None else 0
        avg_val = row['avg_loan'] if row['avg_loan'] is not None else 0
        print(f"    {row['region']}: count={count_val:,}, "
              f"total=${total:,.0f}, avg=${avg_val:,.0f}")
else:
    print("✗ Skipped (columns not found)")

# 5) FILTER COMPLEXE + AGGREGATION
print("\n" + "-" * 70)
print("[QUERY 5] COMPLEX FILTER + AGGREGATION")
t0 = time.time()
if all(c in df.columns for c in ["loan_amount", "funded_amount", "grade"]):
    complex_result = df.filter(
        (col("loan_amount") > 10000) & 
        (col("funded_amount") > 0)
    ).groupBy("grade").agg(
        count("*").alias("count"),
        spark_sum("funded_amount").alias("total")
    ).orderBy(col("total").desc()).collect()
    
    results['complex'] = time.time() - t0
    print(f"✓ Time: {results['complex']:.3f}s")
    print(f"  Filtered groups: {len(complex_result)}")
else:
    print("✗ Skipped (columns not found)")

# =========================================================================
# RÉSUMÉ FINAL
# =========================================================================
benchmark_time = time.time() - benchmark_start

print("\n" + "=" * 70)
print("=== BENCHMARK SUMMARY ===")
print("=" * 70)
print(f"Total rows processed: {row_count:,}")
print(f"Total benchmark time: {benchmark_time:.3f}s")
print(f"\nQuery execution times:")
for i, (query_name, query_time) in enumerate(results.items(), 1):
    print(f"  {i}. {query_name.upper()}: {query_time:.3f}s")

if results:
    total_query_time = sum(results.values())  # Python's built-in sum()
    print(f"\nTotal query time: {total_query_time:.3f}s")
    print(f"Average query time: {total_query_time/len(results):.3f}s")

print("=" * 70)
spark.stop()