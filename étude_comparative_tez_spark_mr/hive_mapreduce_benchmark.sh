#!/bin/bash

# =====================================================
# BENCHMARK: Hive + MapReduce sur Cloudera
# Dataset: Lending Club / benchmark_orc
# =====================================================

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "=========================================="
echo "🧪 TEST: Hive + MapReduce (Cloudera)"
echo "=========================================="
echo "Dataset: benchmark_orc (ORC)"
echo "Execution Engine: MapReduce"
echo "=========================================="

# Fichier de résultats
RESULTS_FILE="hive_mr_results.csv"
echo "Query,Start_Time,End_Time,Duration_Seconds,Rows_Returned" > $RESULTS_FILE

# =====================================================
# FONCTION: Exécuter une requête
# =====================================================
run_test() {
    local query_name=$1
    local description=$2
    local sql_query=$3

    echo -e "\n${BLUE}[$query_name] $description${NC}"

    START=$(date +%s)
    START_TIME=$(date '+%Y-%m-%d %H:%M:%S')

    RESULT=$(echo "$sql_query" | hive 2>&1)

    # Comptage approximatif des lignes retournées
    ROWS=$(echo "$RESULT" | grep -v "OK\|Time taken\|Loading\|Hive\|hive>" | grep -c "^")

    END=$(date +%s)
    END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
    DURATION=$((END - START))

    echo -e "${GREEN}✅ Completed in ${DURATION}s${NC}"
    echo "$RESULT" | tail -5

    echo "$query_name,$START_TIME,$END_TIME,$DURATION,$ROWS" >> $RESULTS_FILE
}

# =====================================================
# CONFIGURATION
# =====================================================
echo -e "\n${YELLOW}Configuration Hive:${NC}"
hive -e "SET hive.execution.engine=mr;" 2>/dev/null
hive -e "SET hive.execution.engine;" | grep hive.execution.engine

# =====================================================
# TESTS
# =====================================================

# Test 1: FILTER
run_test "Test1-Filter" "Filter simple" "
SET hive.execution.engine=mr;
SELECT COUNT(*) AS filter_count
FROM benchmark_orc
WHERE filter_flag = 1;
"

# Test 2: GROUP BY + AGGREGATIONS
run_test "Test2-GroupBy" "Group by grade + aggregations" "
SET hive.execution.engine=mr;
SELECT grade,
       COUNT(*) AS count_loans,
       SUM(funded_amount) AS total_funded,
       AVG(loan_amount) AS avg_loan,
       MIN(loan_amount) AS min_loan,
       MAX(loan_amount) AS max_loan
FROM benchmark_orc
GROUP BY grade
ORDER BY grade;
"

# Test 3: GLOBAL AGGREGATIONS
run_test "Test3-Aggregate" "Global aggregations" "
SET hive.execution.engine=mr;
SELECT COUNT(*) AS total_count,
       SUM(funded_amount) AS total_funded,
       AVG(loan_amount) AS avg_loan,
       MIN(loan_amount) AS min_loan,
       MAX(loan_amount) AS max_loan
FROM benchmark_orc;
"

# Test 4: JOIN + GROUP BY
run_test "Test4-Join" "Join + Group by region" "
SET hive.execution.engine=mr;

CREATE TEMPORARY TABLE branches (branch_id INT, region STRING);
INSERT INTO TABLE branches VALUES
(1,'North'), (2,'South'), (3,'East'), (4,'West'), (5,'Central');

SELECT b.region,
       COUNT(*) AS count_loans,
       SUM(f.funded_amount) AS total,
       AVG(f.loan_amount) AS avg_loan
FROM benchmark_orc f
JOIN branches b
ON f.branch_id = b.branch_id
GROUP BY b.region
ORDER BY b.region;
"

# Test 5: COMPLEX QUERY
run_test "Test5-Complex" "Complex filter + aggregation" "
SET hive.execution.engine=mr;
SELECT grade,
       COUNT(*) AS count,
       SUM(funded_amount) AS total
FROM benchmark_orc
WHERE loan_amount > 10000
  AND funded_amount > 0
GROUP BY grade
ORDER BY total DESC;
"

# =====================================================
# RÉSUMÉ
# =====================================================
echo -e "\n=========================================="
echo -e "${GREEN}🎉 TESTS TERMINÉS!${NC}"
echo "=========================================="

echo -e "\n📊 RÉSULTATS:"
column -t -s',' $RESULTS_FILE

TOTAL_TIME=$(awk -F',' 'NR>1 {sum+=$4} END {print sum}' $RESULTS_FILE)
NUM_TESTS=$(awk 'END {print NR-1}' $RESULTS_FILE)

echo -e "\n📈 STATISTIQUES:"
echo "  Nombre de tests: $NUM_TESTS"
echo "  Temps total: ${TOTAL_TIME}s"
echo "  Temps moyen: $(awk "BEGIN {printf \"%.1f\", $TOTAL_TIME/$NUM_TESTS}")s"

echo -e "\n📁 Fichier résultats: $RESULTS_FILE"
echo "=========================================="