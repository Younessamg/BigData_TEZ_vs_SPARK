#!/bin/bash

# =====================================================
# TEST SIMPLE: Hive avec Tez sur AWS EMR
# Dataset: Lending Club (2.26M lignes, ORC 58 MB)
# =====================================================

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "=========================================="
echo "🧪 TEST: Hive + Tez sur AWS EMR"
echo "=========================================="
echo "Dataset: Lending Club (2,260,668 loans)"
echo "Format: ORC (58 MB)"
echo "=========================================="

# Fichier de résultats
RESULTS_FILE="hive_tez_results.csv"
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
    
    # Exécute la requête et compte les lignes retournées
    RESULT=$(echo "$sql_query" | hive 2>&1)
    ROWS=$(echo "$RESULT" | grep -v "OK\|Time taken\|Loading\|Hive\|hive>" | grep -c "^")
    
    END=$(date +%s)
    END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
    DURATION=$((END - START))
    
    echo -e "${GREEN}✅ Completed in ${DURATION}s${NC}"
    echo "$RESULT" | tail -5
    
    # Sauvegarde
    echo "$query_name,$START_TIME,$END_TIME,$DURATION,$ROWS" >> $RESULTS_FILE
}

# =====================================================
# TESTS
# =====================================================

echo -e "\n${YELLOW}Configuration Hive + Tez:${NC}"
hive -e "SET hive.execution.engine;" 2>/dev/null | grep "hive.execution.engine"

# Test 1: COUNT simple
run_test "Test1-Count" "Count total loans" "
SET hive.execution.engine=tez;
SELECT COUNT(*) as total_loans FROM lending_orc;
"

# Test 2: FILTER
run_test "Test2-Filter" "Filter loans > 10K" "
SET hive.execution.engine=tez;
SELECT COUNT(*) as filtered_count 
FROM lending_orc 
WHERE loan_amnt > 10000;
"

# Test 3: GROUP BY
run_test "Test3-GroupBy" "Group by grade" "
SET hive.execution.engine=tez;
SELECT 
    grade,
    COUNT(*) as loans,
    AVG(loan_amnt) as avg_loan
FROM lending_orc
GROUP BY grade
ORDER BY grade;
"

# Test 4: AGGREGATE
run_test "Test4-Aggregate" "Global aggregations" "
SET hive.execution.engine=tez;
SELECT 
    COUNT(*) as total,
    SUM(funded_amnt) as total_funded,
    AVG(loan_amnt) as avg_loan,
    MIN(loan_amnt) as min_loan,
    MAX(loan_amnt) as max_loan
FROM lending_orc;
"

# Test 5: Complex Query
run_test "Test5-Complex" "Complex filter + group" "
SET hive.execution.engine=tez;
SELECT 
    grade,
    COUNT(*) as loans,
    SUM(funded_amnt) as total_funded
FROM lending_orc
WHERE loan_amnt > 10000 
  AND funded_amnt > 0
GROUP BY grade
ORDER BY total_funded DESC;
"

# =====================================================
# RÉSUMÉ
# =====================================================
echo -e "\n=========================================="
echo -e "${GREEN}🎉 TESTS TERMINÉS!${NC}"
echo "=========================================="

echo -e "\n📊 RÉSULTATS:"
column -t -s',' $RESULTS_FILE

# Calcul du temps total
TOTAL_TIME=$(awk -F',' 'NR>1 {sum+=$4} END {print sum}' $RESULTS_FILE)
NUM_TESTS=$(awk 'END {print NR-1}' $RESULTS_FILE)

echo -e "\n📈 STATISTIQUES:"
echo "  Nombre de tests: $NUM_TESTS"
echo "  Temps total: ${TOTAL_TIME}s"
echo "  Temps moyen: $(awk "BEGIN {printf \"%.1f\", $TOTAL_TIME/$NUM_TESTS}")s"

echo -e "\n📁 Fichier résultats: $RESULTS_FILE"
echo "=========================================="