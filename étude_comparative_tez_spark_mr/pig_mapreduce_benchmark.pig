#!/bin/bash

# =====================================================
# BENCHMARK: Pig + MapReduce
# =====================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

RESULTS_FILE="pig_mr_results.csv"
echo "Test,Start_Time,End_Time,Duration_Seconds" > $RESULTS_FILE

run_test() {
    local test_name=$2
    local pig_cmd=$2

    echo -e "\n${BLUE}[$test_name] Running...${NC}"

    START=$(date +%s)
    START_TIME=$(date '+%Y-%m-%d %H:%M:%S')

    pig -x mapreduce -e "$pig_cmd" > /dev/null 2>&1

    END=$(date +%s)
    END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
    DURATION=$((END - START))

    echo -e "${GREEN}✅ Completed in ${DURATION}s${NC}"
    echo "$test_name,$START_TIME,$END_TIME,$DURATION" >> $RESULTS_FILE
}

# =====================================================
# TESTS
# =====================================================

run_test "Test1-Filter" "
data = LOAD 'benchmark_orc' USING org.apache.hive.hcatalog.pig.HCatLoader();
f = FILTER data BY filter_flag == 1;
g = GROUP f ALL;
DUMP (FOREACH g GENERATE COUNT(f));
"

run_test "Test2-GroupBy" "
data = LOAD 'benchmark_orc' USING org.apache.hive.hcatalog.pig.HCatLoader();
g = GROUP data BY grade;
DUMP (FOREACH g GENERATE group, COUNT(data), SUM(data.funded_amount),
AVG(data.loan_amount), MIN(data.loan_amount), MAX(data.loan_amount));
"

run_test "Test3-Aggregate" "
data = LOAD 'benchmark_orc' USING org.apache.hive.hcatalog.pig.HCatLoader();
g = GROUP data ALL;
DUMP (FOREACH g GENERATE COUNT(data), SUM(data.funded_amount),
AVG(data.loan_amount), MIN(data.loan_amount), MAX(data.loan_amount));
"

run_test "Test4-Join" "
data = LOAD 'benchmark_orc' USING org.apache.hive.hcatalog.pig.HCatLoader();
branches = LOAD 'branches' USING PigStorage(',') AS (branch_id:int, region:chararray);
j = JOIN data BY branch_id, branches BY branch_id;
g = GROUP j BY region;
DUMP (FOREACH g GENERATE group, COUNT(j), SUM(j.funded_amount), AVG(j.loan_amount));
"

run_test "Test5-Complex" "
data = LOAD 'benchmark_orc' USING org.apache.hive.hcatalog.pig.HCatLoader();
f = FILTER data BY loan_amount > 10000 AND funded_amount > 0;
g = GROUP f BY grade;
DUMP (FOREACH g GENERATE group, COUNT(f), SUM(f.funded_amount));
"

# =====================================================
# SUMMARY
# =====================================================
echo -e "\n📊 Résultats Pig MapReduce:"
column -t -s',' $RESULTS_FILE