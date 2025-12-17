-- =====================================================
-- BENCHMARK: Pig + MapReduce
-- Dataset: benchmark_orc (ORC via HCatalog)
-- =====================================================

-- Charger la table Hive ORC
data = LOAD 'benchmark_orc'
USING org.apache.hive.hcatalog.pig.HCatLoader();

-------------------------------------------------------
-- TEST 1 : FILTER
-------------------------------------------------------
test1_filt = FILTER data BY filter_flag == 1;
test1_grp  = GROUP test1_filt ALL;
test1_res  = FOREACH test1_grp GENERATE COUNT(test1_filt);

DUMP test1_res;

-------------------------------------------------------
-- TEST 2 : GROUP BY + AGGREGATIONS
-------------------------------------------------------
test2_grp = GROUP data BY grade;

test2_res = FOREACH test2_grp GENERATE
    group AS grade,
    COUNT(data) AS count_loans,
    SUM(data.funded_amount) AS total_funded,
    AVG(data.loan_amount) AS avg_loan,
    MIN(data.loan_amount) AS min_loan,
    MAX(data.loan_amount) AS max_loan;

DUMP test2_res;

-------------------------------------------------------
-- TEST 3 : GLOBAL AGGREGATIONS
-------------------------------------------------------
test3_grp = GROUP data ALL;

test3_res = FOREACH test3_grp GENERATE
    COUNT(data) AS total_count,
    SUM(data.funded_amount) AS total_funded,
    AVG(data.loan_amount) AS avg_loan,
    MIN(data.loan_amount) AS min_loan,
    MAX(data.loan_amount) AS max_loan;

DUMP test3_res;

-------------------------------------------------------
-- TEST 4 : JOIN + GROUP BY (branches)
-------------------------------------------------------

branches = LOAD 'branches'
USING PigStorage(',')
AS (branch_id:int, region:chararray);

joined = JOIN data BY branch_id, branches BY branch_id;

test4_grp = GROUP joined BY region;

test4_res = FOREACH test4_grp GENERATE
    group AS region,
    COUNT(joined) AS count_loans,
    SUM(joined.funded_amount) AS total,
    AVG(joined.loan_amount) AS avg_loan;

DUMP test4_res;

-------------------------------------------------------
-- TEST 5 : COMPLEX FILTER + AGGREGATION
-------------------------------------------------------
test5_filt = FILTER data BY loan_amount > 10000 AND funded_amount > 0;
test5_grp  = GROUP test5_filt BY grade;

test5_res = FOREACH test5_grp GENERATE
    group AS grade,
    COUNT(test5_filt) AS count,
    SUM(test5_filt.funded_amount) AS total;

DUMP test5_res;