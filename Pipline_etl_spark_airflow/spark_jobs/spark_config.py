SELECTED_COLUMNS = [
    'id', 'loan_amnt', 'term', 'int_rate', 'installment', 'grade', 'sub_grade',
    'emp_length', 'home_ownership', 'annual_inc', 'verification_status',
    'loan_status', 'purpose', 'dti', 'delinq_2yrs', 'fico_range_low',
    'fico_range_high', 'open_acc', 'pub_rec', 'revol_bal', 'revol_util',
    'total_acc', 'issue_d', 'addr_state'
]

DEFAULT_STATUSES = ['Charged Off', 'Default']

SPARK_CONFIG = {
    'spark.sql.shuffle.partitions': '8',
    'spark.default.parallelism': '8',
    'spark.sql.adaptive.enabled': 'true'
}