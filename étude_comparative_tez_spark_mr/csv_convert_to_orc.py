#!/usr/bin/env python3
# ~/bench/spark_convert_to_orc.py - VERSION DEFINITIVE CORRIGÉE
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, lit, abs as sf_abs, monotonically_increasing_id, floor, length
from pyspark.sql.types import DoubleType, LongType, IntegerType, StringType

if len(sys.argv) != 3:
    print("Usage: spark_convert_to_orc.py <input_csv_hdfs_path> <output_orc_hdfs_dir>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("convert_to_orc_canonical").getOrCreate()

# 1. Lire le CSV
df = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "false").csv(input_path)

# 2. Normaliser les noms de colonnes
new_cols = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
for old, new in new_cols.items():
    df = df.withColumnRenamed(old, new)

# 3. Mapping des colonnes
mapping = {
    "id": "id", "member_id": "id",
    "loan_amnt": "loan_amount", "loan_amount": "loan_amount", "amount_requested": "loan_amount",
    "funded_amnt": "funded_amount", "funded_amount": "funded_amount", "funded_amnt_inv": "funded_amount",
    "int_rate": "int_rate", "int_rate_num": "int_rate",
    "grade": "grade", "sub_grade": "grade",
    "loan_status": "loan_status",
    "addr_state": "addr_state", "state": "addr_state",
    "zip_code": "zip_code",
    "dti": "dti", "debt-to-income_ratio": "dti",
    "issue_d": "issue_d", "application_date": "issue_d",
    "emp_length": "emp_length", "employment_length": "emp_length",
    "policy_code": "policy_code",
    "risk_score": "risk_score"
}

# 4. Colonnes canoniques avec leurs types
canonical_schema = {
    "id": LongType(),
    "loan_amount": DoubleType(),
    "funded_amount": DoubleType(),
    "int_rate": DoubleType(),
    "grade": StringType(),
    "loan_status": StringType(),
    "addr_state": StringType(),
    "dti": DoubleType(),
    "issue_d": StringType(),
    "emp_length": StringType(),
    "policy_code": IntegerType(),
    "risk_score": DoubleType(),
    "zip_code": StringType()
}

canonical_cols = list(canonical_schema.keys())

# 5. Créer le DataFrame avec les bonnes colonnes et types
df2 = df
columns_created = set(df2.columns)

for canon_col, canon_type in canonical_schema.items():
    # Chercher si une colonne source correspond
    source_col = None
    for src_col in df.columns:
        if src_col in mapping and mapping[src_col] == canon_col:
            source_col = src_col
            break
    
    if source_col and source_col in df2.columns:
        # Renommer si nécessaire
        if source_col != canon_col:
            df2 = df2.withColumnRenamed(source_col, canon_col)
        # Assurer le bon type
        current_type = dict(df2.dtypes).get(canon_col, "")
        if current_type != canon_type.typeName().lower():
            try:
                df2 = df2.withColumn(canon_col, col(canon_col).cast(canon_type))
            except:
                # Si le cast échoue, créer avec valeur par défaut
                if canon_type == StringType():
                    df2 = df2.withColumn(canon_col, lit("").cast(StringType()))
                elif canon_type in [DoubleType(), FloatType]:
                    df2 = df2.withColumn(canon_col, lit(0.0).cast(DoubleType()))
                elif canon_type in [IntegerType(), LongType]:
                    df2 = df2.withColumn(canon_col, lit(0).cast(canon_type))
    else:
        # Créer la colonne manquante avec type explicite
        if canon_type == StringType():
            df2 = df2.withColumn(canon_col, lit("").cast(StringType()))
        elif canon_type == DoubleType():
            df2 = df2.withColumn(canon_col, lit(0.0).cast(DoubleType()))
        elif canon_type == IntegerType():
            df2 = df2.withColumn(canon_col, lit(0).cast(IntegerType()))
        elif canon_type == LongType():
            df2 = df2.withColumn(canon_col, lit(0).cast(LongType()))

# 6. S'assurer que la colonne id existe et a des valeurs
if "id" not in df2.columns or df2.filter(col("id").isNotNull()).count() == 0:
    df2 = df2.withColumn("id", monotonically_increasing_id().cast(LongType()))

# 7. Nettoyer les colonnes numériques
for num_col in ["loan_amount", "funded_amount", "int_rate", "dti", "risk_score"]:
    if num_col in df2.columns:
        # Nettoyer les pourcentages
        if num_col == "int_rate":
            df2 = df2.withColumn(num_col, regexp_replace(col(num_col).cast("string"), "%", ""))
        # Cast en Double
        df2 = df2.withColumn(num_col, col(num_col).cast(DoubleType()))

# 8. Calculer avg_loan_amount pour le filtre
avg_loan = 0.0
if "loan_amount" in df2.columns:
    try:
        avg_row = df2.selectExpr("avg(loan_amount) as avg").filter(col("avg").isNotNull()).first()
        avg_loan = avg_row["avg"] if avg_row else 0.0
    except:
        avg_loan = 0.0

# 9. Créer filter_flag
cond = when(col("loan_status") == "Charged Off", 1)
cond = cond.when((col("risk_score").isNotNull()) & (col("risk_score") < 600), 1)
cond = cond.when((col("loan_amount").isNotNull()) & (col("loan_amount") > lit(float(avg_loan))), 1)
cond = cond.otherwise(0)
df2 = df2.withColumn("filter_flag", cond.cast(IntegerType()))

# 10. Créer branch_id (version simplifiée)
from pyspark.sql.functions import hash
df2 = df2.withColumn("branch_id", ((sf_abs(hash(col("id"))) % 5) + 1).cast(IntegerType()))

# 11. Créer risk_bucket
if "risk_score" in df2.columns:
    df2 = df2.withColumn("risk_bucket", 
                         when(col("risk_score").isNotNull(), 
                              (floor(col("risk_score") / 50) * 50).cast(IntegerType()))
                         .otherwise(lit(None).cast(IntegerType())))
else:
    df2 = df2.withColumn("risk_bucket", lit(None).cast(IntegerType()))

# 12. Sélectionner les colonnes finales
out_cols = canonical_cols + ["filter_flag", "branch_id", "risk_bucket"]
out_cols_existing = [c for c in out_cols if c in df2.columns]
df_out = df2.select(*out_cols_existing)

# 13. VÉRIFICATION FINALE : Afficher le schéma
print("=== Schéma final avant écriture ORC ===")
df_out.printSchema()
print(f"\nNombre de lignes: {df_out.count()}")
print(f"Colonnes: {', '.join(df_out.columns)}")

# 14. Vérifier qu'aucune colonne n'est de type VOID
for col_name, col_type in df_out.dtypes:
    if col_type.lower() in ["void", "null"]:
        print(f"ATTENTION: Colonne '{col_name}' est de type '{col_type}' -> conversion en String")
        df_out = df_out.withColumn(col_name, col(col_name).cast(StringType()))

# 15. Écrire en ORC
print(f"\nÉcriture ORC vers: {output_path}")
df_out.write.mode("overwrite").orc(output_path)
print("✓ ORC écrit avec succès!")

# 16. Vérification
try:
    orc_df = spark.read.orc(output_path)
    print(f"✓ Vérification: ORC contient {orc_df.count()} lignes")
    orc_df.show(5)
except Exception as e:
    print(f"✗ Erreur vérification: {e}")

spark.stop()