import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_extract, to_date, year,
    round as spark_round, trim, upper, avg, min as spark_min, max as spark_max
)
from pyspark.sql.types import DoubleType, IntegerType
from spark_config import SELECTED_COLUMNS, DEFAULT_STATUSES, SPARK_CONFIG


def create_spark_session():
    builder = SparkSession.builder.appName("LendingClubETL")
    for key, value in SPARK_CONFIG.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def extract_data(spark, input_path):
    """Extrait les données depuis le fichier CSV source."""
    start_time = time.time()
    print("=" * 80)
    print("📂 ÉTAPE 1 : EXTRACTION DES DONNÉES")
    print("=" * 80)
    print(f"📁 Fichier source : {input_path}")
    print("⏳ Lecture du fichier CSV...")
    
    extract_start = time.time()
    df = spark.read.csv(input_path, header=True, inferSchema=True, multiLine=True, escape='"')
    extract_time = time.time() - extract_start
    
    print(f"✅ Fichier chargé en {extract_time:.2f} secondes")
    
    # Compter les lignes
    print("⏳ Comptage des lignes...")
    count_start = time.time()
    row_count = df.count()
    count_time = time.time() - count_start
    
    total_time = time.time() - start_time
    
    print(f"📊 Lignes extraites : {row_count:,}")
    print(f"⏱️  Temps d'exécution : {total_time:.2f} secondes")
    print(f"   ├─ Lecture CSV : {extract_time:.2f}s")
    print(f"   └─ Comptage : {count_time:.2f}s")
    print("=" * 80)
    
    return df


def transform_data(df):
    """Applique toutes les transformations de données."""
    total_start = time.time()
    print("\n" + "=" * 80)
    print("🔄 ÉTAPE 2 : TRANSFORMATION DES DONNÉES")
    print("=" * 80)
    
    initial_count = df.count()
    print(f"📊 Lignes initiales : {initial_count:,}")
    
    # 1. SÉLECTION DES COLONNES
    step_start = time.time()
    print(f"\n📋 [2.1] Sélection des colonnes ({len(SELECTED_COLUMNS)} colonnes)...")
    df = df.select(*SELECTED_COLUMNS)
    step_time = time.time() - step_start
    print(f"   ✅ Colonnes sélectionnées : {len(SELECTED_COLUMNS)}")
    print(f"   ⏱️  Temps d'exécution : {step_time:.2f} secondes")
    
    # 2. FILTRAGE DES NULLS
    step_start = time.time()
    print(f"\n🔍 [2.2] Filtrage des valeurs nulles...")
    print(f"   ⏳ Suppression des lignes avec loan_status, annual_inc ou issue_d null...")
    before_filter = df.count()
    df = df.filter(col('loan_status').isNotNull() & 
                   col('annual_inc').isNotNull() & 
                   col('issue_d').isNotNull())
    after_filter = df.count()
    step_time = time.time() - step_start
    removed = before_filter - after_filter
    print(f"   ✅ Lignes supprimées : {removed:,} ({removed/before_filter*100:.2f}%)")
    print(f"   📊 Lignes restantes : {after_filter:,}")
    print(f"   ⏱️  Temps d'exécution : {step_time:.2f} secondes")
    
    # 3. SUPPRESSION DES DOUBLONS
    step_start = time.time()
    print(f"\n🗑️  [2.3] Suppression des doublons (sur colonne 'id')...")
    before_dedup = df.count()
    df = df.dropDuplicates(['id'])
    after_dedup = df.count()
    step_time = time.time() - step_start
    duplicates = before_dedup - after_dedup
    print(f"   ✅ Doublons supprimés : {duplicates:,}")
    print(f"   📊 Lignes uniques : {after_dedup:,}")
    print(f"   ⏱️  Temps d'exécution : {step_time:.2f} secondes")
    
    # 4. NORMALISATION DES DONNÉES
    step_start = time.time()
    print(f"\n🔧 [2.4] Normalisation des colonnes...")
    
    # Normalisation term
    norm_start = time.time()
    df = df.withColumn('term', regexp_extract(col('term'), r'(\d+)', 1).cast(IntegerType()))
    print(f"   ✅ 'term' normalisé (regexp + cast Integer) : {time.time() - norm_start:.2f}s")
    
    # Normalisation int_rate
    norm_start = time.time()
    df = df.withColumn('int_rate', regexp_extract(col('int_rate'), r'([\d.]+)', 1).cast(DoubleType()))
    print(f"   ✅ 'int_rate' normalisé (regexp + cast Double) : {time.time() - norm_start:.2f}s")
    
    # Normalisation revol_util
    norm_start = time.time()
    df = df.withColumn('revol_util', regexp_extract(col('revol_util'), r'([\d.]+)', 1).cast(DoubleType()))
    print(f"   ✅ 'revol_util' normalisé (regexp + cast Double) : {time.time() - norm_start:.2f}s")
    
    # Normalisation emp_length
    norm_start = time.time()
    df = df.withColumn('emp_length',
        when(col('emp_length').contains('10+'), 10)
        .when(col('emp_length').contains('< 1'), 0)
        .otherwise(regexp_extract(col('emp_length'), r'(\d+)', 1))
        .cast(IntegerType()))
    print(f"   ✅ 'emp_length' normalisé (conditions + regexp) : {time.time() - norm_start:.2f}s")
    
    step_time = time.time() - step_start
    print(f"   ⏱️  Temps total normalisation : {step_time:.2f} secondes")
    
    # 5. CONVERSION DES DATES
    step_start = time.time()
    print(f"\n📅 [2.5] Conversion des dates...")
    df = df.withColumn('issue_d', to_date(col('issue_d'), 'MMM-yyyy'))
    step_time = time.time() - step_start
    print(f"   ✅ Colonne 'issue_d' convertie (format: MMM-yyyy)")
    print(f"   ⏱️  Temps d'exécution : {step_time:.2f} secondes")
    
    # 6. CRÉATION DE LA VARIABLE CIBLE
    step_start = time.time()
    print(f"\n🎯 [2.6] Création de la variable cible 'is_default'...")
    print(f"   📋 Status considérés comme défaut : {DEFAULT_STATUSES}")
    df = df.withColumn('is_default',
        when(col('loan_status').isin(DEFAULT_STATUSES), 1).otherwise(0))
    step_time = time.time() - step_start
    print(f"   ✅ Variable cible créée (0 = Non-défaut, 1 = Défaut)")
    print(f"   ⏱️  Temps d'exécution : {step_time:.2f} secondes")
    
    # 7. FEATURE ENGINEERING
    step_start = time.time()
    print(f"\n✨ [2.7] Feature Engineering...")
    
    # Feature : fico_avg
    feat_start = time.time()
    df = df.withColumn('fico_avg',
        spark_round((col('fico_range_low') + col('fico_range_high')) / 2, 0).cast(IntegerType()))
    print(f"   ✅ 'fico_avg' créé (moyenne de fico_range_low/high) : {time.time() - feat_start:.2f}s")
    
    # Feature : income_to_loan_ratio
    feat_start = time.time()
    df = df.withColumn('income_to_loan_ratio',
        when(col('loan_amnt') > 0, spark_round(col('annual_inc') / col('loan_amnt'), 2))
        .otherwise(None).cast(DoubleType()))
    print(f"   ✅ 'income_to_loan_ratio' créé (annual_inc / loan_amnt) : {time.time() - feat_start:.2f}s")
    
    # Feature : year
    feat_start = time.time()
    df = df.withColumn('year', year(col('issue_d')))
    print(f"   ✅ 'year' créé (extrait de issue_d) : {time.time() - feat_start:.2f}s")
    
    step_time = time.time() - step_start
    print(f"   ⏱️  Temps total feature engineering : {step_time:.2f} secondes")
    
    # 8. NORMALISATION DES STRINGS
    step_start = time.time()
    print(f"\n🔤 [2.8] Normalisation des colonnes textuelles...")
    text_cols = ['grade', 'sub_grade', 'home_ownership', 'verification_status', 
                 'loan_status', 'purpose', 'addr_state']
    print(f"   📋 Colonnes à normaliser : {len(text_cols)} colonnes")
    for i, col_name in enumerate(text_cols, 1):
        norm_start = time.time()
        df = df.withColumn(col_name, upper(trim(col(col_name))))
        print(f"   ✅ [{i}/{len(text_cols)}] '{col_name}' normalisé (UPPER + TRIM) : {time.time() - norm_start:.2f}s")
    step_time = time.time() - step_start
    print(f"   ⏱️  Temps total normalisation textuelle : {step_time:.2f} secondes")
    
    # Récapitulatif final
    final_count = df.count()
    total_time = time.time() - total_start
    
    print("\n" + "=" * 80)
    print("✅ TRANSFORMATION TERMINÉE")
    print("=" * 80)
    print(f"📊 Lignes initiales : {initial_count:,}")
    print(f"📊 Lignes finales : {final_count:,}")
    print(f"📉 Lignes supprimées : {initial_count - final_count:,} ({(initial_count - final_count)/initial_count*100:.2f}%)")
    print(f"⏱️  Temps total de transformation : {total_time:.2f} secondes ({total_time/60:.2f} minutes)")
    print(f"⚡ Performance : {final_count/total_time:,.0f} lignes/seconde")
    print("=" * 80)
    
    return df


def load_data(df, output_path):
    """Charge les données transformées dans le fichier de sortie."""
    start_time = time.time()
    print("\n" + "=" * 80)
    print("💾 ÉTAPE 3 : CHARGEMENT DES DONNÉES")
    print("=" * 80)
    print(f"📁 Dossier de sortie : {output_path}")
    
    # Compter les lignes avant écriture
    print("⏳ Comptage des lignes à écrire...")
    count_start = time.time()
    row_count = df.count()
    count_time = time.time() - count_start
    print(f"📊 Lignes à écrire : {row_count:,} ({count_time:.2f}s)")
    
    # Préparer le DataFrame pour écriture (un seul fichier)
    print("⏳ Préparation du DataFrame (coalesce à 1 partition)...")
    prep_start = time.time()
    df_output = df.coalesce(1)
    prep_time = time.time() - prep_start
    print(f"✅ DataFrame préparé ({prep_time:.2f}s)")
    
    # Écriture
    print("⏳ Écriture du fichier CSV...")
    write_start = time.time()
    df_output.write.mode('overwrite').option('header', 'true').csv(output_path)
    write_time = time.time() - write_start
    
    total_time = time.time() - start_time
    
    print("=" * 80)
    print("✅ CHARGEMENT TERMINÉ")
    print("=" * 80)
    print(f"📊 Lignes écrites : {row_count:,}")
    print(f"⏱️  Temps d'exécution : {total_time:.2f} secondes ({total_time/60:.2f} minutes)")
    print(f"   ├─ Comptage : {count_time:.2f}s")
    print(f"   ├─ Préparation : {prep_time:.2f}s")
    print(f"   └─ Écriture CSV : {write_time:.2f}s")
    print(f"⚡ Débit d'écriture : {row_count/write_time:,.0f} lignes/seconde")
    print("=" * 80)


def main(input_path, output_path):
    import sys
    
    pipeline_start = time.time()
    print("🚀 PIPELINE ETL LENDING CLUB")
    print("=" * 80)
    print(f"🕐 Démarrage : {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    spark_start = time.time()
    spark = create_spark_session()
    spark_init_time = time.time() - spark_start
    print(f"⚙️  Spark Session initialisée : {spark_init_time:.2f}s")
    
    try:
        df_raw = extract_data(spark, input_path)
        df_clean = transform_data(df_raw)
        
        # Cache le DataFrame pour éviter de recalculer
        df_clean = df_clean.cache()
        
        # Calculer le total une seule fois
        total_rows = df_clean.count()
        print(f"\n📊 Total de lignes après transformation : {total_rows:,}")
        
        # Aperçu des données
        print("\n" + "=" * 80)
        print("📊 APERÇU DES DONNÉES (5 premières lignes)")
        print("=" * 80)
        sys.stdout.flush()  # Force l'affichage immédiat
        df_clean.select('loan_amnt', 'int_rate', 'grade', 'is_default', 'fico_avg', 'year').show(5, truncate=False)
        sys.stdout.flush()
        
        # Statistiques détaillées - Distribution is_default
        print("\n" + "=" * 80)
        print("📈 DISTRIBUTION DE LA VARIABLE CIBLE (is_default)")
        print("=" * 80)
        sys.stdout.flush()
        default_dist = df_clean.groupBy('is_default').count().orderBy('is_default')
        default_dist.show(truncate=False)
        sys.stdout.flush()
        
        # Calculer le pourcentage avec formatage clair
        default_stats = default_dist.collect()
        print("\n📊 RÉSUMÉ is_default :")
        print("-" * 80)
        for row in default_stats:
            count = row['count']
            pct = (count / total_rows) * 100
            status = "DÉFAUT (1)" if row['is_default'] == 1 else "NON-DÉFAUT (0)"
            print(f"  • {status:20s}: {count:>12,} lignes ({pct:>6.2f}%)")
        print("-" * 80)
        sys.stdout.flush()
        
        # Statistiques par année
        print("\n" + "=" * 80)
        print("📅 DISTRIBUTION PAR ANNÉE")
        print("=" * 80)
        sys.stdout.flush()
        year_dist = df_clean.groupBy('year').count().orderBy('year')
        year_dist.show(truncate=False)
        sys.stdout.flush()
        
        # Statistiques par grade
        print("\n" + "=" * 80)
        print("⭐ DISTRIBUTION PAR GRADE")
        print("=" * 80)
        sys.stdout.flush()
        grade_dist = df_clean.groupBy('grade').count().orderBy('grade')
        grade_dist.show(truncate=False)
        sys.stdout.flush()
        
        # Statistiques numériques
        print("\n" + "=" * 80)
        print("🔢 STATISTIQUES NUMÉRIQUES (min, max, moyenne, écart-type)")
        print("=" * 80)
        sys.stdout.flush()
        df_clean.select(
            'loan_amnt', 'int_rate', 'annual_inc', 'fico_avg', 
            'income_to_loan_ratio', 'dti'
        ).describe().show(truncate=False)
        sys.stdout.flush()
        
        # Résumé des métriques clés
        print("\n" + "=" * 80)
        print("📋 RÉSUMÉ DES MÉTRIQUES")
        print("=" * 80)
        print(f"  • Total lignes traitées      : {total_rows:>12,}")
        print(f"  • Nombre de colonnes         : {len(df_clean.columns):>12}")
        
        # Calculer quelques statistiques supplémentaires
        loan_stats = df_clean.select(
            spark_min('loan_amnt').alias('min_loan'),
            spark_max('loan_amnt').alias('max_loan'),
            avg('loan_amnt').alias('avg_loan'),
            avg('int_rate').alias('avg_int_rate'),
            avg('fico_avg').alias('avg_fico')
        ).collect()[0]
        
        print(f"  • Montant prêt (min)          : ${loan_stats['min_loan']:>11,.2f}")
        print(f"  • Montant prêt (max)          : ${loan_stats['max_loan']:>11,.2f}")
        print(f"  • Montant prêt (moyenne)      : ${loan_stats['avg_loan']:>11,.2f}")
        print(f"  • Taux intérêt (moyenne)      : {loan_stats['avg_int_rate']:>11.2f}%")
        print(f"  • Score FICO (moyenne)        : {loan_stats['avg_fico']:>11.2f}")
        
        # Compter les années uniques
        year_list = [row['year'] for row in df_clean.select('year').distinct().collect()]
        print(f"  • Années couvertes            : {min(year_list)} - {max(year_list)} ({len(year_list)} années)")
        
        print("=" * 80)
        sys.stdout.flush()
        
        load_data(df_clean, output_path)
        
        # Résumé global du pipeline
        pipeline_time = time.time() - pipeline_start
        print("\n" + "=" * 80)
        print("🎉 PIPELINE TERMINÉ AVEC SUCCÈS!")
        print("=" * 80)
        print(f"📁 Fichier de sortie : {output_path}")
        print(f"📊 Total lignes traitées : {total_rows:,}")
        print(f"🕐 Heure de fin : {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Temps total d'exécution : {pipeline_time:.2f} secondes ({pipeline_time/60:.2f} minutes)")
        print("=" * 80)
        sys.stdout.flush()
        
    except Exception as e:
        print("\n" + "=" * 80)
        print(f"❌ ERREUR : {e}")
        import traceback
        print(traceback.format_exc())
        print("=" * 80)
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python transform_lending_data.py  ")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])