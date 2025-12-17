from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import json
from dags.lending_club_pipeline import generate_statistics

INPUT_CSV = '/opt/airflow/data/raw/dataset.csv'
OUTPUT_DIR = '/opt/airflow/data/processed'
SPARK_SCRIPT = '/opt/airflow/spark_jobs/transform_lending_data.py'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_input():
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Fichier introuvable : {INPUT_CSV}")
    size_gb = os.path.getsize(INPUT_CSV) / (1024**3)
    print(f"✅ Fichier trouvé : {size_gb:.2f} Go")

def generate_statistics():
    """Génère des stats et les affiche dans les logs Airflow"""
    import glob
    
    # Lire les données processed
    files = glob.glob('/opt/airflow/data/processed/year=*/part-*.csv')
    
    if not files:
        print("❌ Aucun fichier trouvé")
        return
    
    df = pd.read_csv(main_file)
    print("\n" + "="*60)
    print("📊 STATISTIQUES DES DONNÉES")
    print("="*60)
    
    # 1. Distribution is_default
    print("\n🎯 Distribution variable cible (is_default):")
    default_counts = df['is_default'].value_counts()
    print(default_counts)
    print(f"Taux de défaut: {default_counts.get(1, 0) / len(df) * 100:.2f}%")
    
    # 2. Stats numériques
    print("\n💰 Statistiques montants de prêts:")
    print(df['loan_amnt'].describe())
    
    print("\n📈 Statistiques taux d'intérêt:")
    print(df['int_rate'].describe())
    
    print("\n💳 Statistiques score FICO:")
    print(df['fico_avg'].describe())
    
    # 3. Distribution par grade
    print("\n🏆 Distribution par grade:")
    print(df['grade'].value_counts().head(10))
    
    # 4. Top purposes
    print("\n🎯 Top 10 raisons de prêt:")
    print(df['purpose'].value_counts().head(10))
    
    # 5. Corrélations
    print("\n🔗 Corrélation avec is_default:")
    numeric_cols = ['loan_amnt', 'int_rate', 'annual_inc', 'dti', 'fico_avg']
    for col in numeric_cols:
        if col in df.columns:
            corr = df[[col, 'is_default']].corr().iloc[0, 1]
            print(f"  {col}: {corr:.3f}")
    
    print("\n" + "="*60)



with DAG(
    dag_id='lending_club_etl',
    default_args=default_args,
    description='Pipeline ETL Lending Club avec Spark',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'spark'],
) as dag:
    
    check = PythonOperator(
        task_id='check_input',
        python_callable=check_input,
    )
    
    spark_job = BashOperator(
        task_id='spark_transform',
        bash_command=f'cd /opt/airflow/spark_jobs && python transform_lending_data.py "{INPUT_CSV}" "{OUTPUT_DIR}"',
    )
    
    verify = BashOperator(
        task_id='verify_output',
        bash_command=f'ls -lh {OUTPUT_DIR}',
    )

    stats = PythonOperator(
    task_id='generate_statistics',
    python_callable=generate_statistics,
)
    
    check >> spark_job >> verify >> stats