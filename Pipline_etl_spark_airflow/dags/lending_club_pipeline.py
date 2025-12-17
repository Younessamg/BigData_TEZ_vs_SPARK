from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

INPUT_CSV = '/opt/airflow/data/raw/dataset.csv'
OUTPUT_DIR = '/opt/airflow/data/processed'
SPARK_SCRIPT = '/opt/airflow/spark_jobs/transform_lending__data.py'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_input(**context):
    """Vérifie le fichier d'entrée et retourne des métriques."""
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Fichier introuvable : {INPUT_CSV}")
    
    size_bytes = os.path.getsize(INPUT_CSV)
    size_gb = size_bytes / (1024**3)
    size_mb = size_bytes / (1024**2)
    
    print("=" * 80)
    print("📋 VALIDATION DU FICHIER D'ENTRÉE")
    print("=" * 80)
    print(f"📂 Fichier : {INPUT_CSV}")
    print(f"📏 Taille : {size_gb:.2f} Go ({size_mb:.0f} Mo)")
    print(f"✅ Fichier valide")
    print("=" * 80)
    
    # Sauvegarder les métriques dans XCom pour visualisation
    metrics = {
        'input_file': INPUT_CSV,
        'file_size_gb': round(size_gb, 2),
        'file_size_mb': round(size_mb, 2),
        'status': 'valid'
    }
    context['ti'].xcom_push(key='input_metrics', value=metrics)
    
    return metrics


def display_results_summary(**context):
    """Affiche un résumé des résultats pour visualisation dans Airflow."""
    
    print("\n" + "=" * 80)
    print("📊 RÉSUMÉ DES RÉSULTATS")
    print("=" * 80)
    
    # Vérifier les fichiers de sortie
    result = subprocess.run(
        ['ls', '-lh', OUTPUT_DIR],
        capture_output=True,
        text=True
    )
    
    output_files = result.stdout
    print(f"\n📁 Contenu du dossier de sortie ({OUTPUT_DIR}):")
    print("-" * 80)
    print(output_files)
    
    # Compter les fichiers
    try:
        files = os.listdir(OUTPUT_DIR)
        csv_files = [f for f in files if f.endswith('.csv')]
        
        print(f"\n📈 Statistiques :")
        print("-" * 80)
        print(f"  • Nombre de fichiers CSV : {len(csv_files)}")
        print(f"  • Total fichiers : {len(files)}")
        
        if csv_files:
            # Trouver le fichier principal (celui avec le plus de lignes ou le plus grand)
            file_sizes = {}
            for file in csv_files:
                file_path = os.path.join(OUTPUT_DIR, file)
                file_sizes[file] = os.path.getsize(file_path) / (1024**2)  # Taille en MB
            
            main_file = max(file_sizes, key=file_sizes.get)
            main_file_size = file_sizes[main_file]
            
            print(f"  • Fichier principal : {main_file}")
            print(f"  • Taille du fichier principal : {main_file_size:.2f} MB")
        
        # Résumé final
        print("\n" + "=" * 80)
        print("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
        print("=" * 80)
        print(f"📂 Dossier de sortie : {OUTPUT_DIR}")
        print(f"📄 Fichiers générés : {len(csv_files)} fichier(s) CSV")
        print("=" * 80)
        
        # Sauvegarder dans XCom
        summary = {
            'output_directory': OUTPUT_DIR,
            'csv_files_count': len(csv_files),
            'total_files': len(files),
            'main_file': main_file if csv_files else None,
            'main_file_size_mb': round(main_file_size, 2) if csv_files else 0,
            'status': 'success'
        }
        context['ti'].xcom_push(key='output_summary', value=summary)
        
        return summary
        
    except Exception as e:
        print(f"\n⚠️ Erreur lors de l'analyse : {e}")
        return {'status': 'error', 'message': str(e)}

def generate_statistics(**context):
    """Génère des statistiques détaillées sur les données traitées et les affiche dans les logs Airflow."""
    import glob
    import pandas as pd
    
    print("\n" + "=" * 80)
    print("📊 GÉNÉRATION DES STATISTIQUES")
    print("=" * 80)
    
    # Chercher les fichiers CSV dans le dossier processed (nouveau format sans partitionnement)
    print(f"🔍 Recherche des fichiers dans : {OUTPUT_DIR}")
    
    # Pattern pour trouver les fichiers CSV (part-*.csv ou directement les fichiers .csv)
    patterns = [
        os.path.join(OUTPUT_DIR, 'part-*.csv'),
        os.path.join(OUTPUT_DIR, '*.csv'),
    ]
    
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
        if files:
            break
    
    # Filtrer les fichiers de métadonnées si présents
    files = [f for f in files if os.path.isfile(f) and not f.endswith('_SUCCESS')]
    
    print(f"📁 Fichiers trouvés : {len(files)}")
    if files:
        for f in files[:5]:  # Afficher les 5 premiers
            file_size = os.path.getsize(f) / (1024**2)  # Taille en MB
            print(f"   • {os.path.basename(f)} ({file_size:.2f} MB)")
    
    if not files:
        print("⚠️ Aucun fichier CSV trouvé dans le dossier de sortie")
        print(f"📂 Dossier vérifié : {OUTPUT_DIR}")
        print("💡 Vérifiez que la tâche spark_transform s'est bien terminée")
        return {'status': 'no_files', 'message': 'Aucun fichier CSV trouvé'}
    
    # Lire un échantillon du fichier principal (le plus grand)
    try:
        main_file = max(files, key=lambda f: os.path.getsize(f))
        file_size_mb = os.path.getsize(main_file) / (1024**2)
        
        print(f"\n📖 Lecture de l'échantillon depuis : {os.path.basename(main_file)}")
        print(f"📏 Taille du fichier : {file_size_mb:.2f} MB")
        print("⏳ Chargement de 10,000 lignes pour analyse...")
        
        # Lire un échantillon pour les statistiques (limiter à 10k lignes pour performance)
        df = pd.read_csv(main_file)
        
        print(f"✅ {len(df):,} lignes chargées")
        
        print("\n" + "=" * 80)
        print("📊 STATISTIQUES DES DONNÉES")
        print("=" * 80)
        
        # 1. Distribution is_default
        print("\n🎯 DISTRIBUTION VARIABLE CIBLE (is_default):")
        print("-" * 80)
        default_counts = df['is_default'].value_counts().sort_index()
        for idx, count in default_counts.items():
            pct = (count / len(df)) * 100
            status = "DÉFAUT" if idx == 1 else "NON-DÉFAUT"
            print(f"  • {status:15s} ({idx}): {count:>7,} lignes ({pct:>6.2f}%)")
        print("-" * 80)
        
        # 2. Stats numériques
        print("\n💰 STATISTIQUES MONTANTS DE PRÊTS (loan_amnt):")
        print("-" * 80)
        loan_stats = df['loan_amnt'].describe()
        print(f"  • Min      : ${loan_stats['min']:,.2f}")
        print(f"  • Max      : ${loan_stats['max']:,.2f}")
        print(f"  • Moyenne  : ${loan_stats['mean']:,.2f}")
        print(f"  • Médiane  : ${loan_stats['50%']:,.2f}")
        print(f"  • Écart-type: ${loan_stats['std']:,.2f}")
        print("-" * 80)
        
        print("\n📈 STATISTIQUES TAUX D'INTÉRÊT (int_rate):")
        print("-" * 80)
        rate_stats = df['int_rate'].describe()
        print(f"  • Min      : {rate_stats['min']:.2f}%")
        print(f"  • Max      : {rate_stats['max']:.2f}%")
        print(f"  • Moyenne  : {rate_stats['mean']:.2f}%")
        print(f"  • Médiane  : {rate_stats['50%']:.2f}%")
        print("-" * 80)
        
        print("\n💳 STATISTIQUES SCORE FICO (fico_avg):")
        print("-" * 80)
        fico_stats = df['fico_avg'].describe()
        print(f"  • Min      : {fico_stats['min']:.0f}")
        print(f"  • Max      : {fico_stats['max']:.0f}")
        print(f"  • Moyenne  : {fico_stats['mean']:.2f}")
        print(f"  • Médiane  : {fico_stats['50%']:.0f}")
        print("-" * 80)
        
        # 3. Distribution par grade
        print("\n🏆 DISTRIBUTION PAR GRADE:")
        print("-" * 80)
        grade_counts = df['grade'].value_counts().head(10).sort_index()
        for grade, count in grade_counts.items():
            pct = (count / len(df)) * 100
            print(f"  • Grade {grade}: {count:>7,} lignes ({pct:>6.2f}%)")
        print("-" * 80)
        
        # 4. Top purposes
        print("\n🎯 TOP 10 RAISONS DE PRÊT (purpose):")
        print("-" * 80)
        purpose_counts = df['purpose'].value_counts().head(10)
        for i, (purpose, count) in enumerate(purpose_counts.items(), 1):
            pct = (count / len(df)) * 100
            print(f"  {i:2d}. {purpose:25s}: {count:>7,} ({pct:>5.2f}%)")
        print("-" * 80)
        
        # 5. Corrélations
        print("\n🔗 CORRÉLATIONS AVEC is_default:")
        print("-" * 80)
        numeric_cols = ['loan_amnt', 'int_rate', 'annual_inc', 'dti', 'fico_avg', 'income_to_loan_ratio']
        correlations = []
        for col_name in numeric_cols:
            if col_name in df.columns:
                try:
                    corr = df[[col_name, 'is_default']].corr().iloc[0, 1]
                    correlations.append((col_name, corr))
                    direction = "➕ Positive" if corr > 0 else "➖ Négative"
                    strength = "Forte" if abs(corr) > 0.3 else "Modérée" if abs(corr) > 0.1 else "Faible"
                    print(f"  • {col_name:25s}: {corr:>7.3f} ({direction}, {strength})")
                except Exception as e:
                    print(f"  • {col_name:25s}: Erreur - {str(e)}")
        print("-" * 80)
        
        print("\n" + "=" * 80)
        print("✅ STATISTIQUES GÉNÉRÉES AVEC SUCCÈS")
        print("=" * 80)
        print(f"📊 Échantillon analysé : {len(df):,} lignes")
        print(f"📁 Fichier source : {os.path.basename(main_file)}")
        print("=" * 80)
        
        # Sauvegarder dans XCom
        stats_summary = {
            'sample_size': len(df),
            'source_file': os.path.basename(main_file),
            'default_rate': (default_counts.get(1, 0) / len(df) * 100) if 1 in default_counts.index else 0,
            'correlations': {col: corr for col, corr in correlations},
            'status': 'success'
        }
        context['ti'].xcom_push(key='statistics_summary', value=stats_summary)
        
        return stats_summary
        
    except Exception as e:
        print(f"\n❌ ERREUR lors de la génération des statistiques : {e}")
        import traceback
        print(traceback.format_exc())
        print("=" * 80)
        return {'status': 'error', 'message': str(e)}

with DAG(
    dag_id='lending_club_etl',
    default_args=default_args,
    description='Pipeline ETL Lending Club avec Spark',
    schedule=None,
    catchup=False,
    tags=['etl', 'spark'],
) as dag:
    
    check = PythonOperator(
        task_id='check_input',
        python_callable=check_input,
    )
    
    spark_job = BashOperator(
        task_id='spark_transform',
        bash_command=f'cd /opt/airflow/spark_jobs && python transform_lending__data.py "{INPUT_CSV}" "{OUTPUT_DIR}"',
        env={
            'PYTHONUNBUFFERED': '1',  # Pour voir les logs en temps réel dans Airflow
        },
    )
    
    display_summary = PythonOperator(
        task_id='display_results_summary',
        python_callable=display_results_summary,
        provide_context=True,
    )
    
    stats = PythonOperator(
        task_id='generate_statistics',
        python_callable=generate_statistics,
        provide_context=True,
    )
    
    check >> spark_job >> display_summary >> stats

