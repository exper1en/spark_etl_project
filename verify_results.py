import os
from pyspark.sql import SparkSession


def verify_results():
    """Проверка корректности выходных данных конвейера"""
    spark = SparkSession.builder \
        .appName("VerifyOutput") \
        .master("local[*]") \
        .getOrCreate()

    print("🔍 ВЕРИФИКАЦИЯ РЕЗУЛЬТАТОВ")
    print("=" * 60)

    base_path = "../results"

    if not os.path.exists(base_path):
        print("❌ Папка results не найдена! Возможно, конвейер ещё не выполнялся.")
        return

    # Ищем последнюю папку run_*
    runs = [d for d in os.listdir(base_path) if d.startswith("run_")]
    if not runs:
        print("❌ Нет папок с результатами в results/")
        return

    latest_run = sorted(runs)[-1]
    run_path = os.path.join(base_path, latest_run)
    print(f"📁 Анализ последнего запуска: {latest_run}\n")

    # 1. Проверка очищенных данных
    parquet_path = f"{run_path}/cleaned_data/clickstream_cleaned"
    if os.path.exists(parquet_path):
        print("1. ОЧИЩЕННЫЕ ДАННЫЕ:")
        df = spark.read.parquet(parquet_path)
        print(f"   • Записей: {df.count():,}")
        print(f"   • Колонок: {len(df.columns)}")
        print("   • Пример:")
        df.select("user_id", "action", "event_date").show(3, truncate=False)
    else:
        print("❌ Очищенные данные отсутствуют")

    # 2. Агрегированные данные
    aggregated_path = f"{run_path}/aggregated"
    if os.path.exists(aggregated_path):
        print("\n2. АГРЕГИРОВАННЫЕ ДАННЫЕ:")
        for folder in os.listdir(aggregated_path):
            folder_path = os.path.join(aggregated_path, folder)
            if os.path.isdir(folder_path):
                df = spark.read.parquet(folder_path)
                print(f"   • {folder}: {df.count()} записей")
    else:
        print("❌ Агрегированные данные не найдены")

    # 3. CSV-отчёты
    reports_path = f"{run_path}/reports"
    if os.path.exists(reports_path):
        print("\n3. ОТЧЁТЫ (CSV):")
        for folder in os.listdir(reports_path):
            folder_path = os.path.join(reports_path, folder)
            if os.path.isdir(folder_path):
                csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
                for csv_file in csv_files:
                    csv_path = os.path.join(folder_path, csv_file)
                    df = spark.read.csv(csv_path, header=True, sep=";")
                    print(f"   • {csv_file}: {df.count()} записей")
    else:
        print("❌ Отчёты не найдены")

    # 4. Текстовый отчёт
    report_file = f"{run_path}/execution_report.txt"
    if os.path.exists(report_file):
        print("\n4. ТЕКСТОВЫЙ ОТЧЁТ (первые 5 строк):")
        with open(report_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[:5]:
                print(f"   {line.strip()}")
    else:
        print("❌ Текстовый отчёт отсутствует")

    spark.stop()

    print("\n" + "=" * 60)
    print("✅ ВЕРИФИКАЦИЯ ЗАВЕРШЕНА")
    print("=" * 60)


if __name__ == "__main__":
    verify_results()