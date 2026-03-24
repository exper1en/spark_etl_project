"""
Генерация синтетических данных для пайплайна обработки.
Создаёт CSV-файл с 50 000 записей и 10% дефектных строк.
"""
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os


def generate_clickstream_data(num_records=50000, output_path="../data/clickstream.csv"):
    """
    Генерирует данные о кликах пользователей.

    Args:
        num_records: количество записей
        output_path: путь для сохранения файла
    """
    fake = Faker('ru_RU')
    np.random.seed(123)        # изменён seed
    random.seed(123)

    print("🔧 Запуск генерации тестового набора...")

    actions = ['click', 'view', 'purchase', 'login', 'logout', 'search', 'add_to_cart']
    devices = ['mobile', 'desktop', 'tablet']
    regions = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург',
               'Казань', 'Нижний Новгород', 'Челябинск', 'Самара']

    data = []

    # Генерация корректных записей
    for i in range(num_records):
        if i % 10000 == 0:
            print(f"   Обработано {i} записей...")

        record = {
            'user_id': fake.uuid4()[:8],
            'session_id': f"sess_{fake.random_number(digits=8)}",
            'action': random.choice(actions),
            'timestamp': (datetime.now() - timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).strftime('%Y-%m-%d %H:%M:%S'),
            'region': random.choice(regions),
            'device': random.choice(devices),
            'duration_sec': random.randint(1, 600),
            'product_id': f"prod_{random.randint(1000, 9999)}",
            'price': round(random.uniform(10, 1000), 2)
        }
        data.append(record)

    # Добавление "плохих" данных (10%)
    bad_records = num_records // 10
    print(f"   Добавление {bad_records} намеренно испорченных записей...")

    problems = [
        {'user_id': ''},
        {'session_id': None},
        {'timestamp': '2024-13-45 25:61:61'},
        {'duration_sec': -random.randint(1, 100)},
        {'device': 'smart_watch'},
        {'price': -random.uniform(10, 100)},
        {'action': 'unknown_action'},
        {'region': ''}
    ]

    for i in range(bad_records):
        base_record = {
            'user_id': fake.uuid4()[:8],
            'session_id': f"sess_{fake.random_number(digits=8)}",
            'action': random.choice(actions),
            'timestamp': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S'),
            'region': random.choice(regions),
            'device': random.choice(devices),
            'duration_sec': random.randint(1, 600),
            'product_id': f"prod_{random.randint(1000, 9999)}",
            'price': round(random.uniform(10, 1000), 2)
        }
        problem = random.choice(problems)
        base_record.update(problem)
        data.append(base_record)

    df = pd.DataFrame(data)
    df = df.sample(frac=1, random_state=123).reset_index(drop=True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='UTF-8')

    print("\n" + "=" * 60)
    print("✅ ГЕНЕРАЦИЯ ЗАВЕРШЕНА")
    print("=" * 60)
    print(f"📄 Всего записей: {len(df):,}")
    print(f"💾 Файл сохранён: {output_path}")
    print("\n📊 Краткая статистика:")
    print(f"  • Уникальных пользователей: {df['user_id'].nunique()}")
    print(f"  • Уникальных регионов: {df['region'].nunique()}")
    print(f"  • Период данных: {df['timestamp'].min()} – {df['timestamp'].max()}")
    print(f"  • Пустых user_id: {df['user_id'].isna().sum() + (df['user_id'] == '').sum()}")
    print(f"  • Отрицательных duration_sec: {(df['duration_sec'] < 0).sum()}")

    print("\n🔍 Пример первых трёх строк:")
    print(df.head(3).to_string())

    return df


if __name__ == "__main__":
    df = generate_clickstream_data(50000)
    print("\n🔎 Детальная проверка:")
    print(df.info())