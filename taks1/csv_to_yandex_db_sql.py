import pandas as pd
import argparse
import os
import re
from typing import List, Tuple

def infer_sql_type(dtype) -> str:
    """
    Преобразует тип pandas в SQL-тип Yandex DB (YDB).
    """
    if pd.api.types.is_integer_dtype(dtype):
        return 'Uint64'
    if pd.api.types.is_float_dtype(dtype):
        return 'Double'
    if pd.api.types.is_bool_dtype(dtype):
        return 'Bool'
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'Timestamp'
    return 'Utf8'


def sanitize_column(col: str) -> str:
    """
    Преобразует имя столбца в допустимый идентификатор: заменяет недопустимые символы на подчёркивания.
    """
    clean = re.sub(r'[^0-9A-Za-z_]', '_', col)
    if re.match(r'^[0-9]', clean):
        clean = '_' + clean
    return clean


def escape_value(val, dtype) -> str:
    """
    Экранирует значение для вставки: строки в двойных кавычках, даты — через CAST(... AS Timestamp).
    """
    if pd.isna(val):
        return 'NULL'
    if dtype.name.startswith('datetime') or dtype.name.startswith('Timestamp'):
        ts = val.strftime('%Y-%m-%dT%H:%M:%SZ')
        return f'CAST("{ts}" AS Timestamp)'
    if isinstance(val, str):
        esc = val.replace('"', '\\"')
        return f'"{esc}"'
    return str(val)


def generate_sql(df: pd.DataFrame, table_name: str) -> Tuple[str,str]:
    """
    Возвращает DDL и UPSERT запросы отдельно.
    """
    clean_cols = list(df.columns)
    # DDL
    col_defs: List[str] = ['id Uint64']
    for col in clean_cols:
        col_defs.append(f'{col} {infer_sql_type(df[col].dtype)}')
    col_defs.append('PRIMARY KEY (id)')
    ddl = (
        f'CREATE TABLE {table_name} (\n'
        + '    ' + ',\n    '.join(col_defs) + '\n'
        + ');'
    )
    # UPSERT
    cols_bt = ', '.join(f'`{c}`' for c in ['id'] + clean_cols)
    rows = []
    for idx, row in df.iterrows():
        vals = [str(idx + 1)] + [escape_value(row[col], df[col].dtype) for col in clean_cols]
        rows.append(f"({', '.join(vals)})")
    upsert = (
        f'UPSERT INTO {table_name}\n'
        + f'    ({cols_bt})\n'
        + 'VALUES\n'
        + '    ' + ',\n    '.join(rows)
        + ';'
    )
    return ddl, upsert


def main():
    parser = argparse.ArgumentParser(
        description='Генерация DDL и UPSERT для Yandex DB (YDB) из CSV'
    )
    parser.add_argument('csv_file', help='Путь к CSV-файлу')
    parser.add_argument('-o', '--output', default=None,
                        help='Базовое имя для сохранения SQL. Будут созданы <base>_ddl.sql и <base>_upsert.sql')
    parser.add_argument('-t', '--table', default=None,
                        help='Имя таблицы (по умолчанию — имя CSV без расширения)')
    args = parser.parse_args()

    # Чтение CSV: распознаем даты автоматически
    df = pd.read_csv(args.csv_file, parse_dates=True)
    df.columns = [sanitize_column(c) for c in df.columns]
    table_name = sanitize_column(args.table or os.path.splitext(os.path.basename(args.csv_file))[0])

    ddl, upsert = generate_sql(df, table_name)

    if args.output:
        base = os.path.splitext(args.output)[0]
        ddl_file = f"{base}_ddl.sql"
        upsert_file = f"{base}_upsert.sql"
        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write(ddl + '\n')
        with open(upsert_file, 'w', encoding='utf-8') as f:
            f.write(upsert + '\n')
        print(f"DDL сохранён в {ddl_file}")
        print(f"UPSERT сохранён в {upsert_file}")
    else:
        print("-- DDL запрос --")
        print(ddl)
        print("\n-- UPSERT запрос --")
        print(upsert)

if __name__ == '__main__':
    main()
