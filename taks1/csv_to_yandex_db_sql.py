import pandas as pd
import argparse
import os

def infer_sql_type(dtype):

    #Преобразует тип pandas в SQL-тип для Yandex DB.

    if pd.api.types.is_integer_dtype(dtype):
        return 'BIGINT'
    if pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE PRECISION'
    if pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    # По умолчанию — текстовая колонка
    return 'TEXT'


def escape_value(val):
    if pd.isna(val):
        return 'NULL'
    if isinstance(val, str):
        # Дублируем одиночные кавычки
        val = val.replace("'", "''")
        return f"'{val}'"
    if isinstance(val, pd.Timestamp):
        return f"'{val.isoformat()}'"
    # Числа и булевы значения
    return str(val)


def main():
    parser = argparse.ArgumentParser(description='Генерация SQL-скрипта для создания таблицы в Yandex DB и вставки данных из CSV')
    parser.add_argument('csv_file', help='Путь к входному CSV-файлу')
    parser.add_argument('-o', '--output', default=None, help='Путь к выходному SQL-файлу (по умолчанию — вывод в stdout)')
    parser.add_argument('-t', '--table', default=None, help='Имя создаваемой таблицы (по умолчанию — имя CSV без расширения)')
    args = parser.parse_args()

    df = pd.read_csv(args.csv_file)

    # Определяем имя таблицы
    table_name = args.table or os.path.splitext(os.path.basename(args.csv_file))[0]
    column_defs = []
    for col in df.columns:
        sql_type = infer_sql_type(df[col].dtype)
        # Оборачиваем имена колонок в двойные кавычки на случай специальных символов
        column_defs.append(f'"{col}" {sql_type}')

    create_stmt = (
        f'CREATE TABLE "{table_name}" (\n'
        + '    ' + ',\n    '.join(column_defs) + '\n'
        + ');\n\n'
    )

    # INSERT-ы для каждой строки
    insert_stmts = []
    cols_quoted = ', '.join([f'"{col}"' for col in df.columns])
    for _, row in df.iterrows():
        values = ', '.join(escape_value(x) for x in row)
        insert_stmts.append(f'INSERT INTO "{table_name}" ({cols_quoted}) VALUES ({values});')

    sql = create_stmt + '\n'.join(insert_stmts)

    # Выводим или сохраняем
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(sql)
        print(f"SQL-скрипт сохранен в '{args.output}'")
    else:
        print(sql)


if __name__ == '__main__':
    main()
