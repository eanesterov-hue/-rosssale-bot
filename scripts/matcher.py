"""
V1 Match: поиск брокеров по объекту
"""
import pandas as pd
from datetime import datetime, timedelta


def normalize(text: str) -> str:
    """Нормализация строки: trim + lower + убрать двойные пробелы"""
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def parse_date(date_str: str) -> datetime:
    """Парсинг даты в формате DD.MM.YYYY"""
    return datetime.strptime(date_str, "%d.%m.%Y")


def find_brokers(
    file_path: str,
    object_query: str,
    days: int = 14,
    match_mode: str = "exact",
    exclude_status: str = None
) -> dict:
    """
    Найти брокеров по объекту.
    
    Args:
        file_path: путь к xlsx файлу
        object_query: запрос (название объекта)
        days: период в днях (по умолчанию 14)
        match_mode: "exact" или "contains"
        exclude_status: статус для исключения (например "Отменен")
    
    Returns:
        dict с результатами
    """
    # Загрузка
    df = pd.read_excel(file_path)
    
    # Проверка колонок
    required = ["Брокер", "Дата", "Объект"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Отсутствует колонка: {col}")
    
    # Нормализация запроса
    query_norm = normalize(object_query)
    
    # Нормализация объектов
    df["_object_norm"] = df["Объект"].apply(normalize)
    
    # Фильтр по дате
    cutoff = datetime.now() - timedelta(days=days)
    df["_date"] = df["Дата"].apply(parse_date)
    df = df[df["_date"] >= cutoff]
    
    # Фильтр по объекту
    if match_mode == "exact":
        df = df[df["_object_norm"] == query_norm]
    else:  # contains
        df = df[df["_object_norm"].str.contains(query_norm, na=False)]
    
    # Фильтр по статусу
    if exclude_status and "Статус" in df.columns:
        df = df[df["Статус"] != exclude_status]
    
    # Уникальные брокеры
    brokers = df["Брокер"].dropna().str.strip()
    brokers = brokers[brokers != ""]
    brokers = sorted(brokers.unique())
    
    return {
        "object": object_query,
        "days": days,
        "match": match_mode,
        "brokers": brokers
    }
