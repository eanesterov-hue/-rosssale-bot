#!/usr/bin/env python3
"""
Telegram-бот для поиска брокеров по объекту и району
"""
import os
import sys
import json
from pathlib import Path

# Добавляем корень проекта в path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import pandas as pd
from rapidfuzz import fuzz, process
from datetime import datetime, timedelta

# Загружаем переменные окружения
load_dotenv(PROJECT_ROOT / ".env")

# Конфигурация
DATA_FILE = PROJECT_ROOT / "data" / "showings.xlsx"
DISTRICTS_FILE = PROJECT_ROOT / "data" / "districts.json"
DAYS = 60  # 2 месяца

# Группы синонимов: объекты, которые являются одним ЖК
# При поиске одного — показываем брокеров по всем из группы
SYNONYMS = [
    # Кириллица ↔ Латиница
    ["Прайм парк", "Prime Park"],
    ["Шагал", "Shagal"],
    ["Соул", "Soul"],
    ["Слава", "Slava"],
    ["Ракурс", "Rakurs"],
    ["Принципал плаза", "Principal Plaza"],
    ["Примавера новая", "Primavera"],
    ["Синатра", "Sinatra вторичка"],
    ["Балчуг резиденс", "Balchug Residence"],
    ["Сидней Сити", "Sidney City", "Sydney city"],
    
    # Вторичка = Первичка
    ["Башня Федерация", "Башня Федерация вторичка"],
    ["Садовые кварталы", "Вторичка Садовые кварталы"],
    ["Династия", "Вторичка Династия"],
    ["Knightsbridge Private Park", "Вторичка Knightsbridge Private Park"],
    ["Остров", "Остров Вторичка"],
    ["ЖК Крылья", "Крылья вторичка"],
    
    # Разные написания
    ["ЖК Таврический", "Таврический"],
    ["Дом в Николино", "Николино"],
    ["Башня Город Столиц", "Город Столиц"],
    ["Level Мичуринский", "Мичуринский"],
    ["Canal Front", "Canal Front Residences 3"],
    ["Поклонная 9", "Поклонная, 9", "Покланная 9"],  # + опечатка
    
    # Опечатки
    ["Lucky", "Lacky"],
]

# Словарь алиасов: запрос → на что заменить
# Фонетические соответствия, опечатки, альтернативные написания
ALIASES = {
    # Фонетика: кириллица → латиница
    "клауд": "cloud",
    "клауд тауэр": "cloud tower",
    "тауэр": "tower",
    "гранд": "grand",
    "гарден": "garden",
    "вест гарден": "west garden",
    "вест": "west",
    "резиденс": "residences",
    "пиннакл": "pinnacle",
    "марина": "marina",
    "канал": "canal",
    "фронт": "front",
    "панорамик": "panoramic",
    "стелла": "stella",
    "марис": "maris",
    "вида": "vida",
    "крик": "creek",
    "бич": "beach",
    
    # Опечатки в данных
    "поклонная": "покланная",  # в данных с опечаткой
    
    # Альтернативные написания
    "праймпарк": "прайм парк",
    "прайм": "прайм парк",
    "артхаус": "артхаус",
    "веллтон": "веллтон тауэрс",
    "квартал на ленинском": "жк квартал на ленинском",
}


def transliterate_ru_to_en(text: str) -> str:
    """Транслитерация кириллицы в латиницу"""
    table = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
    }
    result = ""
    for char in text.lower():
        result += table.get(char, char)
    return result


def normalize(text: str) -> str:
    """Нормализация строки"""
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def normalize_for_search(text: str) -> str:
    """Нормализация для поиска — с транслитерацией"""
    text = normalize(text)
    # Транслитерируем кириллицу
    return transliterate_ru_to_en(text)


def apply_aliases(query: str) -> list:
    """
    Применить словарь алиасов.
    Возвращает список вариантов запроса для поиска.
    """
    query_norm = normalize(query)
    variants = [query_norm]
    
    # Проверяем точное совпадение с алиасом
    if query_norm in ALIASES:
        variants.append(normalize(ALIASES[query_norm]))
    
    # Проверяем, содержится ли алиас в запросе
    for alias, replacement in ALIASES.items():
        if alias in query_norm and alias != query_norm:
            new_query = query_norm.replace(alias, replacement)
            if new_query not in variants:
                variants.append(new_query)
    
    return variants


def get_synonyms(object_name: str) -> list:
    """
    Получить все синонимы объекта (включая сам объект).
    """
    object_norm = normalize(object_name)
    
    for group in SYNONYMS:
        group_norm = [normalize(o) for o in group]
        if object_norm in group_norm:
            return group  # Возвращаем всю группу
    
    return [object_name]  # Нет синонимов — только сам объект


def load_districts():
    """Загрузить справочник районов"""
    if not DISTRICTS_FILE.exists():
        return {}
    
    with open(DISTRICTS_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data.get("objects", {})


def get_objects_by_district(district_query: str) -> list:
    """
    Найти все ЖК в указанном районе.
    Возвращает список объектов.
    """
    districts_data = load_districts()
    district_query_norm = normalize(district_query)
    
    results = []
    
    for obj_name, info in districts_data.items():
        district_norm = normalize(info.get("district", ""))
        city_norm = normalize(info.get("city", ""))
        
        # Ищем по району (fuzzy)
        if district_query_norm in district_norm or district_norm in district_query_norm:
            results.append({
                "object": obj_name,
                "district": info.get("district"),
                "city": info.get("city"),
                "country": info.get("country")
            })
        # Также ищем по городу (для Дубая — можно искать "Дубай")
        elif district_query_norm in city_norm or city_norm == district_query_norm:
            results.append({
                "object": obj_name,
                "district": info.get("district"),
                "city": info.get("city"),
                "country": info.get("country")
            })
    
    return results


def get_all_districts() -> list:
    """Получить список всех уникальных районов"""
    districts_data = load_districts()
    districts_set = set()
    
    for obj_name, info in districts_data.items():
        district = info.get("district")
        city = info.get("city")
        if district and city:
            districts_set.add(f"{district} ({city})")
    
    return sorted(districts_set)


def load_data():
    """Загрузить данные из файла"""
    if not DATA_FILE.exists():
        return None, "Файл данных не найден"
    
    df = pd.read_excel(DATA_FILE)
    
    # Проверка колонок
    required = ["Брокер", "Дата", "Объект"]
    for col in required:
        if col not in df.columns:
            return None, f"Отсутствует колонка: {col}"
    
    return df, None


def parse_date(date_str: str) -> datetime:
    """Парсинг даты"""
    return datetime.strptime(date_str, "%d.%m.%Y")


def find_best_match(query: str, objects: list) -> tuple:
    """
    Найти ближайшее совпадение объекта.
    Возвращает (найденный_объект, score, exact_match) или (None, 0, False)
    """
    if not objects:
        return None, 0, False
    
    # Получаем варианты запроса через алиасы
    query_variants = apply_aliases(query)
    
    # Подготовим нормализованные версии объектов
    objects_norm = [normalize(o) for o in objects]
    objects_translit = [normalize_for_search(o) for o in objects]
    
    # Для каждого варианта запроса пробуем найти совпадение
    for query_variant in query_variants:
        query_translit = normalize_for_search(query_variant)
        
        # 1. Точное совпадение
        for i, obj in enumerate(objects):
            if query_variant == objects_norm[i] or query_translit == objects_translit[i]:
                return obj, 100, True
        
        # 2. Вхождение (contains) — запрос содержится в объекте
        for i, obj in enumerate(objects):
            if query_variant in objects_norm[i] or query_translit in objects_translit[i]:
                return obj, 95, True
        
        # 3. Обратное вхождение — объект содержится в запросе
        for i, obj in enumerate(objects):
            if objects_norm[i] in query_variant or objects_translit[i] in query_translit:
                return obj, 90, True
    
    # 4. Fuzzy search с транслитерацией (порог 75) — только очень похожие
    best_result = None
    best_score = 0
    
    for query_variant in query_variants:
        query_translit = normalize_for_search(query_variant)
        result = process.extractOne(
            query_translit,
            objects_translit,
            scorer=fuzz.WRatio,
            score_cutoff=75  # высокий порог — только реально похожие
        )
        
        if result and result[1] > best_score:
            best_result = result
            best_score = result[1]
    
    if best_result:
        match_translit, score, idx = best_result
        return objects[idx], score, False
    
    return None, 0, False


def search_brokers(query: str) -> dict:
    """
    Поиск брокеров по объекту с fuzzy matching.
    """
    df, error = load_data()
    if error:
        return {"error": error}
    
    # Фильтр по дате
    cutoff = datetime.now() - timedelta(days=DAYS)
    df["_date"] = df["Дата"].apply(parse_date)
    df = df[df["_date"] >= cutoff]
    
    if df.empty:
        return {"error": "Нет данных за указанный период"}
    
    # Уникальные объекты
    objects = df["Объект"].dropna().unique().tolist()
    
    # Поиск ближайшего объекта
    best_match, score, exact = find_best_match(query, objects)
    
    if not best_match:
        return {
            "found": False,
            "query": query,
            "days": DAYS
        }
    
    # Получаем все синонимы найденного объекта
    synonyms = get_synonyms(best_match)
    
    # Фильтруем по всем синонимам
    df_filtered = df[df["Объект"].isin(synonyms)]
    
    # Уникальные брокеры
    brokers = df_filtered["Брокер"].dropna().str.strip()
    brokers = brokers[brokers != ""]
    brokers = sorted(brokers.unique())
    
    # Какие объекты реально нашлись в данных
    found_objects = df_filtered["Объект"].unique().tolist()
    
    return {
        "found": True,
        "query": query,
        "object": best_match,
        "objects": found_objects,  # все найденные варианты
        "days": DAYS,
        "brokers": brokers,
        "score": score,
        "exact": exact  # точное совпадение или fuzzy
    }


def search_by_district(district_query: str) -> dict:
    """
    Поиск брокеров по району.
    Возвращает результаты сгруппированные по ЖК.
    """
    df, error = load_data()
    if error:
        return {"error": error}
    
    # Фильтр по дате
    cutoff = datetime.now() - timedelta(days=DAYS)
    df["_date"] = df["Дата"].apply(parse_date)
    df = df[df["_date"] >= cutoff]
    
    if df.empty:
        return {"error": "Нет данных за указанный период"}
    
    # Получаем все ЖК в этом районе из справочника
    district_objects = get_objects_by_district(district_query)
    
    if not district_objects:
        # Пробуем fuzzy поиск по названию района
        all_districts = get_all_districts()
        district_query_norm = normalize(district_query)
        
        # Fuzzy поиск
        result = process.extractOne(
            district_query_norm,
            [normalize(d) for d in all_districts],
            scorer=fuzz.WRatio,
            score_cutoff=70
        )
        
        if result:
            match_norm, score, idx = result
            suggested = all_districts[idx]
            return {
                "found": False,
                "query": district_query,
                "suggestion": suggested,
                "days": DAYS
            }
        
        return {
            "found": False,
            "query": district_query,
            "days": DAYS
        }
    
    # Собираем имена ЖК из справочника
    object_names = [obj["object"] for obj in district_objects]
    
    # Фильтруем данные по этим объектам
    # Нормализуем для сравнения
    objects_norm = {normalize(name): name for name in object_names}
    df["_obj_norm"] = df["Объект"].apply(normalize)
    
    # Находим объекты, которые есть в данных
    df_filtered = df[df["_obj_norm"].isin(objects_norm.keys())]
    
    if df_filtered.empty:
        # Район есть в справочнике, но показов нет
        district_info = district_objects[0]  # Берём информацию о районе
        return {
            "found": True,
            "no_showings": True,
            "query": district_query,
            "district": district_info.get("district"),
            "city": district_info.get("city"),
            "objects_in_district": object_names,
            "days": DAYS
        }
    
    # Группируем по ЖК и собираем брокеров
    results_by_object = {}
    
    for _, row in df_filtered.iterrows():
        obj_norm = row["_obj_norm"]
        obj_name = objects_norm.get(obj_norm, row["Объект"])
        broker = str(row["Брокер"]).strip() if pd.notna(row["Брокер"]) else ""
        
        if not broker:
            continue
            
        if obj_name not in results_by_object:
            results_by_object[obj_name] = set()
        results_by_object[obj_name].add(broker)
    
    # Формируем результат
    district_info = district_objects[0]
    
    return {
        "found": True,
        "query": district_query,
        "district": district_info.get("district"),
        "city": district_info.get("city"),
        "days": DAYS,
        "by_object": {obj: sorted(brokers) for obj, brokers in results_by_object.items()},
        "total_brokers": len(set().union(*results_by_object.values())) if results_by_object else 0
    }


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    await update.message.reply_text(
        "👋 Привет!\n\n"
        "Напиши название ЖК, и я покажу, какие брокеры вели там показы за последние 60 дней.\n\n"
        "Примеры:\n"
        "• Прайм парк — поиск по ЖК\n"
        "• район Хамовники — поиск по району"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    await update.message.reply_text(
        "🔍 Как пользоваться:\n\n"
        "**По ЖК:** просто напиши название\n"
        "Пример: Прайм парк\n\n"
        "**По району:** напиши «район» + название\n"
        "Пример: район Хамовники\n"
        "Пример: район Business Bay\n\n"
        "Можно писать с опечатками — я постараюсь угадать 😉",
        parse_mode="Markdown"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    query = update.message.text.strip()
    
    if not query:
        return
    
    # Проверяем: поиск по району?
    query_lower = query.lower()
    if query_lower.startswith("район ") or query_lower.startswith("район:"):
        # Извлекаем название района
        district_query = query[6:].strip().lstrip(":")
        if district_query:
            await handle_district_search(update, district_query)
            return
    
    # Обычный поиск по ЖК
    result = search_brokers(query)
    
    if "error" in result:
        await update.message.reply_text(f"❌ Ошибка: {result['error']}")
        return
    
    if not result["found"]:
        await update.message.reply_text(
            f"🔍 По запросу «{result['query']}» ничего не найдено.\n\n"
            f"Проверьте написание и попробуйте снова."
        )
        return
    
    # Проверяем: точное совпадение или fuzzy
    if not result.get("exact", True):
        # Fuzzy match — показываем ТОЛЬКО подсказку, без брокеров
        await update.message.reply_text(
            f"🔍 Точного совпадения не найдено.\n\n"
            f"Возможно, вы имели в виду: **{result['object']}**?\n\n"
            f"Введите точное название для поиска.",
            parse_mode="Markdown"
        )
        return
    
    # Точное совпадение — показываем брокеров
    brokers_list = "\n".join([f"• {b}" for b in result["brokers"]])
    
    # Показываем все найденные варианты объекта (если синонимы)
    found_objects = result.get("objects", [result["object"]])
    if len(found_objects) > 1:
        objects_str = " / ".join(found_objects)
        header = f"🏠 {objects_str}"
    else:
        header = f"🏠 {result['object']}"
    
    response = (
        f"{header}\n\n"
        f"За последние {result['days']} дней показы вели:\n"
        f"{brokers_list}"
    )
    
    await update.message.reply_text(response)


async def handle_district_search(update: Update, district_query: str):
    """Обработка поиска по району"""
    result = search_by_district(district_query)
    
    if "error" in result:
        await update.message.reply_text(f"❌ Ошибка: {result['error']}")
        return
    
    if not result["found"]:
        # Не нашли район
        if "suggestion" in result:
            await update.message.reply_text(
                f"🔍 Район «{result['query']}» не найден.\n\n"
                f"Возможно, вы имели в виду: **{result['suggestion']}**?",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                f"🔍 Район «{result['query']}» не найден в справочнике.\n\n"
                f"Проверьте написание и попробуйте снова."
            )
        return
    
    # Район найден, но нет показов
    if result.get("no_showings"):
        objects_list = "\n".join([f"• {obj}" for obj in result["objects_in_district"][:10]])
        if len(result["objects_in_district"]) > 10:
            objects_list += f"\n...и ещё {len(result['objects_in_district']) - 10}"
        
        await update.message.reply_text(
            f"📍 **{result['district']}** ({result['city']})\n\n"
            f"ЖК в этом районе:\n{objects_list}\n\n"
            f"❌ За последние {result['days']} дней показов в этих ЖК не было.",
            parse_mode="Markdown"
        )
        return
    
    # Есть показы — формируем ответ по ЖК
    response_parts = [f"📍 **{result['district']}** ({result['city']})\n"]
    response_parts.append(f"За последние {result['days']} дней:\n")
    
    for obj_name, brokers in result["by_object"].items():
        brokers_str = ", ".join(brokers)
        response_parts.append(f"\n🏠 **{obj_name}**:\n{brokers_str}")
    
    response_parts.append(f"\n\n_Всего брокеров: {result['total_brokers']}_")
    
    response = "".join(response_parts)
    
    # Telegram ограничение — 4096 символов
    if len(response) > 4000:
        # Отправляем частями
        await update.message.reply_text(
            f"📍 **{result['district']}** ({result['city']})\n\n"
            f"За последние {result['days']} дней найдено {len(result['by_object'])} ЖК с показами.\n"
            f"Всего брокеров: {result['total_brokers']}",
            parse_mode="Markdown"
        )
        
        # Отправляем детали по частям
        chunk = ""
        for obj_name, brokers in result["by_object"].items():
            brokers_str = ", ".join(brokers)
            line = f"🏠 **{obj_name}**:\n{brokers_str}\n\n"
            
            if len(chunk) + len(line) > 3500:
                await update.message.reply_text(chunk, parse_mode="Markdown")
                chunk = line
            else:
                chunk += line
        
        if chunk:
            await update.message.reply_text(chunk, parse_mode="Markdown")
    else:
        await update.message.reply_text(response, parse_mode="Markdown")


def main():
    """Запуск бота"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    
    if not token:
        print("❌ Ошибка: TELEGRAM_BOT_TOKEN не найден в .env")
        print("Добавь токен в файл .env")
        return
    
    print("🤖 Запуск бота...")
    
    # Создаём приложение
    app = Application.builder().token(token).build()
    
    # Регистрируем обработчики
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Запускаем
    print("✅ Бот запущен! Нажми Ctrl+C для остановки.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
