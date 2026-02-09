#!/usr/bin/env python3
"""
V1 Match CLI — поиск брокеров по объекту
"""
import argparse
import json
from matcher import find_brokers


def main():
    parser = argparse.ArgumentParser(
        description="Найти брокеров по показам объекта"
    )
    parser.add_argument(
        "--file", "-f",
        default="data/showings.xlsx",
        help="Путь к xlsx файлу с выгрузкой (по умолчанию: data/showings.xlsx)"
    )
    parser.add_argument(
        "--object", "-o",
        required=True,
        help="Название объекта для поиска"
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=14,
        help="Период в днях (по умолчанию: 14)"
    )
    parser.add_argument(
        "--match", "-m",
        choices=["exact", "contains"],
        default="contains",
        help="Режим поиска: exact или contains (по умолчанию: contains)"
    )
    parser.add_argument(
        "--exclude-status", "-e",
        help="Статус для исключения (например: Отменен)"
    )
    parser.add_argument(
        "--json", "-j",
        action="store_true",
        help="Вывод в формате JSON"
    )
    
    args = parser.parse_args()
    
    try:
        result = find_brokers(
            file_path=args.file,
            object_query=args.object,
            days=args.days,
            match_mode=args.match,
            exclude_status=args.exclude_status
        )
        
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"\nОбъект: {result['object']}")
            print(f"Период: {result['days']} дней")
            print(f"Режим: {result['match']}")
            print(f"\nБрокеры ({len(result['brokers'])}):")
            if result['brokers']:
                for broker in result['brokers']:
                    print(f"  - {broker}")
            else:
                print("  (не найдено)")
                
    except Exception as e:
        print(f"Ошибка: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
