#!/usr/bin/env python3
"""
Скрипт для анализа собранного датасета тибетских текстов
"""

import json
import sys
from pathlib import Path
from collections import defaultdict


def analyze_dataset(data_dir: str = "tibetan_data"):
    """Анализ собранного датасета"""
    
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"❌ Директория {data_dir} не существует")
        return
    
    # Читаем метаданные
    metadata_file = data_path / "metadata.json"
    if not metadata_file.exists():
        print(f"❌ Файл метаданных не найден: {metadata_file}")
        return
    
    with open(metadata_file, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    print(f"\n{'='*70}")
    print(f"АНАЛИЗ ДАТАСЕТА: {data_dir}")
    print(f"{'='*70}\n")
    
    # Общая статистика
    total_pages = len(metadata)
    pages_with_images = sum(1 for m in metadata if m.get('image_file'))
    pages_with_text = sum(1 for m in metadata if m.get('text_file'))
    fully_complete = sum(1 for m in metadata if m.get('success'))
    
    print(f"📊 ОБЩАЯ СТАТИСТИКА")
    print(f"  Всего страниц: {total_pages}")
    print(f"  Страниц с изображениями: {pages_with_images} ({pages_with_images/total_pages*100:.1f}%)")
    print(f"  Страниц с текстом: {pages_with_text} ({pages_with_text/total_pages*100:.1f}%)")
    print(f"  Полностью завершенных: {fully_complete} ({fully_complete/total_pages*100:.1f}%)")
    
    # Статистика по текстам
    text_lengths = [m['text_length'] for m in metadata if m.get('text_length', 0) > 0]
    if text_lengths:
        avg_length = sum(text_lengths) / len(text_lengths)
        min_length = min(text_lengths)
        max_length = max(text_lengths)
        
        print(f"\n📝 СТАТИСТИКА ПО ТЕКСТАМ")
        print(f"  Средняя длина: {avg_length:.0f} символов")
        print(f"  Минимальная длина: {min_length} символов")
        print(f"  Максимальная длина: {max_length} символов")
        print(f"  Общий объем текста: {sum(text_lengths):,} символов")
    
    # Статистика по источникам изображений
    image_sources = defaultdict(int)
    for m in metadata:
        if m.get('image_source'):
            image_sources[m['image_source']] += 1
    
    if image_sources:
        print(f"\n🖼️  ИСТОЧНИКИ ИЗОБРАЖЕНИЙ")
        for source, count in image_sources.items():
            print(f"  {source}: {count} ({count/total_pages*100:.1f}%)")
    
    # Статистика по томам
    volumes = defaultdict(int)
    for m in metadata:
        page_id = m['page_id']
        vol = page_id.split('-')[0]
        volumes[vol] += 1
    
    if volumes:
        print(f"\n📚 СТАТИСТИКА ПО ТОМАМ")
        for vol in sorted(volumes.keys(), key=int):
            print(f"  Том {vol}: {volumes[vol]} страниц")
    
    # Примеры страниц
    print(f"\n📄 ПРИМЕРЫ СТРАНИЦ")
    for i, m in enumerate(metadata[:5], 1):
        print(f"\n  {i}. {m['page_id']}")
        print(f"     Изображение: {'✓' if m.get('image_file') else '✗'}")
        print(f"     Текст: {'✓' if m.get('text_file') else '✗'} ({m.get('text_length', 0)} символов)")
        if m.get('text_preview'):
            preview = m['text_preview'][:80] + "..." if len(m['text_preview']) > 80 else m['text_preview']
            print(f"     Превью: {preview}")
    
    # Файловая структура
    images_dir = data_path / "images"
    texts_dir = data_path / "texts"
    
    image_files = list(images_dir.glob("*.png")) if images_dir.exists() else []
    text_files = list(texts_dir.glob("*.txt")) if texts_dir.exists() else []
    
    print(f"\n💾 ФАЙЛОВАЯ СИСТЕМА")
    print(f"  Изображений на диске: {len(image_files)}")
    print(f"  Текстовых файлов: {len(text_files)}")
    
    # Размер датасета
    total_size = 0
    for f in data_path.rglob("*"):
        if f.is_file():
            total_size += f.stat().st_size
    
    size_mb = total_size / (1024 * 1024)
    print(f"  Общий размер: {size_mb:.2f} МБ")
    
    # Проблемные страницы
    problematic = [m for m in metadata if not m.get('success')]
    if problematic:
        print(f"\n⚠️  ПРОБЛЕМНЫЕ СТРАНИЦЫ ({len(problematic)})")
        for m in problematic[:10]:
            print(f"  - {m['page_id']}: ", end="")
            issues = []
            if not m.get('image_file'):
                issues.append("нет изображения")
            if not m.get('text_file'):
                issues.append("нет текста")
            print(", ".join(issues))
        
        if len(problematic) > 10:
            print(f"  ... и еще {len(problematic) - 10}")
    
    print(f"\n{'='*70}\n")
    
    # Рекомендации
    if fully_complete == total_pages:
        print("✅ Датасет полностью собран! Все страницы содержат и изображения и тексты.")
    elif fully_complete / total_pages > 0.9:
        print("✓ Датасет в хорошем состоянии. Большинство страниц собрано успешно.")
    else:
        print("⚠ В датасете есть проблемы. Рекомендуется повторный запуск парсера.")
    
    print(f"\nДиректория датасета: {data_path.absolute()}")
    print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Анализ собранного датасета')
    parser.add_argument('--dir', '-d', default='tibetan_data', 
                       help='Директория с данными (по умолчанию: tibetan_data)')
    
    args = parser.parse_args()
    
    analyze_dataset(args.dir)



