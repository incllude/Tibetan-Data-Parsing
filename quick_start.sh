#!/bin/bash
# Скрипт быстрого старта для парсера тибетских текстов

set -e

echo "=========================================="
echo "  ПАРСЕР ТИБЕТСКИХ ТЕКСТОВ - QUICK START"
echo "=========================================="
echo ""

# Проверка виртуального окружения
if [ ! -d "venv" ]; then
    echo "📦 Создание виртуального окружения..."
    python3 -m venv venv
fi

echo "🔧 Активация виртуального окружения..."
source venv/bin/activate

# Установка зависимостей
echo "📥 Установка зависимостей..."
pip install -q playwright aiohttp aiofiles

# Установка браузера
if [ ! -d "$HOME/Library/Caches/ms-playwright/chromium-"* ]; then
    echo "🌐 Установка браузера Chromium..."
    python -m playwright install chromium
else
    echo "✓ Браузер Chromium уже установлен"
fi

echo ""
echo "✅ Установка завершена!"
echo ""
echo "=========================================="
echo "  ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ"
echo "=========================================="
echo ""
echo "1. Тест на одной странице:"
echo "   python improved_parser.py --pages 1-1b --output test_output"
echo ""
echo "2. Парсинг первых 20 страниц:"
echo "   python improved_parser.py --start-page 1 --end-page 10 --output tibetan_data"
echo ""
echo "3. Анализ собранных данных:"
echo "   python analyze_dataset.py --dir tibetan_data"
echo ""
echo "=========================================="
echo ""

# Опционально: запустить тестовый парсинг
read -p "Запустить тестовый парсинг страницы 1-1b? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "🚀 Запуск тестового парсинга..."
    python improved_parser.py --pages 1-1b --output test_output
    
    echo ""
    echo "📊 Анализ результатов..."
    python analyze_dataset.py --dir test_output
fi

echo ""
echo "✓ Готово! Виртуальное окружение активно."
echo "  Для деактивации используйте: deactivate"
echo ""



