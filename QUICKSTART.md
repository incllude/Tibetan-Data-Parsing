# 🚀 Быстрый старт

## Автоматическая установка и тест

```bash
chmod +x quick_start.sh
./quick_start.sh
```

Этот скрипт:
1. Создаст виртуальное окружение
2. Установит все зависимости
3. Установит браузер Chromium
4. Предложит запустить тестовый парсинг

## Ручная установка за 3 шага

### Шаг 1: Создание окружения

```bash
python3 -m venv venv
source venv/bin/activate
```

### Шаг 2: Установка зависимостей

```bash
pip install playwright aiohttp aiofiles
python -m playwright install chromium
```

### Шаг 3: Первый запуск

```bash
python improved_parser.py --pages 1-1b --output test_output
```

## Проверка результатов

```bash
# Посмотреть собранные файлы
ls -lh test_output/images/
ls -lh test_output/texts/

# Прочитать метаданные
cat test_output/metadata.json

# Прочитать текст первой страницы
cat test_output/texts/1-1b.txt

# Анализ датасета
python analyze_dataset.py --dir test_output
```

## Частые команды

### Сбор данных

```bash
# Парсинг первых 10 страниц тома 1
python improved_parser.py --start-page 1 --end-page 10

# Парсинг конкретных страниц
python improved_parser.py --pages 1-1b 1-2a 1-2b 1-3a

# Парсинг с ограничением
python improved_parser.py --start-page 1 --end-page 50 --max-pages 20

# Парсинг нескольких томов
python improved_parser.py --start-vol 1 --end-vol 3 --start-page 1 --end-page 100
```

### Отладка

```bash
# Парсинг с видимым браузером
python improved_parser.py --pages 1-1b --no-headless

# Исследование структуры сайта
python inspect_site.py
```

### Анализ

```bash
# Полный анализ датасета
python analyze_dataset.py --dir tibetan_data

# Проверка конкретной страницы
cat tibetan_data/texts/1-1b.txt | head -3
```

## Ожидаемый результат

После успешного парсинга вы должны увидеть:

```
############################################################
# РЕЗУЛЬТАТЫ ПАРСИНГА
############################################################
Всего страниц: 1
✅ Полностью успешно: 1
⚠ Частично успешно: 0
✗ Неудачно: 0
```

И структуру файлов:

```
tibetan_data/
├── images/
│   └── 1-1b.png          # Изображение страницы
├── texts/
│   └── 1-1b.txt          # Текстовая расшифровка
├── raw_html/
│   └── 1-1b.html         # HTML для отладки
└── metadata.json         # Метаданные всех страниц
```

## Проверка качества

Текст страницы 1-1b должен начинаться с:

```
༄༅༅། །རྒྱ་གར་སྐད་དུ། བི་ན་ཡ་བསྟུ། བོད་སྐད་དུ། འདུལ་བ་གཞི།
```

Проверить:

```bash
head -1 test_output/texts/1-1b.txt
```

## Решение проблем

### "Command not found: python3"

```bash
# Используйте python вместо python3
python -m venv venv
```

### "pip: command not found"

```bash
# Активируйте окружение
source venv/bin/activate
```

### Браузер не запускается

```bash
# Переустановите браузер
python -m playwright install --force chromium
```

### Пустые результаты

```bash
# Проверьте подключение к интернету
curl -I https://online.adarshah.org/

# Запустите с видимым браузером для отладки
python improved_parser.py --pages 1-1b --no-headless
```

## Следующие шаги

1. ✅ Протестируйте парсер на нескольких страницах
2. 📊 Запустите полный сбор данных (может занять несколько часов)
3. 🔍 Проанализируйте собранный датасет
4. 🎨 Используйте данные для обучения генеративной модели

## Полная документация

Смотрите [README.md](README.md) для полной документации.

---

**Важно**: При использовании данных указывайте источник: [adarshah.org](https://online.adarshah.org/)



