#!/usr/bin/env python3
"""
Парсер для сбора изображений тибетских иероглифов с сайта adarshah.org
"""

import asyncio
import json
import os
import re
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import aiohttp
from playwright.async_api import async_playwright, Page, Browser
from urllib.parse import urljoin, urlparse, parse_qs


class TibetanScraper:
    """Класс для парсинга тибетских текстов и изображений"""
    
    def __init__(self, output_dir: str = "tibetan_data"):
        self.output_dir = Path(output_dir)
        self.images_dir = self.output_dir / "images"
        self.texts_dir = self.output_dir / "texts"
        self.metadata_file = self.output_dir / "metadata.json"
        
        # Создаем необходимые директории
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.texts_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_url = "https://online.adarshah.org/"
        self.metadata = []
        
    async def download_image(self, session: aiohttp.ClientSession, url: str, filename: str) -> bool:
        """Скачивание изображения"""
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    content = await response.read()
                    filepath = self.images_dir / filename
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    print(f"✓ Изображение сохранено: {filename}")
                    return True
                else:
                    print(f"✗ Ошибка загрузки изображения {url}: статус {response.status}")
                    return False
        except Exception as e:
            print(f"✗ Ошибка при скачивании {url}: {str(e)}")
            return False
    
    def save_text(self, page_id: str, text: str):
        """Сохранение текста в файл"""
        filepath = self.texts_dir / f"{page_id}.txt"
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f"✓ Текст сохранен: {page_id}.txt")
    
    def save_metadata(self):
        """Сохранение метаданных"""
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=2)
        print(f"✓ Метаданные сохранены: {len(self.metadata)} записей")
    
    async def extract_page_data(self, page: Page, page_id: str) -> Optional[Dict]:
        """
        Извлечение данных для конкретной страницы
        Возвращает словарь с данными о странице
        """
        try:
            # Ждем загрузки контента
            await page.wait_for_timeout(2000)
            
            # Пытаемся найти изображение страницы
            # Изображения обычно в элементах <img> с определенными классами/атрибутами
            page_data = await page.evaluate(f"""
                () => {{
                    const pageId = '{page_id}';
                    const result = {{
                        page_id: pageId,
                        image_url: null,
                        text: null,
                        found: false
                    }};
                    
                    // Ищем контейнер с данными страницы
                    // Структура может быть разной, пробуем несколько вариантов
                    
                    // Вариант 1: Ищем изображение по data-атрибутам или классам
                    const images = document.querySelectorAll('img');
                    for (const img of images) {{
                        // Проверяем src изображения на соответствие page_id
                        const src = img.src || img.dataset.src || '';
                        if (src && (src.includes(pageId) || src.includes(pageId.replace('-', '')))) {{
                            result.image_url = src;
                            result.found = true;
                            break;
                        }}
                    }}
                    
                    // Вариант 2: Ищем через canvas если изображение отрисовывается там
                    if (!result.found) {{
                        const canvases = document.querySelectorAll('canvas');
                        if (canvases.length > 0) {{
                            // Берем первый canvas как основное изображение
                            result.image_url = canvases[0].toDataURL('image/png');
                            result.found = true;
                        }}
                    }}
                    
                    // Ищем текст страницы
                    // Текст обычно находится в div с классом содержащим 'text', 'content' или подобное
                    const textContainers = document.querySelectorAll('[class*="text"], [class*="content"], [id*="text"], [id*="content"]');
                    
                    for (const container of textContainers) {{
                        const text = container.textContent.trim();
                        // Проверяем что текст тибетский (содержит тибетские символы)
                        if (text && /[\u0F00-\u0FFF]/.test(text)) {{
                            // Берем только релевантный текст (первые N символов или весь блок)
                            result.text = text;
                            break;
                        }}
                    }}
                    
                    // Если не нашли, пробуем найти любой тибетский текст на странице
                    if (!result.text) {{
                        const allText = document.body.textContent;
                        const tibetanMatch = allText.match(/[\u0F00-\u0FFF][^<>]{{100,}}/);
                        if (tibetanMatch) {{
                            result.text = tibetanMatch[0].trim();
                        }}
                    }}
                    
                    return result;
                }}
            """)
            
            return page_data
            
        except Exception as e:
            print(f"✗ Ошибка при извлечении данных страницы {page_id}: {str(e)}")
            return None
    
    async def scrape_page(self, page: Page, session: aiohttp.ClientSession, page_id: str) -> bool:
        """
        Парсинг одной страницы
        """
        try:
            print(f"\n→ Обработка страницы: {page_id}")
            
            # Формируем URL
            url = f"{self.base_url}index.html?kdb=degekangyur&sutra=d1&page={page_id}"
            
            # Переходим на страницу
            print(f"  Загрузка: {url}")
            await page.goto(url, wait_until='networkidle', timeout=30000)
            
            # Ждем загрузки контента
            await page.wait_for_timeout(3000)
            
            # Извлекаем данные
            page_data = await self.extract_page_data(page, page_id)
            
            if not page_data or not page_data.get('found'):
                print(f"✗ Не удалось найти данные для страницы {page_id}")
                return False
            
            # Сохраняем изображение
            image_url = page_data.get('image_url')
            if image_url:
                # Определяем имя файла
                image_filename = f"{page_id}.png"
                
                # Если это data URL, сохраняем напрямую
                if image_url.startswith('data:'):
                    import base64
                    # Извлекаем base64 данные
                    image_data = image_url.split(',')[1]
                    decoded = base64.b64decode(image_data)
                    filepath = self.images_dir / image_filename
                    with open(filepath, 'wb') as f:
                        f.write(decoded)
                    print(f"✓ Изображение сохранено: {image_filename}")
                else:
                    # Скачиваем по URL
                    full_url = urljoin(self.base_url, image_url)
                    await self.download_image(session, full_url, image_filename)
            
            # Сохраняем текст
            text = page_data.get('text')
            if text:
                self.save_text(page_id, text)
            else:
                print(f"⚠ Текст не найден для страницы {page_id}")
            
            # Сохраняем метаданные
            metadata_entry = {
                'page_id': page_id,
                'image_file': f"{page_id}.png" if image_url else None,
                'text_file': f"{page_id}.txt" if text else None,
                'url': url,
                'scraped_at': datetime.now().isoformat(),
                'text_preview': text[:100] if text else None
            }
            self.metadata.append(metadata_entry)
            
            return True
            
        except Exception as e:
            print(f"✗ Ошибка при обработке страницы {page_id}: {str(e)}")
            return False
    
    def generate_page_ids(self, start_vol: int, end_vol: int, start_page: int, end_page: int) -> List[str]:
        """
        Генерация списка ID страниц
        Формат: {vol}-{page}{a/b}
        Например: 1-1b, 1-2a, 1-2b, ...
        """
        page_ids = []
        for vol in range(start_vol, end_vol + 1):
            for page_num in range(start_page, end_page + 1):
                page_ids.append(f"{vol}-{page_num}a")
                page_ids.append(f"{vol}-{page_num}b")
        return page_ids
    
    async def run(self, page_ids: Optional[List[str]] = None, max_pages: Optional[int] = None):
        """
        Основной метод запуска парсера
        
        Args:
            page_ids: Список ID страниц для парсинга. Если None, используется диапазон по умолчанию
            max_pages: Максимальное количество страниц для парсинга
        """
        if page_ids is None:
            # По умолчанию парсим первые несколько страниц для теста
            page_ids = self.generate_page_ids(1, 1, 1, 10)
        
        if max_pages:
            page_ids = page_ids[:max_pages]
        
        print(f"=== Начало парсинга ===")
        print(f"Количество страниц: {len(page_ids)}")
        print(f"Директория вывода: {self.output_dir.absolute()}")
        
        async with async_playwright() as p:
            # Запускаем браузер
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            async with aiohttp.ClientSession() as session:
                success_count = 0
                for i, page_id in enumerate(page_ids, 1):
                    print(f"\n[{i}/{len(page_ids)}]", end=" ")
                    
                    success = await self.scrape_page(page, session, page_id)
                    if success:
                        success_count += 1
                    
                    # Небольшая пауза между запросами
                    await asyncio.sleep(1)
                
                # Сохраняем метаданные
                self.save_metadata()
            
            await browser.close()
        
        print(f"\n=== Парсинг завершен ===")
        print(f"Успешно обработано: {success_count}/{len(page_ids)}")
        print(f"Результаты сохранены в: {self.output_dir.absolute()}")


async def main():
    """Точка входа"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Парсер тибетских иероглифов с adarshah.org')
    parser.add_argument('--output', '-o', default='tibetan_data', help='Директория для сохранения данных')
    parser.add_argument('--start-vol', type=int, default=1, help='Начальный том')
    parser.add_argument('--end-vol', type=int, default=1, help='Конечный том')
    parser.add_argument('--start-page', type=int, default=1, help='Начальная страница')
    parser.add_argument('--end-page', type=int, default=10, help='Конечная страница')
    parser.add_argument('--max-pages', type=int, help='Максимальное количество страниц')
    parser.add_argument('--pages', nargs='+', help='Конкретные страницы для парсинга (например: 1-1b 1-2a)')
    
    args = parser.parse_args()
    
    scraper = TibetanScraper(output_dir=args.output)
    
    if args.pages:
        page_ids = args.pages
    else:
        page_ids = scraper.generate_page_ids(
            args.start_vol, args.end_vol,
            args.start_page, args.end_page
        )
    
    await scraper.run(page_ids=page_ids, max_pages=args.max_pages)


if __name__ == "__main__":
    asyncio.run(main())


