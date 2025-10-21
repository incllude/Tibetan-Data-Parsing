#!/usr/bin/env python3
"""
Улучшенный парсер для сайта adarshah.org
Специально адаптирован для структуры этого сайта
"""

import asyncio
import json
import os
import re
import base64
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import aiohttp
from playwright.async_api import async_playwright, Page, Browser, ElementHandle
from urllib.parse import urljoin, urlparse, parse_qs


class ImprovedTibetanScraper:
    """Улучшенный парсер с точным сопоставлением изображений и текстов"""
    
    def __init__(self, output_dir: str = "tibetan_data", kdb: str = "degekangyur", sutra: str = "d1",
                 image_format: str = "png", jpeg_quality: int = 95):
        self.output_dir = Path(output_dir)
        self.images_dir = self.output_dir / "images"
        self.texts_dir = self.output_dir / "texts"
        self.metadata_file = self.output_dir / "metadata.json"
        self.raw_dir = self.output_dir / "raw_html"  # Для отладки
        
        # Создаем необходимые директории
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.texts_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_url = "https://online.adarshah.org/"
        self.kdb = kdb  # Каталог (degekangyur, degetengyur и т.д.)
        self.sutra = sutra  # Сутра (d1, D1109 и т.д.)
        self.image_format = image_format.lower()  # 'png' или 'jpeg'
        self.jpeg_quality = jpeg_quality  # Качество JPEG (1-100)
        self.metadata = []
        
    async def wait_for_page_load(self, page: Page, timeout: int = 30000):
        """Ожидание полной загрузки страницы с проверкой контента"""
        try:
            # Ждем загрузки основного контента
            await page.wait_for_load_state('networkidle', timeout=timeout)
            
            # Ждем появления canvas или изображения (признак того, что контент загрузился)
            try:
                await page.wait_for_selector('canvas, img[src*="jpg"], img[src*="png"]', 
                                             timeout=15000, state='visible')
                print(f"  ✓ Контент загружен")
            except Exception:
                print(f"  ⚠ Canvas/изображение не появились в течение 15 сек")
            
            # Дополнительное время для рендеринга
            await page.wait_for_timeout(5000)
        except Exception as e:
            print(f"  ⚠ Таймаут ожидания загрузки: {str(e)}")
    
    async def extract_image_from_canvas(self, page: Page) -> Optional[str]:
        """Извлечение изображения из canvas элемента"""
        try:
            # Формируем параметры для canvas в зависимости от формата
            if self.image_format == 'jpeg':
                mime_type = 'image/jpeg'
                quality = self.jpeg_quality / 100.0
            else:
                mime_type = 'image/png'
                quality = 1.0  # PNG не использует quality, но передаем для единообразия
            
            # Пытаемся найти canvas и получить его содержимое
            canvas_data = await page.evaluate("""
                ({mimeType, quality}) => {
                    const canvas = document.querySelector('canvas');
                    if (canvas) {
                        try {
                            if (mimeType === 'image/png') {
                                return canvas.toDataURL(mimeType);
                            } else {
                                return canvas.toDataURL(mimeType, quality);
                            }
                        } catch (e) {
                            console.error('Error getting canvas data:', e);
                            return null;
                        }
                    }
                    return null;
                }
            """, {'mimeType': mime_type, 'quality': quality})
            return canvas_data
        except Exception as e:
            print(f"  ✗ Ошибка извлечения из canvas: {str(e)}")
            return None
    
    async def find_page_image(self, page: Page, page_id: str) -> Optional[Tuple[str, str]]:
        """
        Поиск изображения страницы
        Возвращает: (image_data, source_type) где source_type = 'canvas' | 'img' | 'screenshot'
        """
        # Способ 1: Извлечение из canvas
        canvas_data = await self.extract_image_from_canvas(page)
        if canvas_data:
            return (canvas_data, 'canvas')
        
        # Способ 2: Поиск img элемента
        try:
            img_src = await page.evaluate("""
                (pageId) => {
                    const images = document.querySelectorAll('img');
                    for (const img of images) {
                        const src = img.src || img.dataset.src || '';
                        // Проверяем различные варианты совпадения
                        if (src && (
                            src.includes(pageId) || 
                            src.includes(pageId.replace('-', '')) ||
                            img.alt === pageId ||
                            img.id === pageId
                        )) {
                            return img.src;
                        }
                    }
                    // Если не нашли конкретное изображение, берем первое крупное
                    for (const img of images) {
                        if (img.width > 300 && img.height > 300) {
                            return img.src;
                        }
                    }
                    return null;
                }
            """, page_id)
            
            if img_src:
                return (img_src, 'img')
        except Exception as e:
            print(f"  ✗ Ошибка поиска img: {str(e)}")
        
        # Способ 3: Скриншот видимой области с текстом
        try:
            # Находим область с изображением текста
            element = await page.query_selector('body')
            if element:
                # Создаем скриншот в нужном формате
                if self.image_format == 'jpeg':
                    screenshot = await element.screenshot(type='jpeg', quality=self.jpeg_quality)
                    screenshot_b64 = base64.b64encode(screenshot).decode()
                    return (f"data:image/jpeg;base64,{screenshot_b64}", 'screenshot')
                else:
                    screenshot = await element.screenshot(type='png')
                    screenshot_b64 = base64.b64encode(screenshot).decode()
                    return (f"data:image/png;base64,{screenshot_b64}", 'screenshot')
        except Exception as e:
            print(f"  ✗ Ошибка создания скриншота: {str(e)}")
        
        return None
    
    async def extract_tibetan_text(self, page: Page, page_id: str) -> Optional[str]:
        """
        Извлечение тибетского текста для конкретной страницы
        Использует маркеры <jp> и data-pbname для точного определения текста
        """
        try:
            text_data = await page.evaluate("""
                (pageId) => {
                    // Преобразуем page_id в формат для jp маркера
                    // Например: "1-1b" -> "1-1-1b"
                    const parts = pageId.split('-');
                    let jpId;
                    if (parts.length === 2) {
                        // Формат: "1-1b" -> "1-1-1b"
                        jpId = parts[0] + '-' + parts[1].slice(0, -1) + '-' + parts[1];
                    } else {
                        jpId = pageId;
                    }
                    
                    // Метод 1: Поиск по маркерам <jp>
                    const jpStart = document.querySelector(`jp[id="${jpId}"]`);
                    let textByJp = '';
                    
                    if (jpStart) {
                        // Находим следующий jp маркер для определения границы
                        let currentNode = jpStart.nextSibling;
                        while (currentNode) {
                            // Проверяем является ли это следующим jp маркером
                            if (currentNode.nodeName === 'JP') {
                                break;
                            }
                            
                            // Извлекаем текст
                            if (currentNode.nodeType === Node.TEXT_NODE) {
                                textByJp += currentNode.textContent;
                            } else if (currentNode.nodeType === Node.ELEMENT_NODE) {
                                textByJp += currentNode.textContent;
                            }
                            
                            currentNode = currentNode.nextSibling;
                        }
                    }
                    
                    // Метод 2: Поиск по атрибуту data-pbname
                    const textElements = document.querySelectorAll(`span.text-pb[data-pbname="${pageId}"]`);
                    let textByAttr = '';
                    
                    textElements.forEach(el => {
                        textByAttr += el.textContent;
                    });
                    
                    // Выбираем наиболее подходящий текст
                    let finalText = '';
                    let method = '';
                    
                    // Очистка текста от лишних элементов
                    function cleanText(text) {
                        // Удаляем ID страниц вида "1-2a"
                        text = text.replace(/\\d+-\\d+[ab]/g, '');
                        // Удаляем множественные пробелы и переносы
                        text = text.replace(/\\s+/g, ' ');
                        return text.trim();
                    }
                    
                    if (textByJp && /[\u0F00-\u0FFF]/.test(textByJp)) {
                        finalText = cleanText(textByJp);
                        method = 'jp-markers';
                    } else if (textByAttr && /[\u0F00-\u0FFF]/.test(textByAttr)) {
                        finalText = cleanText(textByAttr);
                        method = 'data-pbname';
                    }
                    
                    if (finalText) {
                        return {
                            text: finalText,
                            method: method,
                            jp_id: jpId,
                            elements_found: textElements.length
                        };
                    }
                    
                    return null;
                }
            """, page_id)
            
            if text_data and text_data.get('text'):
                print(f"  ℹ Метод извлечения: {text_data['method']}")
                print(f"  ℹ JP ID: {text_data['jp_id']}")
                if text_data.get('elements_found'):
                    print(f"  ℹ Найдено элементов: {text_data['elements_found']}")
                return text_data['text']
            
            return None
            
        except Exception as e:
            print(f"  ✗ Ошибка извлечения текста: {str(e)}")
            return None
    
    def save_image(self, image_data: str, filename: str) -> bool:
        """Сохранение изображения из data URL или обычного URL"""
        try:
            filepath = self.images_dir / filename
            
            if image_data.startswith('data:'):
                # Это data URL, декодируем base64
                header, encoded = image_data.split(',', 1)
                decoded = base64.b64decode(encoded)
                with open(filepath, 'wb') as f:
                    f.write(decoded)
            else:
                # Это должен быть уже декодированный контент
                with open(filepath, 'wb') as f:
                    f.write(image_data)
            
            print(f"  ✓ Изображение сохранено: {filename}")
            return True
            
        except Exception as e:
            print(f"  ✗ Ошибка сохранения изображения: {str(e)}")
            return False
    
    async def download_image_url(self, session: aiohttp.ClientSession, url: str, filename: str) -> bool:
        """Скачивание изображения по URL"""
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    content = await response.read()
                    filepath = self.images_dir / filename
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    print(f"  ✓ Изображение загружено: {filename}")
                    return True
                else:
                    print(f"  ✗ Ошибка загрузки: статус {response.status}")
                    return False
        except Exception as e:
            print(f"  ✗ Ошибка загрузки: {str(e)}")
            return False
    
    def save_text(self, page_id: str, text: str) -> bool:
        """Сохранение текста в файл"""
        try:
            filepath = self.texts_dir / f"{page_id}.txt"
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(text)
            print(f"  ✓ Текст сохранен: {page_id}.txt ({len(text)} символов)")
            return True
        except Exception as e:
            print(f"  ✗ Ошибка сохранения текста: {str(e)}")
            return False
    
    async def save_page_html(self, page: Page, page_id: str):
        """Сохранение HTML для отладки"""
        try:
            html = await page.content()
            filepath = self.raw_dir / f"{page_id}.html"
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html)
        except Exception as e:
            print(f"  ⚠ Не удалось сохранить HTML: {str(e)}")
    
    def save_metadata(self):
        """Сохранение метаданных"""
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Метаданные сохранены: {len(self.metadata)} записей")
    
    async def scrape_page(self, page: Page, session: aiohttp.ClientSession, page_id: str, 
                         max_retries: int = 3) -> bool:
        """
        Парсинг одной страницы с механизмом повторных попыток
        """
        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    print(f"\n  🔄 Попытка {attempt}/{max_retries}")
                    await asyncio.sleep(5)  # Пауза перед повторной попыткой
                
                print(f"\n{'='*60}")
                print(f"→ Обработка страницы: {page_id}")
                print(f"{'='*60}")
                
                # Формируем URL с параметрами каталога и сутры
                url = f"{self.base_url}index.html?kdb={self.kdb}&sutra={self.sutra}&page={page_id}"
                print(f"  URL: {url}")
                
                # Переходим на страницу с увеличенным timeout
                await page.goto(url, wait_until='domcontentloaded', timeout=60000)
                await self.wait_for_page_load(page)
            
                # Сохраняем HTML для отладки
                await self.save_page_html(page, page_id)
                
                # Извлекаем изображение
                print(f"\n  → Поиск изображения...")
                image_result = await self.find_page_image(page, page_id)
                
                image_saved = False
                # Определяем расширение файла в зависимости от формата
                file_extension = 'jpg' if self.image_format == 'jpeg' else 'png'
                image_filename = f"{page_id}.{file_extension}"
                image_source = None
                
                if image_result:
                    image_data, source_type = image_result
                    image_source = source_type
                    print(f"  ℹ Источник изображения: {source_type}")
                    
                    # Проверяем что это не просто пустой скриншот
                    if source_type == 'screenshot':
                        print(f"  ⚠ Получен скриншот вместо canvas/img - возможно страница не загрузилась")
                        # Если это первая попытка, пробуем еще раз
                        if attempt < max_retries:
                            continue
                    
                    if source_type == 'img' and not image_data.startswith('data:'):
                        # Это URL, нужно скачать
                        full_url = urljoin(self.base_url, image_data)
                        image_saved = await self.download_image_url(session, full_url, image_filename)
                    else:
                        # Это data URL или уже готовые данные
                        image_saved = self.save_image(image_data, image_filename)
                else:
                    print(f"  ✗ Изображение не найдено")
                    if attempt < max_retries:
                        continue
                
                # Извлекаем текст
                print(f"\n  → Поиск текста...")
                text = await self.extract_tibetan_text(page, page_id)
                
                text_saved = False
                if text:
                    # Показываем превью текста
                    preview = text[:150] + "..." if len(text) > 150 else text
                    print(f"  ℹ Превью: {preview}")
                    text_saved = self.save_text(page_id, text)
                else:
                    print(f"  ✗ Текст не найден")
                    # Если нет ни изображения ни текста, пробуем еще раз
                    if not image_saved and attempt < max_retries:
                        continue
                
                # Сохраняем метаданные
                metadata_entry = {
                    'page_id': page_id,
                    'image_file': image_filename if image_saved else None,
                    'image_source': image_source,
                    'text_file': f"{page_id}.txt" if text_saved else None,
                    'text_length': len(text) if text else 0,
                    'text_preview': text[:200] if text else None,
                    'url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'success': image_saved or text_saved,
                    'attempts': attempt
                }
                self.metadata.append(metadata_entry)
                
                success = image_saved and text_saved
                
                if success:
                    print(f"\n  ✅ Страница успешно обработана")
                    return True
                elif image_saved or text_saved:
                    print(f"\n  ⚠ Страница обработана частично")
                    return False
                else:
                    # Ничего не получили, пробуем еще раз
                    if attempt < max_retries:
                        print(f"\n  ⚠ Ничего не получено, повторная попытка...")
                        continue
                    else:
                        print(f"\n  ✗ Не удалось получить данные после {max_retries} попыток")
                        return False
                
            except Exception as e:
                print(f"\n  ✗ Ошибка при обработке (попытка {attempt}/{max_retries}): {str(e)}")
                if attempt < max_retries:
                    import traceback
                    traceback.print_exc()
                    continue
                else:
                    import traceback
                    traceback.print_exc()
                    return False
        
        return False
    
    def generate_page_ids(self, start_vol: int, end_vol: int, start_page: int, end_page: int) -> List[str]:
        """
        Генерация списка ID страниц
        Формат: {vol}-{page}{a/b}
        """
        page_ids = []
        for vol in range(start_vol, end_vol + 1):
            for page_num in range(start_page, end_page + 1):
                page_ids.append(f"{vol}-{page_num}a")
                page_ids.append(f"{vol}-{page_num}b")
        return page_ids
    
    async def run(self, page_ids: Optional[List[str]] = None, max_pages: Optional[int] = None, 
                  headless: bool = True):
        """
        Основной метод запуска парсера
        """
        if page_ids is None:
            page_ids = self.generate_page_ids(1, 1, 1, 5)
        
        if max_pages:
            page_ids = page_ids[:max_pages]
        
        print(f"\n{'#'*60}")
        print(f"# ПАРСЕР ТИБЕТСКИХ ТЕКСТОВ")
        print(f"{'#'*60}")
        print(f"Количество страниц: {len(page_ids)}")
        print(f"Каталог: {self.kdb}, Сутра: {self.sutra}")
        print(f"Директория вывода: {self.output_dir.absolute()}")
        print(f"Формат изображений: {self.image_format.upper()}" + 
              (f" (качество: {self.jpeg_quality}%)" if self.image_format == 'jpeg' else ""))
        print(f"Режим браузера: {'headless' if headless else 'visible'}")
        print(f"{'#'*60}\n")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()
            
            async with aiohttp.ClientSession() as session:
                success_count = 0
                partial_count = 0
                fail_count = 0
                
                for i, page_id in enumerate(page_ids, 1):
                    print(f"\n[{i}/{len(page_ids)}]")
                    
                    try:
                        success = await self.scrape_page(page, session, page_id)
                        
                        if success:
                            success_count += 1
                        else:
                            # Проверяем был ли хоть какой-то успех
                            if self.metadata and self.metadata[-1].get('success'):
                                partial_count += 1
                            else:
                                fail_count += 1
                        
                        # Пауза между запросами
                        await asyncio.sleep(2)
                        
                    except KeyboardInterrupt:
                        print("\n\n⚠ Прервано пользователем")
                        break
                    except Exception as e:
                        print(f"\n  ✗ Необработанная ошибка: {str(e)}")
                        fail_count += 1
                        continue
                
                # Сохраняем метаданные
                self.save_metadata()
            
            await browser.close()
        
        # Итоговая статистика
        print(f"\n{'#'*60}")
        print(f"# РЕЗУЛЬТАТЫ ПАРСИНГА")
        print(f"{'#'*60}")
        print(f"Всего страниц: {len(page_ids)}")
        print(f"✅ Полностью успешно: {success_count}")
        print(f"⚠ Частично успешно: {partial_count}")
        print(f"✗ Неудачно: {fail_count}")
        print(f"\nДанные сохранены в: {self.output_dir.absolute()}")
        print(f"  - Изображения: {self.images_dir}")
        print(f"  - Тексты: {self.texts_dir}")
        print(f"  - Метаданные: {self.metadata_file}")
        print(f"{'#'*60}\n")


async def main():
    """Точка входа"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Улучшенный парсер тибетских иероглифов с adarshah.org',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Тест на одной странице (каталог по умолчанию - degekangyur)
  python improved_parser.py --pages 1-1b
  
  # Парсинг из каталога degetengyur, сутра D1109
  python improved_parser.py --kdb degetengyur --sutra D1109 --pages 1-1b
  
  # Парсинг с сохранением в JPEG (меньше размер)
  python improved_parser.py --pages 1-1b --image-format jpeg
  
  # JPEG с качеством 85% (еще меньше размер)
  python improved_parser.py --pages 1-1b --image-format jpeg --jpeg-quality 85
  
  # Парсинг первых 10 страниц из другого каталога в JPEG
  python improved_parser.py --kdb degetengyur --sutra D1109 --start-page 1 --end-page 5 --image-format jpeg
  
  # Парсинг с видимым браузером (для отладки)
  python improved_parser.py --kdb degetengyur --sutra D1109 --pages 1-1b --no-headless
        """
    )
    
    parser.add_argument('--output', '-o', default='tibetan_data', 
                       help='Директория для сохранения данных')
    parser.add_argument('--kdb', default='degekangyur',
                       help='Каталог (например: degekangyur, degetengyur)')
    parser.add_argument('--sutra', default='d1',
                       help='Сутра (например: d1, D1109)')
    parser.add_argument('--image-format', choices=['png', 'jpeg'], default='png',
                       help='Формат изображений: png или jpeg (по умолчанию: png)')
    parser.add_argument('--jpeg-quality', type=int, default=95, 
                       help='Качество JPEG от 1 до 100 (по умолчанию: 95)')
    parser.add_argument('--start-vol', type=int, default=1, 
                       help='Начальный том (по умолчанию: 1)')
    parser.add_argument('--end-vol', type=int, default=1, 
                       help='Конечный том (по умолчанию: 1)')
    parser.add_argument('--start-page', type=int, default=1, 
                       help='Начальная страница (по умолчанию: 1)')
    parser.add_argument('--end-page', type=int, default=5, 
                       help='Конечная страница (по умолчанию: 5)')
    parser.add_argument('--max-pages', type=int, 
                       help='Максимальное количество страниц')
    parser.add_argument('--pages', nargs='+', 
                       help='Конкретные страницы (например: 1-1b 1-2a)')
    parser.add_argument('--no-headless', action='store_true',
                       help='Показывать браузер (для отладки)')
    
    args = parser.parse_args()
    
    scraper = ImprovedTibetanScraper(
        output_dir=args.output, 
        kdb=args.kdb, 
        sutra=args.sutra,
        image_format=args.image_format,
        jpeg_quality=args.jpeg_quality
    )
    
    if args.pages:
        page_ids = args.pages
    else:
        page_ids = scraper.generate_page_ids(
            args.start_vol, args.end_vol,
            args.start_page, args.end_page
        )
    
    await scraper.run(
        page_ids=page_ids, 
        max_pages=args.max_pages,
        headless=not args.no_headless
    )


if __name__ == "__main__":
    asyncio.run(main())

