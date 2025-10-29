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
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import aiohttp
from playwright.async_api import async_playwright, Page, Browser, ElementHandle
from urllib.parse import urljoin, urlparse, parse_qs


class ImprovedTibetanScraper:
    """Улучшенный парсер с точным сопоставлением изображений и текстов"""
    
    def __init__(self, output_dir: str = "tibetan_data", kdb: str = "degekangyur", sutra: str = "d1",
                 image_format: str = "png", jpeg_quality: int = 95, delay_between_pages: float = 2.0,
                 volume_sutras: Optional[Dict[int, str]] = None, auto_sutra: bool = False, 
                 max_sutra_attempts: int = 10, max_failed_pages: int = 5, quiet_mode: bool = False):
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
        self.sutra = sutra  # Сутра по умолчанию (d1, D1109 и т.д.)
        self.volume_sutras = volume_sutras or {}  # Сопоставление volume -> sutra
        self.auto_sutra = auto_sutra  # Автоматический подбор sutra
        self.max_sutra_attempts = max_sutra_attempts  # Максимальное количество попыток инкремента sutra
        self.image_format = image_format.lower()  # 'png' или 'jpeg'
        self.jpeg_quality = jpeg_quality  # Качество JPEG (1-100)
        self.delay_between_pages = delay_between_pages  # Задержка между запросами (секунды)
        self.max_failed_pages = max_failed_pages  # Максимальное количество неудачных страниц подряд перед переходом к следующему volume
        self.quiet_mode = quiet_mode  # Тихий режим - выводить только ошибки и предупреждения
        self.metadata = []
        self.last_successful_sutra = sutra  # Последняя успешно найденная sutra (для оптимизации автоподбора)
        
        # Отслеживание неудачных попыток для автоматического пропуска volume
        self.current_volume = None
        self.failed_pages_in_volume = 0
        
        # КЭШ для оптимизации: сохраняем загруженный HTML и страницу Playwright
        self.cached_html = None  # Кэшированный HTML контент
        self.cached_page_id = None  # ID страницы, для которой был загружен HTML
        self.cached_available_pages = set()  # Множество страниц, доступных в кэшированном HTML
        self.http_requests_saved = 0  # Счетчик сэкономленных HTTP запросов
    
    def get_sutra_for_volume(self, volume: int) -> str:
        """Получить sutra для конкретного volume, используя mapping или значение по умолчанию"""
        # Если включен auto_sutra, используем last_successful_sutra по умолчанию
        # (если для volume еще не определена sutra)
        if self.auto_sutra:
            return self.volume_sutras.get(volume, self.last_successful_sutra)
        else:
            return self.volume_sutras.get(volume, self.sutra)
    
    def increment_sutra(self, sutra: str) -> str:
        """
        Увеличить числовую часть sutra на 1
        Примеры: d1 -> d2, D1109 -> D1110, d999 -> d1000
        """
        import re
        # Ищем числовую часть в конце строки
        match = re.match(r'^([^\d]*)(\d+)$', sutra)
        if match:
            prefix = match.group(1)
            number = int(match.group(2))
            return f"{prefix}{number + 1}"
        else:
            # Если не найдено число, возвращаем исходное значение
            print(f"  ⚠ Не удалось извлечь число из sutra: {sutra}")
            return sutra
    
    def parse_sutra_number(self, sutra: str) -> Optional[int]:
        """Извлечь числовую часть из sutra"""
        import re
        match = re.search(r'(\d+)$', sutra)
        return int(match.group(1)) if match else None
    
    def extract_available_pages_from_html(self, html_content: str) -> set:
        """
        Извлекает список всех доступных страниц из HTML контента
        Ищет атрибуты data-pbname в HTML
        """
        import re
        available_pages = set()
        
        # Ищем все data-pbname="X-Xa" или data-pbname="X-Xb"
        pattern = r'data-pbname="(\d+-\d+[ab])"'
        matches = re.findall(pattern, html_content)
        available_pages.update(matches)
        
        if not self.quiet_mode and available_pages:
            print(f"  📦 В HTML найдено {len(available_pages)} страниц: {sorted(available_pages)[:10]}{'...' if len(available_pages) > 10 else ''}")
        
        return available_pages
    
    async def cache_current_page(self, page: Page, page_id: str):
        """
        Кэширует текущий HTML контент страницы и определяет доступные страницы
        """
        try:
            html_content = await page.content()
            self.cached_html = html_content
            self.cached_page_id = page_id
            self.cached_available_pages = self.extract_available_pages_from_html(html_content)
            
            if not self.quiet_mode:
                print(f"  💾 HTML кэширован для страницы {page_id}")
        except Exception as e:
            print(f"  ⚠ Ошибка кэширования HTML: {str(e)}")
    
    def is_page_in_cache(self, page_id: str) -> bool:
        """
        Проверяет, доступна ли страница в кэшированном HTML
        """
        return page_id in self.cached_available_pages
    
    async def load_cached_html_to_page(self, page: Page, page_id: str):
        """
        Загружает кэшированный HTML в Playwright страницу
        Это избегает HTTP запроса, используя уже загруженные данные
        """
        try:
            if self.cached_html:
                # Используем setContent для загрузки HTML без HTTP запроса
                await page.set_content(self.cached_html, wait_until='domcontentloaded')
                
                # Даем время на рендеринг
                await page.wait_for_timeout(1000)
                
                if not self.quiet_mode:
                    print(f"  ♻️ Использован кэшированный HTML (HTTP запрос НЕ выполнен)")
                
                self.http_requests_saved += 1
                return True
        except Exception as e:
            print(f"  ⚠ Ошибка загрузки кэша: {str(e)}")
        
        return False
        
    async def wait_for_page_load(self, page: Page, timeout: int = 30000):
        """Ожидание полной загрузки страницы с проверкой контента"""
        try:
            # Ждем загрузки основного контента
            await page.wait_for_load_state('networkidle', timeout=timeout)
            
            # Ждем появления canvas или изображения (признак того, что контент загрузился)
            try:
                await page.wait_for_selector('canvas, img[src*="jpg"], img[src*="png"]', 
                                             timeout=15000, state='visible')
                if not self.quiet_mode:
                    print(f"  ✓ Контент загружен")
            except Exception:
                print(f"  ⚠ Canvas/изображение не появились в течение 15 сек")
            
            # Прокручиваем страницу для загрузки lazy-loaded изображений
            try:
                if not self.quiet_mode:
                    print(f"  → Загрузка lazy-loaded изображений...")
                
                # Триггерим загрузку всех lazy images
                await page.evaluate("""
                    () => {
                        // Прокручиваем постепенно для триггера всех lazy loaders
                        window.scrollTo(0, document.body.scrollHeight / 2);
                    }
                """)
                await page.wait_for_timeout(1500)
                
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await page.wait_for_timeout(3000)  # Увеличено время ожидания
                
                await page.evaluate('window.scrollTo(0, 0)')
                await page.wait_for_timeout(2000)  # Увеличено время ожидания
                
                # Пытаемся форсировать загрузку lazy images
                await page.evaluate("""
                    () => {
                        const lazyImages = document.querySelectorAll('img.lazy');
                        lazyImages.forEach(img => {
                            if (img.dataset.src) {
                                img.src = img.dataset.src;
                            }
                            // Триггерим событие для lazy loader
                            img.dispatchEvent(new Event('load'));
                        });
                    }
                """)
                await page.wait_for_timeout(2000)
                
            except Exception as e:
                print(f"  ⚠ Ошибка прокрутки страницы: {str(e)}")
            
            # Дополнительное время для рендеринга
            await page.wait_for_timeout(2000)
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
                    console.log(`Всего найдено изображений: ${images.length}`);
                    
                    // Создаем разные варианты pageId для поиска
                    // Например: "3-1b" -> ["3-1b", "31b", "3-1-1b"]
                    const searchPatterns = [pageId];
                    
                    // Убираем дефисы
                    searchPatterns.push(pageId.replace(/-/g, ''));
                    
                    // Преобразуем формат {vol}-{page}{side} в {vol}-{page}-{page}{side}
                    const parts = pageId.split('-');
                    if (parts.length === 2) {
                        const vol = parts[0];
                        const pageMatch = parts[1].match(/^(\\d+)([ab])$/);
                        if (pageMatch) {
                            const pageNum = pageMatch[1];
                            const pageSide = pageMatch[2];
                            // "3-1b" -> "3-1-1b"
                            searchPatterns.push(`${vol}-${pageNum}-${pageNum}${pageSide}`);
                            // Также без дефисов
                            searchPatterns.push(`${vol}${pageNum}${pageNum}${pageSide}`);
                        }
                    }
                    
                    console.log(`Паттерны для поиска:`, searchPatterns);
                    
                    // Логируем первые несколько изображений для debug
                    images.forEach((img, idx) => {
                        if (idx < 5) {
                            console.log(`Image ${idx}: src="${img.src || 'none'}", class="${img.className}", width=${img.width}, height=${img.height}, parent="${img.parentElement?.className || 'none'}"`);
                        }
                    });
                    
                    // Ищем изображение по всем паттернам
                    for (const img of images) {
                        const src = img.src || img.dataset.src || '';
                        const alt = img.alt || '';
                        const id = img.id || '';
                        
                        for (const pattern of searchPatterns) {
                            if (src.includes(pattern) || alt === pattern || id === pattern) {
                                console.log(`✓ Найдено изображение по паттерну ${pattern}: ${src}`);
                                return img.src;
                            }
                        }
                    }
                    
                    // Если не нашли конкретное, ищем img с классом image-pb или lazy
                    for (const img of images) {
                        const parent = img.parentElement;
                        if (parent && parent.classList.contains('image-pb')) {
                            console.log('✓ Найдено изображение в image-pb:', img.src);
                            return img.src;
                        }
                    }
                    
                    // Если не нашли, берем первое крупное изображение с классом lazy
                    for (const img of images) {
                        if (img.classList.contains('lazy') && img.width > 300 && img.height > 300) {
                            console.log('✓ Найдено lazy изображение:', img.src);
                            return img.src;
                        }
                    }
                    
                    // Последняя попытка: любое крупное изображение
                    for (const img of images) {
                        if (img.width > 300 && img.height > 300) {
                            console.log('✓ Найдено крупное изображение:', img.src);
                            return img.src;
                        }
                    }
                    
                    console.log('✗ Не найдено ни одного подходящего изображения');
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
                if not self.quiet_mode:
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
            
            if not self.quiet_mode:
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
                    if not self.quiet_mode:
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
            if not self.quiet_mode:
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
    
    async def auto_detect_sutra_for_volume(self, page: Page, session: aiohttp.ClientSession, 
                                           volume: int) -> Optional[str]:
        """
        Автоматический подбор sutra для volume путем попыток загрузки первой страницы
        Возвращает найденную sutra или None если не найдена
        """
        if not self.quiet_mode:
            print(f"\n  🔍 Автоподбор sutra для volume {volume}...")
        
        # Начинаем с последней успешной sutra (оптимизация: sutra обычно растут с volume)
        current_sutra = self.last_successful_sutra
        if not self.quiet_mode:
            print(f"  ℹ Начинаем с последней успешной sutra: {current_sutra}")
        
        page_id = f"{volume}-1b"  # Первая страница тома
        
        for attempt in range(self.max_sutra_attempts):
            try:
                url = f"{self.base_url}index.html?kdb={self.kdb}&sutra={current_sutra}&page={page_id}"
                if not self.quiet_mode:
                    print(f"  → Попытка {attempt + 1}/{self.max_sutra_attempts}: sutra={current_sutra}")
                
                # Пытаемся загрузить страницу
                await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                time.sleep(2)
                
                # Пробуем найти изображение (строгая проверка)
                image_result = await self.find_page_image(page, page_id)
                
                # ВАЖНО: Принимаем только реальные изображения (canvas или img), НЕ screenshot
                if image_result:
                    image_data, source_type = image_result
                    
                    # Проверяем что это реальное изображение, а не скриншот
                    if source_type in ['canvas', 'img']:
                        if not self.quiet_mode:
                            print(f"  ✅ Найдена рабочая sutra: {current_sutra} (источник: {source_type})")
                        # Сохраняем найденную sutra для этого volume
                        self.volume_sutras[volume] = current_sutra
                        # Обновляем последнюю успешную sutra для оптимизации следующих volume
                        self.last_successful_sutra = current_sutra
                        return current_sutra
                    else:
                        print(f"  ✗ Sutra {current_sutra} не подходит (получен только {source_type}, нужен canvas/img)")
                else:
                    print(f"  ✗ Sutra {current_sutra} не подходит (изображение не найдено)")
                    
            except Exception as e:
                print(f"  ✗ Sutra {current_sutra} не подходит (ошибка: {str(e)[:50]}...)")
            
            # Увеличиваем sutra и пробуем снова
            current_sutra = self.increment_sutra(current_sutra)
            time.sleep(1)  # Небольшая пауза между попытками
        
        print(f"  ❌ Не удалось найти рабочую sutra после {self.max_sutra_attempts} попыток")
        return None
    
    async def scrape_page(self, page: Page, session: aiohttp.ClientSession, page_id: str, 
                         max_retries: int = 3) -> Tuple[bool, bool]:
        """
        Парсинг одной страницы с механизмом повторных попыток
        
        Returns:
            Tuple[bool, bool]: (success, used_cache)
                - success: True если страница успешно обработана
                - used_cache: True если использовался кэш (не было HTTP запроса)
        """
        # Извлекаем volume из page_id (формат: "volume-page{a/b}")
        volume = int(page_id.split('-')[0])
        
        # Автоподбор sutra для первой страницы тома (если включен auto_sutra)
        # При auto_sutra всегда запускаем автоподбор для первой страницы нового volume,
        # игнорируя предустановленные volume_sutras
        if self.auto_sutra and page_id == f"{volume}-1b":
            # Удаляем предустановленную sutra если она есть, чтобы использовать last_successful_sutra
            if volume in self.volume_sutras:
                if not self.quiet_mode:
                    print(f"  ℹ Игнорируем предустановленную sutra для volume {volume}, используем автоподбор")
                del self.volume_sutras[volume]
            
            detected_sutra = await self.auto_detect_sutra_for_volume(page, session, volume)
            if detected_sutra is None:
                print(f"\n  ❌ Не удалось автоматически определить sutra для volume {volume}")
                if not self.quiet_mode:
                    print(f"  ℹ Используем последнюю успешную sutra ({self.last_successful_sutra}) для остальных страниц")
                # Сохраняем последнюю успешную sutra для этого volume
                self.volume_sutras[volume] = self.last_successful_sutra
        
        # Переменная для отслеживания использования кэша
        used_cache = False
        
        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    print(f"\n  🔄 Попытка {attempt}/{max_retries}")
                    time.sleep(5)  # Пауза перед повторной попыткой
                
                if not self.quiet_mode:
                    print(f"\n{'='*60}")
                    print(f"→ Обработка страницы: {page_id}")
                    print(f"{'='*60}")
                
                # Получаем правильную sutra для этого volume
                page_sutra = self.get_sutra_for_volume(volume)
                
                # Формируем URL с параметрами каталога и сутры
                url = f"{self.base_url}index.html?kdb={self.kdb}&sutra={page_sutra}&page={page_id}"
                
                # ОПТИМИЗАЦИЯ: Проверяем, есть ли страница в кэше
                page_loaded_from_cache = False
                if self.is_page_in_cache(page_id):
                    if not self.quiet_mode:
                        print(f"  🎯 Страница найдена в кэше! Пропускаем HTTP запрос")
                    # Загружаем кэшированный HTML
                    page_loaded_from_cache = await self.load_cached_html_to_page(page, page_id)
                    if page_loaded_from_cache:
                        used_cache = True  # Отмечаем, что использовали кэш
                
                # Если не загрузили из кэша, делаем обычный HTTP запрос
                if not page_loaded_from_cache:
                    if not self.quiet_mode:
                        print(f"  URL: {url}")
                        print(f"  Volume: {volume}, Sutra: {page_sutra}")
                        if self.auto_sutra and volume in self.volume_sutras:
                            print(f"  ℹ Sutra определена автоматически")
                    
                    # Переходим на страницу с увеличенным timeout
                    await page.goto(url, wait_until='domcontentloaded', timeout=60000)
                    await self.wait_for_page_load(page)
                    
                    # Кэшируем загруженный HTML для будущих запросов
                    await self.cache_current_page(page, page_id)
            
                # Сохраняем HTML для отладки
                await self.save_page_html(page, page_id)
                
                # Извлекаем изображение
                if not self.quiet_mode:
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
                    if not self.quiet_mode:
                        print(f"  ℹ Источник изображения: {source_type}")
                    
                    # Проверяем что это не просто пустой скриншот
                    if source_type == 'screenshot':
                        print(f"  ⚠ Получен скриншот вместо canvas/img - возможно страница не загрузилась")
                        print(f"  ✗ Скриншот НЕ сохраняется (требуется реальное изображение)")
                        
                        # Если включен auto_sutra, пробуем инкрементировать sutra
                        if self.auto_sutra and attempt < max_retries:
                            if not self.quiet_mode:
                                print(f"  🔍 Пробуем инкрементировать sutra...")
                            tried_sutras = []
                            current_sutra = page_sutra
                            
                            for sutra_attempt in range(self.max_sutra_attempts):
                                current_sutra = self.increment_sutra(current_sutra)
                                tried_sutras.append(current_sutra)
                                if not self.quiet_mode:
                                    print(f"  → Попытка с sutra: {current_sutra}")
                                
                                # Пробуем загрузить с новой sutra
                                new_url = f"{self.base_url}index.html?kdb={self.kdb}&sutra={current_sutra}&page={page_id}"
                                await page.goto(new_url, wait_until='domcontentloaded', timeout=60000)
                                await self.wait_for_page_load(page)
                                time.sleep(1)
                                
                                # Проверяем изображение
                                new_image_result = await self.find_page_image(page, page_id)
                                if new_image_result:
                                    new_image_data, new_source_type = new_image_result
                                    if new_source_type in ['canvas', 'img']:
                                        if not self.quiet_mode:
                                            print(f"  ✅ Найдена рабочая sutra: {current_sutra}")
                                        # Обновляем sutra для этого volume
                                        self.volume_sutras[volume] = current_sutra
                                        self.last_successful_sutra = current_sutra
                                        # Обновляем данные для сохранения
                                        image_result = new_image_result
                                        image_data = new_image_data
                                        source_type = new_source_type
                                        image_source = new_source_type
                                        url = new_url
                                        break
                            else:
                                # Не нашли рабочую sutra
                                print(f"  ✗ Не найдена рабочая sutra после попыток: {', '.join(tried_sutras)}")
                                if attempt < max_retries:
                                    continue
                                else:
                                    image_saved = False
                        elif attempt < max_retries:
                            continue
                        else:
                            image_saved = False
                    
                    # Сохраняем изображение если source_type не screenshot
                    if source_type != 'screenshot':
                        if source_type == 'img' and not image_data.startswith('data:'):
                            # Это URL, нужно скачать
                            full_url = urljoin(self.base_url, image_data)
                            image_saved = await self.download_image_url(session, full_url, image_filename)
                        else:
                            # Это data URL или уже готовые данные (canvas или img с data:)
                            image_saved = self.save_image(image_data, image_filename)
                else:
                    print(f"  ✗ Изображение не найдено")
                    
                    # Если включен auto_sutra, пробуем инкрементировать sutra
                    if self.auto_sutra and attempt < max_retries:
                        if not self.quiet_mode:
                            print(f"  🔍 Пробуем инкрементировать sutra...")
                        tried_sutras = []
                        current_sutra = page_sutra
                        
                        for sutra_attempt in range(self.max_sutra_attempts):
                            current_sutra = self.increment_sutra(current_sutra)
                            tried_sutras.append(current_sutra)
                            if not self.quiet_mode:
                                print(f"  → Попытка с sutra: {current_sutra}")
                            
                            # Пробуем загрузить с новой sutra
                            new_url = f"{self.base_url}index.html?kdb={self.kdb}&sutra={current_sutra}&page={page_id}"
                            await page.goto(new_url, wait_until='domcontentloaded', timeout=60000)
                            await self.wait_for_page_load(page)
                            time.sleep(1)
                            
                            # Проверяем изображение
                            new_image_result = await self.find_page_image(page, page_id)
                            if new_image_result:
                                new_image_data, new_source_type = new_image_result
                                if new_source_type in ['canvas', 'img']:
                                    if not self.quiet_mode:
                                        print(f"  ✅ Найдена рабочая sutra: {current_sutra}")
                                    # Обновляем sutra для этого volume
                                    self.volume_sutras[volume] = current_sutra
                                    self.last_successful_sutra = current_sutra
                                    # Обновляем данные для сохранения
                                    image_result = new_image_result
                                    image_data = new_image_data
                                    source_type = new_source_type
                                    image_source = new_source_type
                                    url = new_url
                                    
                                    # Сохраняем изображение
                                    if source_type == 'img' and not image_data.startswith('data:'):
                                        full_url = urljoin(self.base_url, image_data)
                                        image_saved = await self.download_image_url(session, full_url, image_filename)
                                    else:
                                        image_saved = self.save_image(image_data, image_filename)
                                    break
                        else:
                            # Не нашли рабочую sutra
                            print(f"  ✗ Не найдена рабочая sutra после попыток: {', '.join(tried_sutras)}")
                            if attempt < max_retries:
                                continue
                    elif attempt < max_retries:
                        continue
                
                # Извлекаем текст
                if not self.quiet_mode:
                    print(f"\n  → Поиск текста...")
                text = await self.extract_tibetan_text(page, page_id)
                
                text_saved = False
                if text:
                    # Показываем превью текста
                    if not self.quiet_mode:
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
                    'volume': volume,
                    'sutra': self.get_sutra_for_volume(volume),
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
                    if not self.quiet_mode:
                        print(f"\n  ✅ Страница успешно обработана")
                    return (True, used_cache)
                elif image_saved or text_saved:
                    print(f"\n  ⚠ Страница обработана частично")
                    return (False, used_cache)
                else:
                    # Ничего не получили, пробуем еще раз
                    if attempt < max_retries:
                        print(f"\n  ⚠ Ничего не получено, повторная попытка...")
                        continue
                    else:
                        print(f"\n  ✗ Не удалось получить данные после {max_retries} попыток")
                        return (False, used_cache)
                
            except Exception as e:
                print(f"\n  ✗ Ошибка при обработке (попытка {attempt}/{max_retries}): {str(e)}")
                if attempt < max_retries:
                    import traceback
                    traceback.print_exc()
                    continue
                else:
                    import traceback
                    traceback.print_exc()
                    return (False, used_cache)
        
        return (False, used_cache)
    
    def generate_page_ids(self, start_vol: int, end_vol: int, start_page: int, end_page: int) -> List[str]:
        """
        Генерация списка ID страниц
        Формат: {vol}-{page}{a/b}
        Примечание: Страница {vol}-1a никогда не существует, поэтому начинаем с {vol}-1b
        """
        page_ids = []
        for vol in range(start_vol, end_vol + 1):
            for page_num in range(start_page, end_page + 1):
                # Пропускаем страницу 1a для каждого volume - она никогда не существует
                if page_num == 1:
                    page_ids.append(f"{vol}-{page_num}b")  # Только 1b
                else:
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
        print(f"Каталог: {self.kdb}")
        
        # Информация о sutra
        if self.auto_sutra:
            print(f"Режим sutra: АВТОМАТИЧЕСКИЙ")
            print(f"  Начальная sutra: {self.sutra}")
            print(f"  Максимум попыток инкремента: {self.max_sutra_attempts}")
            print(f"  Оптимизация: Продолжение с последней успешной sutra")
            if self.volume_sutras:
                print(f"  Предустановленные sutra:")
                for vol, sutra in sorted(self.volume_sutras.items()):
                    print(f"    Volume {vol}: {sutra}")
        elif self.volume_sutras:
            print(f"Сутры по volume:")
            for vol, sutra in sorted(self.volume_sutras.items()):
                print(f"  Volume {vol}: {sutra}")
            print(f"Сутра по умолчанию: {self.sutra}")
        else:
            print(f"Сутра: {self.sutra}")
        
        print(f"Директория вывода: {self.output_dir.absolute()}")
        print(f"Формат изображений: {self.image_format.upper()}" + 
              (f" (качество: {self.jpeg_quality}%)" if self.image_format == 'jpeg' else ""))
        print(f"Режим браузера: {'headless' if headless else 'visible'}")
        print(f"Задержка между HTTP запросами: {self.delay_between_pages} сек (не применяется к кэшу)")
        print(f"Лимит неудач для пропуска volume: {self.max_failed_pages} страниц")
        if self.quiet_mode:
            print(f"Режим вывода: ТИХИЙ (только ошибки и предупреждения)")
        print(f"{'#'*60}\n")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()
            
            # Включаем вывод console.log из браузера (только если не quiet_mode)
            if not self.quiet_mode:
                page.on("console", lambda msg: print(f"  [Browser] {msg.text}"))
            
            async with aiohttp.ClientSession() as session:
                success_count = 0
                partial_count = 0
                fail_count = 0
                
                skip_until_next_volume = False
                skipped_count = 0
                
                for i, page_id in enumerate(page_ids, 1):
                    # Определяем текущий volume
                    volume = int(page_id.split('-')[0])
                    
                    # Проверяем, нужно ли сбросить счетчик неудач при переходе на новый volume
                    if self.current_volume != volume:
                        self.current_volume = volume
                        self.failed_pages_in_volume = 0
                        skip_until_next_volume = False  # Новый volume - пробуем снова
                    
                    # Если достигнут лимит неудач для этого volume, пропускаем оставшиеся страницы
                    if skip_until_next_volume:
                        print(f"\n[{i}/{len(page_ids)}] ⏭ Пропущена страница {page_id} (volume {volume} пропускается)")
                        skipped_count += 1
                        continue
                    
                    print(f"\n[{i}/{len(page_ids)}]")
                    
                    try:
                        success, used_cache = await self.scrape_page(page, session, page_id)
                        
                        if success:
                            success_count += 1
                            self.failed_pages_in_volume = 0  # Сбрасываем счетчик при успехе
                        else:
                            # Проверяем был ли хоть какой-то успех
                            if self.metadata and self.metadata[-1].get('success'):
                                partial_count += 1
                                self.failed_pages_in_volume = 0  # Частичный успех тоже считается
                            else:
                                fail_count += 1
                                self.failed_pages_in_volume += 1
                                
                                # Проверяем, достигнут ли лимит неудач
                                if self.failed_pages_in_volume >= self.max_failed_pages:
                                    print(f"\n  ⚠ Достигнут лимит неудач ({self.max_failed_pages}) для volume {volume}")
                                    print(f"  ⏭ Пропускаем оставшиеся страницы volume {volume}, переход к следующему volume")
                                    skip_until_next_volume = True
                        
                        # Пауза между запросами ТОЛЬКО если был выполнен реальный HTTP запрос (не из кэша)
                        if not used_cache:
                            if not self.quiet_mode:
                                print(f"  ⏱ Задержка {self.delay_between_pages} сек перед следующим HTTP запросом...")
                            time.sleep(self.delay_between_pages)
                        else:
                            if not self.quiet_mode:
                                print(f"  ⚡ Пропуск задержки (страница из кэша)")
                        
                    except KeyboardInterrupt:
                        print("\n\n⚠ Прервано пользователем")
                        break
                    except Exception as e:
                        print(f"\n  ✗ Необработанная ошибка: {str(e)}")
                        fail_count += 1
                        self.failed_pages_in_volume += 1
                        
                        # Проверяем лимит неудач и при исключениях
                        if self.failed_pages_in_volume >= self.max_failed_pages:
                            print(f"\n  ⚠ Достигнут лимит неудач ({self.max_failed_pages}) для volume {volume}")
                            print(f"  ⏭ Пропускаем оставшиеся страницы volume {volume}, переход к следующему volume")
                            skip_until_next_volume = True
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
        if skipped_count > 0:
            print(f"⏭ Пропущено (лимит неудач): {skipped_count}")
        
        # Статистика оптимизации
        total_processed = success_count + partial_count + fail_count
        if self.http_requests_saved > 0:
            print(f"\n⚡ ОПТИМИЗАЦИЯ:")
            print(f"  HTTP запросов сэкономлено: {self.http_requests_saved}")
            if total_processed > 0:
                efficiency = (self.http_requests_saved / total_processed) * 100
                print(f"  Эффективность кэширования: {efficiency:.1f}%")
        
        print(f"\nДанные сохранены в: {self.output_dir.absolute()}")
        print(f"  - Изображения: {self.images_dir}")
        print(f"  - Тексты: {self.texts_dir}")
        print(f"  - Метаданные: {self.metadata_file}")
        print(f"{'#'*60}\n")


async def main():
    """Точка входа"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Улучшенный парсер тибетских иероглифов с adarshah.org (с кэшированием HTML)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Тест на одной странице (каталог по умолчанию - degekangyur)
  python improved_parser.py --pages 1-1b
  
  # АВТОМАТИЧЕСКИЙ ПОДБОР SUTRA (рекомендуется для массового парсинга!)
  python improved_parser.py --auto-sutra --sutra d1 --start-vol 1 --end-vol 100 --start-page 1 --end-page 100
  
  # Автоподбор для degetengyur
  python improved_parser.py --kdb degetengyur --auto-sutra --sutra D1109 --start-vol 1 --end-vol 50 --start-page 1 --end-page 100
  
  # Автоподбор с JPEG и увеличенным числом попыток
  python improved_parser.py --auto-sutra --sutra d1 --max-sutra-attempts 20 --start-vol 1 --end-vol 100 --image-format jpeg --jpeg-quality 85
  
  # Ручное указание sutra для конкретных volumes
  python improved_parser.py --volume-sutras 1:d1 2:d2 3:d3 --start-vol 1 --end-vol 3 --start-page 1 --end-page 100
  
  # Парсинг из каталога degetengyur с ручным указанием sutra
  python improved_parser.py --kdb degetengyur --sutra D1109 --pages 1-1b
  
  # Парсинг с видимым браузером (для отладки)
  python improved_parser.py --auto-sutra --pages 1-1b --no-headless
  
  # Тихий режим (показывать только ошибки и предупреждения)
  python improved_parser.py --auto-sutra --sutra d1 --start-vol 1 --end-vol 100 --quiet
  
Примечания:
  - Страница 1-1a никогда не существует на сайте и будет автоматически пропущена
  - --auto-sutra автоматически подбирает правильную sutra для каждого volume, инкрементируя число при неудачах
  - При --auto-sutra программа пытается загрузить {volume}-1b с разными sutra (d1, d2, d3...) до успеха
  - Максимальное число попыток инкремента задается через --max-sutra-attempts (по умолчанию 10)
  
Оптимизация:
  - Парсер автоматически кэширует HTML страницы
  - Когда одна HTML страница содержит данные для нескольких страниц (например, 1-1b, 1-25a, 1-25b),
    парсер автоматически использует кэш и НЕ делает повторные HTTP запросы
  - Задержка (--delay) применяется ТОЛЬКО к реальным HTTP запросам, страницы из кэша обрабатываются мгновенно
  - Это значительно ускоряет парсинг и снижает нагрузку на сервер
        """
    )
    
    parser.add_argument('--output', '-o', default='tibetan_data', 
                       help='Директория для сохранения данных')
    parser.add_argument('--kdb', default='degekangyur',
                       help='Каталог (например: degekangyur, degetengyur)')
    parser.add_argument('--sutra', default='d1',
                       help='Сутра по умолчанию (например: d1, D1109)')
    parser.add_argument('--volume-sutras', nargs='+', metavar='VOLUME:SUTRA',
                       help='Сопоставление volume->sutra (например: 1:d1 2:d2 3:d3)')
    parser.add_argument('--auto-sutra', action='store_true',
                       help='Автоматический подбор sutra для каждого volume (инкремент числа при неудачах)')
    parser.add_argument('--max-sutra-attempts', type=int, default=10,
                       help='Максимальное количество попыток инкремента sutra (по умолчанию: 10)')
    parser.add_argument('--max-failed-pages', type=int, default=5,
                       help='Максимальное количество неудачных страниц подряд перед пропуском volume (по умолчанию: 5)')
    parser.add_argument('--image-format', choices=['png', 'jpeg'], default='png',
                       help='Формат изображений: png или jpeg (по умолчанию: png)')
    parser.add_argument('--jpeg-quality', type=int, default=95, 
                       help='Качество JPEG от 1 до 100 (по умолчанию: 95)')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Задержка между HTTP запросами в секундах. Не применяется к страницам из кэша (по умолчанию: 2.0)')
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
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Тихий режим: выводить только ошибки и предупреждения, скрывать успешные операции')
    
    args = parser.parse_args()
    
    # Парсим volume-sutras если предоставлены
    volume_sutras = {}
    if args.volume_sutras:
        for mapping in args.volume_sutras:
            try:
                volume_str, sutra = mapping.split(':')
                volume = int(volume_str)
                volume_sutras[volume] = sutra
            except ValueError:
                print(f"⚠ Неверный формат volume-sutra: {mapping}. Ожидается формат VOLUME:SUTRA (например: 1:d1)")
                continue
    
    scraper = ImprovedTibetanScraper(
        output_dir=args.output, 
        kdb=args.kdb, 
        sutra=args.sutra,
        image_format=args.image_format,
        jpeg_quality=args.jpeg_quality,
        delay_between_pages=args.delay,
        volume_sutras=volume_sutras,
        auto_sutra=args.auto_sutra,
        max_sutra_attempts=args.max_sutra_attempts,
        max_failed_pages=args.max_failed_pages,
        quiet_mode=args.quiet
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

