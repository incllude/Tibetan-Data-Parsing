#!/usr/bin/env python3
"""
Парсер тибетских текстов с adarshah.org
Текст парсится со страницы, изображения скачиваются напрямую по URL
"""

import asyncio
import json
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import aiohttp
from playwright.async_api import async_playwright, Page


class ImprovedTibetanScraper:
    """Парсер тибетских текстов и изображений"""
    
    def __init__(self, output_dir: str = "tibetan_data", kdb: str = "degekangyur", sutra: str = "d1",
                 delay_between_pages: float = 2.0, volume_sutras: Optional[Dict[int, str]] = None, 
                 auto_sutra: bool = False, max_sutra_attempts: int = 10, max_failed_pages: int = 5, 
                 quiet_mode: bool = False):
        self.output_dir = Path(output_dir)
        self.images_dir = self.output_dir / "images"
        self.texts_dir = self.output_dir / "texts"
        self.metadata_file = self.output_dir / "metadata.json"
        
        # Создаем необходимые директории
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.texts_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_url = "https://online.adarshah.org/"
        self.images_base_url = "https://files.dharma-treasure.org/"
        self.kdb = kdb  # Каталог (degekangyur, degetengyur и т.д.)
        self.sutra = sutra  # Сутра по умолчанию (d1, D1109 и т.д.)
        self.volume_sutras = volume_sutras or {}  # Сопоставление volume -> sutra
        self.auto_sutra = auto_sutra  # Автоматический подбор sutra
        self.max_sutra_attempts = max_sutra_attempts  # Максимальное количество попыток инкремента sutra
        self.delay_between_pages = delay_between_pages  # Задержка между запросами (секунды)
        self.max_failed_pages = max_failed_pages  # Максимальное количество неудачных страниц подряд
        self.quiet_mode = quiet_mode  # Тихий режим
        self.metadata = []
        self.last_successful_sutra = sutra  # Последняя успешная sutra
        
        # Отслеживание неудачных попыток
        self.current_volume = None
        self.failed_pages_in_volume = 0
        
        # Кэш для оптимизации
        self.cached_html = None
        self.cached_page_id = None
        self.cached_available_pages = set()
        self.http_requests_saved = 0
    
    def get_sutra_for_volume(self, volume: int) -> str:
        """Получить sutra для конкретного volume"""
        if self.auto_sutra:
            return self.volume_sutras.get(volume, self.last_successful_sutra)
        else:
            return self.volume_sutras.get(volume, self.sutra)
    
    def increment_sutra(self, sutra: str) -> str:
        """
        Увеличить числовую часть sutra на 1
        Примеры: d1 -> d2, D1109 -> D1110
        """
        import re
        match = re.match(r'^([^\d]*)(\d+)$', sutra)
        if match:
            prefix = match.group(1)
            number = int(match.group(2))
            return f"{prefix}{number + 1}"
        else:
            print(f"  ⚠ Не удалось извлечь число из sutra: {sutra}")
            return sutra
    
    def extract_available_pages_from_html(self, html_content: str) -> set:
        """Извлекает список доступных страниц из HTML"""
        import re
        available_pages = set()
        pattern = r'data-pbname="(\d+-\d+[ab])"'
        matches = re.findall(pattern, html_content)
        available_pages.update(matches)
        
        if not self.quiet_mode and available_pages:
            print(f"  📦 В HTML найдено {len(available_pages)} страниц")
        
        return available_pages
    
    async def cache_current_page(self, page: Page, page_id: str):
        """Кэширует HTML контент страницы"""
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
        """Проверяет доступность страницы в кэше"""
        return page_id in self.cached_available_pages
    
    async def load_cached_html_to_page(self, page: Page, page_id: str):
        """Загружает кэшированный HTML в Playwright страницу"""
        try:
            if self.cached_html:
                await page.set_content(self.cached_html, wait_until='domcontentloaded')
                await page.wait_for_timeout(1000)
                
                if not self.quiet_mode:
                    print(f"  ♻️ Использован кэшированный HTML")
                
                self.http_requests_saved += 1
                return True
        except Exception as e:
            print(f"  ⚠ Ошибка загрузки кэша: {str(e)}")
        
        return False
        
    async def wait_for_page_load(self, page: Page, timeout: int = 30000):
        """Ожидание загрузки страницы"""
        try:
            await page.wait_for_load_state('networkidle', timeout=timeout)
            await page.wait_for_timeout(2000)
        except Exception as e:
            print(f"  ⚠ Таймаут ожидания загрузки: {str(e)}")
    
    async def extract_tibetan_text(self, page: Page, page_id: str) -> Optional[str]:
        """Извлечение тибетского текста для конкретной страницы"""
        try:
            text_data = await page.evaluate("""
                (pageId) => {
                    // Преобразуем page_id в формат для jp маркера
                    const parts = pageId.split('-');
                    let jpId;
                    if (parts.length === 2) {
                        jpId = parts[0] + '-' + parts[1].slice(0, -1) + '-' + parts[1];
                    } else {
                        jpId = pageId;
                    }
                    
                    // Функция для извлечения текста с сохранением разрывов строк
                    function extractTextWithLineBreaks(element) {
                        let text = '';
                        const childNodes = element.childNodes;
                        
                        for (let node of childNodes) {
                            if (node.nodeType === Node.TEXT_NODE) {
                                text += node.textContent;
                            } else if (node.nodeType === Node.ELEMENT_NODE) {
                                if (node.classList && node.classList.contains('ln') && 
                                    node.classList.contains('break')) {
                                    text += '\\n';
                                } else {
                                    text += extractTextWithLineBreaks(node);
                                }
                            }
                        }
                        return text;
                    }
                    
                    // Метод 1: Поиск по маркерам <jp>
                    const jpStart = document.querySelector(`jp[id="${jpId}"]`);
                    let textByJp = '';
                    
                    if (jpStart) {
                        let currentNode = jpStart.nextSibling;
                        while (currentNode) {
                            if (currentNode.nodeName === 'JP') {
                                break;
                            }
                            if (currentNode.nodeType === Node.TEXT_NODE) {
                                textByJp += currentNode.textContent;
                            } else if (currentNode.nodeType === Node.ELEMENT_NODE) {
                                textByJp += extractTextWithLineBreaks(currentNode);
                            }
                            currentNode = currentNode.nextSibling;
                        }
                    }
                    
                    // Метод 2: Поиск по атрибуту data-pbname
                    const textElements = document.querySelectorAll(`span.text-pb[data-pbname="${pageId}"]`);
                    let textByAttr = '';
                    
                    textElements.forEach(el => {
                        textByAttr += extractTextWithLineBreaks(el);
                    });
                    
                    // Очистка текста
                    function cleanText(text) {
                        text = text.replace(/\\d+-\\d+[ab]/g, '');
                        text = text.split('\\n').map(line => line.replace(/\\s+/g, ' ').trim()).join('\\n');
                        text = text.split('\\n').filter(line => line.length > 0).join('\\n');
                        return text.trim();
                    }
                    
                    let finalText = '';
                    let method = '';
                    
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
                return text_data['text']
            
            return None
            
        except Exception as e:
            print(f"  ✗ Ошибка извлечения текста: {str(e)}")
            return None
    
    def get_image_url(self, page_id: str) -> str:
        """Формирует URL для скачивания изображения страницы"""
        # Извлекаем volume из page_id (формат: "12-2b")
        parts = page_id.split('-')
        volume = parts[0]
        page = parts[1]
        
        # Формируем URL: https://files.dharma-treasure.org/{kdb}/{kdb}{volume}-1/{volume}-1-{page}.jpg
        image_url = f"{self.images_base_url}{self.kdb}/{self.kdb}{volume}-1/{volume}-1-{page}.jpg"
        return image_url
    
    async def download_image(self, session: aiohttp.ClientSession, page_id: str) -> bool:
        """Скачивание изображения по прямому URL"""
        try:
            image_url = self.get_image_url(page_id)
            filename = f"{page_id}.jpg"
            
            if not self.quiet_mode:
                print(f"  → Скачивание изображения: {image_url}")
            
            async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    content = await response.read()
                    filepath = self.images_dir / filename
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    if not self.quiet_mode:
                        print(f"  ✓ Изображение сохранено: {filename}")
                    return True
                else:
                    print(f"  ✗ Ошибка загрузки изображения: статус {response.status}")
                    return False
        except Exception as e:
            print(f"  ✗ Ошибка скачивания изображения: {str(e)}")
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
    
    def save_metadata(self):
        """Сохранение метаданных"""
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Метаданные сохранены: {len(self.metadata)} записей")
    
    async def auto_detect_sutra_for_volume(self, page: Page, session: aiohttp.ClientSession, 
                                           volume: int) -> Optional[str]:
        """Автоматический подбор sutra для volume"""
        if not self.quiet_mode:
            print(f"\n  🔍 Автоподбор sutra для volume {volume}...")
        
        current_sutra = self.last_successful_sutra
        if not self.quiet_mode:
            print(f"  ℹ Начинаем с последней успешной sutra: {current_sutra}")
        
        page_id = f"{volume}-1b"  # Первая страница тома
        
        for attempt in range(self.max_sutra_attempts):
            try:
                url = f"{self.base_url}index.html?kdb={self.kdb}&sutra={current_sutra}&page={page_id}"
                if not self.quiet_mode:
                    print(f"  → Попытка {attempt + 1}/{self.max_sutra_attempts}: sutra={current_sutra}")
                
                await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                time.sleep(2)
                
                # Проверяем наличие текста
                text = await self.extract_tibetan_text(page, page_id)
                
                if text and len(text) > 50:  # Проверяем что текст существенный
                    if not self.quiet_mode:
                        print(f"  ✅ Найдена рабочая sutra: {current_sutra}")
                    self.volume_sutras[volume] = current_sutra
                    self.last_successful_sutra = current_sutra
                    return current_sutra
                else:
                    print(f"  ✗ Sutra {current_sutra} не подходит (текст не найден)")
                    
            except Exception as e:
                print(f"  ✗ Sutra {current_sutra} не подходит (ошибка: {str(e)[:50]}...)")
            
            current_sutra = self.increment_sutra(current_sutra)
            time.sleep(1)
        
        print(f"  ❌ Не удалось найти рабочую sutra после {self.max_sutra_attempts} попыток")
        return None
    
    async def scrape_page(self, page: Page, session: aiohttp.ClientSession, page_id: str, 
                         max_retries: int = 3) -> Tuple[bool, bool]:
        """
        Парсинг одной страницы
        
        Returns:
            Tuple[bool, bool]: (success, used_cache)
        """
        volume = int(page_id.split('-')[0])
        
        # Автоподбор sutra для первой страницы тома
        if self.auto_sutra and page_id == f"{volume}-1b":
            if volume in self.volume_sutras:
                if not self.quiet_mode:
                    print(f"  ℹ Игнорируем предустановленную sutra для volume {volume}, используем автоподбор")
                del self.volume_sutras[volume]
            
            detected_sutra = await self.auto_detect_sutra_for_volume(page, session, volume)
            if detected_sutra is None:
                print(f"\n  ❌ Не удалось автоматически определить sutra для volume {volume}")
                if not self.quiet_mode:
                    print(f"  ℹ Используем последнюю успешную sutra ({self.last_successful_sutra})")
                self.volume_sutras[volume] = self.last_successful_sutra
        
        used_cache = False
        
        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    print(f"\n  🔄 Попытка {attempt}/{max_retries}")
                    time.sleep(5)
                
                if not self.quiet_mode:
                    print(f"\n{'='*60}")
                    print(f"→ Обработка страницы: {page_id}")
                    print(f"{'='*60}")
                
                page_sutra = self.get_sutra_for_volume(volume)
                url = f"{self.base_url}index.html?kdb={self.kdb}&sutra={page_sutra}&page={page_id}"
                
                # Проверяем кэш
                page_loaded_from_cache = False
                if self.is_page_in_cache(page_id):
                    if not self.quiet_mode:
                        print(f"  🎯 Страница найдена в кэше!")
                    page_loaded_from_cache = await self.load_cached_html_to_page(page, page_id)
                    if page_loaded_from_cache:
                        used_cache = True
                
                # Если не из кэша - загружаем
                if not page_loaded_from_cache:
                    if not self.quiet_mode:
                        print(f"  URL: {url}")
                        print(f"  Volume: {volume}, Sutra: {page_sutra}")
                    
                    await page.goto(url, wait_until='domcontentloaded', timeout=60000)
                    await self.wait_for_page_load(page)
                    await self.cache_current_page(page, page_id)
                
                # Извлекаем текст
                if not self.quiet_mode:
                    print(f"\n  → Извлечение текста...")
                text = await self.extract_tibetan_text(page, page_id)
                
                text_saved = False
                if text:
                    if not self.quiet_mode:
                        preview = text[:150] + "..." if len(text) > 150 else text
                        print(f"  ℹ Превью: {preview}")
                    text_saved = self.save_text(page_id, text)
                else:
                    print(f"  ✗ Текст не найден")
                    # Пробуем инкрементировать sutra если авто-режим
                    if self.auto_sutra and attempt < max_retries:
                        print(f"  🔍 Пробуем инкрементировать sutra...")
                        current_sutra = page_sutra
                        
                        for sutra_attempt in range(self.max_sutra_attempts):
                            current_sutra = self.increment_sutra(current_sutra)
                            if not self.quiet_mode:
                                print(f"  → Попытка с sutra: {current_sutra}")
                            
                            new_url = f"{self.base_url}index.html?kdb={self.kdb}&sutra={current_sutra}&page={page_id}"
                            await page.goto(new_url, wait_until='domcontentloaded', timeout=60000)
                            await self.wait_for_page_load(page)
                            time.sleep(1)
                            
                            new_text = await self.extract_tibetan_text(page, page_id)
                            if new_text and len(new_text) > 50:
                                if not self.quiet_mode:
                                    print(f"  ✅ Найдена рабочая sutra: {current_sutra}")
                                self.volume_sutras[volume] = current_sutra
                                self.last_successful_sutra = current_sutra
                                text = new_text
                                text_saved = self.save_text(page_id, text)
                                url = new_url
                                break
                        else:
                            if attempt < max_retries:
                                continue
                    elif attempt < max_retries:
                        continue
                
                # Скачиваем изображение если текст найден
                image_saved = False
                image_filename = None
                if text_saved:
                    if not self.quiet_mode:
                        print(f"\n  → Скачивание изображения...")
                    image_saved = await self.download_image(session, page_id)
                    if image_saved:
                        image_filename = f"{page_id}.jpg"
                
                # Сохраняем метаданные
                metadata_entry = {
                    'page_id': page_id,
                    'volume': volume,
                    'sutra': self.get_sutra_for_volume(volume),
                    'image_file': image_filename if image_saved else None,
                    'text_file': f"{page_id}.txt" if text_saved else None,
                    'text_length': len(text) if text else 0,
                    'text_preview': text[:200] if text else None,
                    'url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'success': image_saved and text_saved,
                    'attempts': attempt
                }
                self.metadata.append(metadata_entry)
                
                success = image_saved and text_saved
                
                if success:
                    if not self.quiet_mode:
                        print(f"\n  ✅ Страница успешно обработана")
                    return (True, used_cache)
                elif text_saved or image_saved:
                    print(f"\n  ⚠ Страница обработана частично")
                    return (False, used_cache)
                else:
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
        """Генерация списка ID страниц (формат: {vol}-{page}{a/b})"""
        page_ids = []
        for vol in range(start_vol, end_vol + 1):
            for page_num in range(start_page, end_page + 1):
                if page_num == 1:
                    page_ids.append(f"{vol}-{page_num}b")  # Только 1b
                else:
                    page_ids.append(f"{vol}-{page_num}a")
                    page_ids.append(f"{vol}-{page_num}b")
        return page_ids
    
    async def run(self, page_ids: Optional[List[str]] = None, max_pages: Optional[int] = None, 
                  headless: bool = True):
        """Основной метод запуска парсера"""
        if page_ids is None:
            page_ids = self.generate_page_ids(1, 1, 1, 5)
        
        if max_pages:
            page_ids = page_ids[:max_pages]
        
        print(f"\n{'#'*60}")
        print(f"# ПАРСЕР ТИБЕТСКИХ ТЕКСТОВ")
        print(f"{'#'*60}")
        print(f"Количество страниц: {len(page_ids)}")
        print(f"Каталог: {self.kdb}")
        
        if self.auto_sutra:
            print(f"Режим sutra: АВТОМАТИЧЕСКИЙ")
            print(f"  Начальная sutra: {self.sutra}")
            print(f"  Максимум попыток инкремента: {self.max_sutra_attempts}")
        elif self.volume_sutras:
            print(f"Сутры по volume:")
            for vol, sutra in sorted(self.volume_sutras.items()):
                print(f"  Volume {vol}: {sutra}")
            print(f"Сутра по умолчанию: {self.sutra}")
        else:
            print(f"Сутра: {self.sutra}")
        
        print(f"Директория вывода: {self.output_dir.absolute()}")
        print(f"Режим браузера: {'headless' if headless else 'visible'}")
        print(f"Задержка между HTTP запросами: {self.delay_between_pages} сек")
        print(f"Лимит неудач для пропуска volume: {self.max_failed_pages} страниц")
        if self.quiet_mode:
            print(f"Режим вывода: ТИХИЙ")
        print(f"{'#'*60}\n")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            if not self.quiet_mode:
                page.on("console", lambda msg: print(f"  [Browser] {msg.text}"))
            
            async with aiohttp.ClientSession() as session:
                success_count = 0
                partial_count = 0
                fail_count = 0
                skip_until_next_volume = False
                skipped_count = 0
                
                for i, page_id in enumerate(page_ids, 1):
                    volume = int(page_id.split('-')[0])
                    
                    if self.current_volume != volume:
                        self.current_volume = volume
                        self.failed_pages_in_volume = 0
                        skip_until_next_volume = False
                    
                    if skip_until_next_volume:
                        print(f"\n[{i}/{len(page_ids)}] ⏭ Пропущена страница {page_id}")
                        skipped_count += 1
                        continue
                    
                    print(f"\n[{i}/{len(page_ids)}]")
                    
                    try:
                        success, used_cache = await self.scrape_page(page, session, page_id)
                        
                        if success:
                            success_count += 1
                            self.failed_pages_in_volume = 0
                        else:
                            if self.metadata and self.metadata[-1].get('success'):
                                partial_count += 1
                                self.failed_pages_in_volume = 0
                            else:
                                fail_count += 1
                                self.failed_pages_in_volume += 1
                                
                                if self.failed_pages_in_volume >= self.max_failed_pages:
                                    print(f"\n  ⚠ Достигнут лимит неудач для volume {volume}")
                                    print(f"  ⏭ Пропускаем оставшиеся страницы volume {volume}")
                                    skip_until_next_volume = True
                        
                        if not used_cache:
                            if not self.quiet_mode:
                                print(f"  ⏱ Задержка {self.delay_between_pages} сек...")
                            time.sleep(self.delay_between_pages)
                        else:
                            if not self.quiet_mode:
                                print(f"  ⚡ Пропуск задержки (кэш)")
                        
                    except KeyboardInterrupt:
                        print("\n\n⚠ Прервано пользователем")
                        break
                    except Exception as e:
                        print(f"\n  ✗ Необработанная ошибка: {str(e)}")
                        fail_count += 1
                        self.failed_pages_in_volume += 1
                        
                        if self.failed_pages_in_volume >= self.max_failed_pages:
                            print(f"\n  ⚠ Достигнут лимит неудач для volume {volume}")
                            print(f"  ⏭ Пропускаем оставшиеся страницы volume {volume}")
                            skip_until_next_volume = True
                        continue
                
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
            print(f"⏭ Пропущено: {skipped_count}")
        
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
        description='Парсер тибетских текстов с adarshah.org',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Тест на одной странице
  python improved_parser.py --pages 1-1b
  
  # Автоматический подбор sutra (рекомендуется)
  python improved_parser.py --auto-sutra --sutra d1 --start-vol 1 --end-vol 10 --start-page 1 --end-page 50
  
  # Парсинг degetengyur с автоподбором
  python improved_parser.py --kdb degetengyur --auto-sutra --sutra D1109 --start-vol 1 --end-vol 5
  
  # Ручное указание sutra для конкретных volumes
  python improved_parser.py --volume-sutras 1:d1 2:d2 3:d3 --start-vol 1 --end-vol 3
  
  # Тихий режим
  python improved_parser.py --auto-sutra --sutra d1 --start-vol 1 --end-vol 10 --quiet
        """
    )
    
    parser.add_argument('--output', '-o', default='tibetan_data', 
                       help='Директория для сохранения данных')
    parser.add_argument('--kdb', default='degekangyur',
                       help='Каталог (например: degekangyur, degetengyur)')
    parser.add_argument('--sutra', default='d1',
                       help='Сутра по умолчанию (например: d1, D1109)')
    parser.add_argument('--volume-sutras', nargs='+', metavar='VOLUME:SUTRA',
                       help='Сопоставление volume->sutra (например: 1:d1 2:d2)')
    parser.add_argument('--auto-sutra', action='store_true',
                       help='Автоматический подбор sutra для каждого volume')
    parser.add_argument('--max-sutra-attempts', type=int, default=10,
                       help='Максимальное количество попыток инкремента sutra (по умолчанию: 10)')
    parser.add_argument('--max-failed-pages', type=int, default=5,
                       help='Максимальное количество неудачных страниц подряд (по умолчанию: 5)')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Задержка между HTTP запросами в секундах (по умолчанию: 2.0)')
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
                       help='Тихий режим: выводить только ошибки и предупреждения')
    
    args = parser.parse_args()
    
    # Парсим volume-sutras
    volume_sutras = {}
    if args.volume_sutras:
        for mapping in args.volume_sutras:
            try:
                volume_str, sutra = mapping.split(':')
                volume = int(volume_str)
                volume_sutras[volume] = sutra
            except ValueError:
                print(f"⚠ Неверный формат: {mapping}. Ожидается VOLUME:SUTRA")
                continue
    
    scraper = ImprovedTibetanScraper(
        output_dir=args.output, 
        kdb=args.kdb, 
        sutra=args.sutra,
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
