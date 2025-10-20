#!/usr/bin/env python3
"""
Скрипт для исследования структуры сайта adarshah.org
Помогает понять как именно загружаются изображения и тексты
"""

import asyncio
from playwright.async_api import async_playwright


async def inspect_page():
    """Исследование структуры одной страницы"""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # headless=False чтобы видеть браузер
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        
        # Переходим на страницу
        url = "https://online.adarshah.org/index.html?kdb=degekangyur&sutra=d1&page=1-1b"
        print(f"Загрузка: {url}")
        
        await page.goto(url, wait_until='networkidle')
        await page.wait_for_timeout(5000)  # Ждем полной загрузки
        
        # Анализируем структуру страницы
        print("\n=== Анализ структуры страницы ===\n")
        
        # 1. Поиск изображений
        print("1. ИЗОБРАЖЕНИЯ:")
        images_info = await page.evaluate("""
            () => {
                const images = [];
                document.querySelectorAll('img').forEach(img => {
                    images.push({
                        src: img.src,
                        alt: img.alt,
                        className: img.className,
                        id: img.id,
                        width: img.width,
                        height: img.height
                    });
                });
                return images;
            }
        """)
        for i, img in enumerate(images_info, 1):
            print(f"  Изображение {i}:")
            print(f"    src: {img['src'][:100]}...")
            print(f"    class: {img['className']}")
            print(f"    id: {img['id']}")
            print(f"    size: {img['width']}x{img['height']}")
            print()
        
        # 2. Поиск canvas элементов
        print("\n2. CANVAS элементы:")
        canvas_info = await page.evaluate("""
            () => {
                const canvases = [];
                document.querySelectorAll('canvas').forEach(canvas => {
                    canvases.push({
                        id: canvas.id,
                        className: canvas.className,
                        width: canvas.width,
                        height: canvas.height
                    });
                });
                return canvases;
            }
        """)
        for i, canvas in enumerate(canvas_info, 1):
            print(f"  Canvas {i}:")
            print(f"    id: {canvas['id']}")
            print(f"    class: {canvas['className']}")
            print(f"    size: {canvas['width']}x{canvas['height']}")
            print()
        
        # 3. Поиск тибетского текста
        print("\n3. ТИБЕТСКИЙ ТЕКСТ:")
        text_info = await page.evaluate("""
            () => {
                const tibetanRegex = /[\u0F00-\u0FFF]/;
                const textElements = [];
                
                // Ищем все элементы содержащие тибетский текст
                const allElements = document.querySelectorAll('*');
                allElements.forEach(el => {
                    // Проверяем только текстовые узлы
                    if (el.childNodes.length > 0) {
                        for (const child of el.childNodes) {
                            if (child.nodeType === Node.TEXT_NODE) {
                                const text = child.textContent.trim();
                                if (text && tibetanRegex.test(text) && text.length > 20) {
                                    textElements.push({
                                        tagName: el.tagName,
                                        className: el.className,
                                        id: el.id,
                                        text: text.substring(0, 200),
                                        textLength: text.length
                                    });
                                }
                            }
                        }
                    }
                });
                
                return textElements;
            }
        """)
        
        for i, text_el in enumerate(text_info[:5], 1):  # Показываем первые 5
            print(f"  Текстовый элемент {i}:")
            print(f"    tag: {text_el['tagName']}")
            print(f"    class: {text_el['className']}")
            print(f"    id: {text_el['id']}")
            print(f"    длина: {text_el['textLength']} символов")
            print(f"    текст: {text_el['text']}...")
            print()
        
        # 4. Структура документа
        print("\n4. СТРУКТУРА СТРАНИЦЫ:")
        structure = await page.evaluate("""
            () => {
                const mainDivs = [];
                document.querySelectorAll('body > *').forEach(el => {
                    mainDivs.push({
                        tagName: el.tagName,
                        id: el.id,
                        className: el.className,
                        childrenCount: el.children.length
                    });
                });
                return mainDivs;
            }
        """)
        
        for div in structure:
            print(f"  <{div['tagName']}> id=\"{div['id']}\" class=\"{div['className']}\" children={div['childrenCount']}")
        
        # 5. Специфичные для страницы атрибуты
        print("\n5. DATA-АТРИБУТЫ и ID связанные со страницей:")
        page_specific = await page.evaluate("""
            () => {
                const elements = [];
                document.querySelectorAll('[data-page], [id*="page"], [class*="page"]').forEach(el => {
                    const attrs = {};
                    for (const attr of el.attributes) {
                        if (attr.name.includes('page') || attr.name.includes('data')) {
                            attrs[attr.name] = attr.value;
                        }
                    }
                    elements.push({
                        tagName: el.tagName,
                        attrs: attrs,
                        className: el.className,
                        id: el.id
                    });
                });
                return elements;
            }
        """)
        
        for el in page_specific[:10]:  # Первые 10
            print(f"  <{el['tagName']}> id=\"{el['id']}\" class=\"{el['className']}\"")
            for attr_name, attr_value in el['attrs'].items():
                print(f"    {attr_name}=\"{attr_value}\"")
            print()
        
        # Сохраняем скриншот для визуального анализа
        await page.screenshot(path='page_screenshot.png', full_page=True)
        print("\n✓ Скриншот сохранен: page_screenshot.png")
        
        # Сохраняем HTML
        html_content = await page.content()
        with open('page_source.html', 'w', encoding='utf-8') as f:
            f.write(html_content)
        print("✓ HTML сохранен: page_source.html")
        
        print("\n=== Исследование завершено ===")
        print("Проверьте файлы page_screenshot.png и page_source.html для дополнительного анализа")
        
        # Держим браузер открытым для ручного исследования
        print("\nБраузер остается открытым для ручного исследования.")
        print("Нажмите Enter для завершения...")
        input()
        
        await browser.close()


if __name__ == "__main__":
    asyncio.run(inspect_page())


