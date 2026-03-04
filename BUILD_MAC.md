# Сборка ConvMac под macOS

**Важно:** Собрать .app для macOS можно только на Mac (PyInstaller не умеет кросс-компиляцию). Если у вас только Windows — используйте **вариант с GitHub Actions** ниже.

## Вариант A: Сборка на Mac (локально)

### 1. Требования на Mac

- macOS 10.14+ (Mojave или новее)
- Python 3.8+
- Установленный **ffmpeg** (рекомендуется: `brew install ffmpeg`) — приложение может искать его в PATH или можно вшить в .app при сборке

### 2. Подготовка окружения

```bash
cd /путь/к/Scripts   # папка, где лежит convmac.py

# Рекомендуется виртуальное окружение
python3 -m venv venv_mac
source venv_mac/bin/activate

# Зависимости для запуска и сборки
pip install --upgrade pip
pip install PyQt5 pyinstaller

# Опционально (Kafka, мониторинг процессов)
pip install psutil kafka-python
```

### 3. Сборка исполняемого .app

```bash
# В той же папке, где convmac.py и convmac.spec
pyinstaller convmac.spec
```

После успешной сборки:

- **Результат:** `dist/ConvMac.app`
- Запуск: двойной клик по `ConvMac.app` или из терминала: `open dist/ConvMac.app`

### 4. Опционально: вшить ffmpeg в приложение

Если на Mac установлен Homebrew и ffmpeg (`brew install ffmpeg`), spec-файл при сборке автоматически подхватит `ffmpeg` и `ffprobe` из `/opt/homebrew/bin` или `/usr/local/bin` и положит их внутрь .app. Тогда приложение сможет работать без отдельно установленного ffmpeg в системе.

Если ffmpeg не найден при сборке — при запуске ConvMac будет вызывать `ffmpeg` из PATH (пользователь должен установить его сам).

### 5. Проверка перед распространением

Рекомендуется проверить запуск из терминала, чтобы увидеть возможные ошибки:

```bash
open -a dist/ConvMac.app
# или
dist/ConvMac.app/Contents/MacOS/ConvMac
```

При проблемах с подписью/карантином на macOS:

```bash
xattr -cr dist/ConvMac.app
```

## Вариант B: Сборка через GitHub Actions (с Windows без Mac)

Сборка идёт на сервере GitHub (macOS). Вы только пушите код и скачиваете готовый `.app`.

### Шаг 1: Репозиторий на GitHub

- Создайте репозиторий (или используйте существующий).
- В корне репо (или в папке `Scripts`) должны быть:
  - `convmac.py`
  - `convmac.spec`
  - `requirements_convmac.txt`
  - `.github/workflows/build-convmac-mac.yml`

Если всё лежит в подпапке `Scripts`, при ручном запуске workflow можно указать **Project subdir**: `Scripts`.

### Шаг 2: Запуск сборки

**Вариант А — автоматически при push**

Сделайте push в ветку `main` или `master`. Workflow запустится сам, если менялись указанные выше файлы.

**Вариант Б — вручную**

1. На GitHub откройте репозиторий → вкладка **Actions**.
2. Слева выберите **Build ConvMac (macOS)**.
3. Справа нажмите **Run workflow** → **Run workflow**.
4. Если проект в подпапке (например `Scripts`), в поле **Project subdir** введите `Scripts`.
5. Дождитесь зелёной галочки у запуска (обычно 3–5 минут).

### Шаг 3: Скачать ConvMac.app

1. Откройте завершённый run (зелёная галочка).
2. Внизу страницы в блоке **Artifacts** нажмите **ConvMac-macos** — скачается файл без расширения.
3. Переименуйте его, добавив в конец **`.zip`** (например: `ConvMac-macos` → `ConvMac-macos.zip`).
4. Распакуйте архив: внутри будет папка **ConvMac.app**.
5. Перенесите **ConvMac.app** на Mac (флешка, облако, AirDrop) и запускайте двойным кликом.

---

## Одним файлом (без .app)

Если нужен один исполняемый файл без структуры .app (только на Mac):

```bash
pyinstaller --onefile --windowed --name ConvMac convmac.py
```

Исполняемый файл будет в `dist/ConvMac`. Запуск: `./dist/ConvMac`. Учтите, что первый запуск может быть дольше из‑за распаковки во временную папку.
