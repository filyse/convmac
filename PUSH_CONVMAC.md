# Один раз: запушить ConvMac и собрать .app через GitHub Actions

Репозиторий уже инициализирован. Остаётся только привязать GitHub и запушить.

## 1. Создать репозиторий на GitHub

- Зайдите на [github.com](https://github.com) → **New repository**.
- Имя любое (например `convmac` или `Scripts`).
- **Не** добавляйте README / .gitignore — репо должно быть пустым.
- Создайте репозиторий.

## 2. Привязать репо и запушить (в PowerShell из `c:\Scripts`)

Подставьте вместо `ВАШ_ЛОГИН` и `ИМЯ_РЕПО` свои значения (например `filys` и `convmac`):

```powershell
cd c:\Scripts

git remote add origin https://github.com/ВАШ_ЛОГИН/ИМЯ_РЕПО.git
git add convmac.py convmac.spec requirements_convmac.txt .github/
git add .gitignore PUSH_CONVMAC.md BUILD_MAC.md
git commit -m "ConvMac: app + macOS build workflow"
git branch -M main
git push -u origin main
```

При `git push` GitHub попросит логин/пароль или токен — введите свои данные.

## 3. Запустить сборку и скачать ConvMac.app

1. На GitHub откройте репозиторий → вкладка **Actions**.
2. Слева **Build ConvMac (macOS)** → справа **Run workflow** → **Run workflow**.
3. Через несколько минут откройте завершённый run (зелёная галочка).
4. Внизу в **Artifacts** скачайте **ConvMac-macos**.
5. Скачанный артефакт будет без расширения — переименуйте в `ConvMac-macos.zip`, распакуйте. Внутри будет **ConvMac.app** — перенесите на Mac и запускайте.

После первого push при следующих изменениях `convmac.py` / `convmac.spec` сборка может запускаться автоматически при push в `main`.
