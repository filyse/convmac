# -*- mode: python ; coding: utf-8 -*-
# ConvMac — сборка под macOS (.app с библиотеками).
# Сборка для macOS возможна только на Mac (PyInstaller не кросс-компилирует).
# На Windows этот spec выведет подсказку; на Mac: pyinstaller convmac.spec

import os
import sys

block_cipher = None

# Сборка для macOS только на macOS
if sys.platform != 'darwin':
    print('')
    print('  ConvMac для macOS нельзя собрать на Windows.')
    print('  Варианты:')
    print('    1) Собрать на Mac: pyinstaller convmac.spec')
    print('    2) Использовать GitHub Actions (см. .github/workflows/build-convmac-mac.yml)')
    print('')
    raise SystemExit(1)

# Опционально: добавить ffmpeg/ffprobe в бандл (если есть в типичных путях Homebrew)
binaries = []
for name in ('ffmpeg', 'ffprobe'):
    for prefix in ('/opt/homebrew/bin', '/usr/local/bin'):
        path = os.path.join(prefix, name)
        if os.path.isfile(path):
            binaries.append((path, '.'))
            print(f'Adding {name}: {path}')
            break
    else:
        print(f'Note: {name} не найден в PATH/Homebrew — приложение будет искать его в системе при запуске.')

# Скрытые импорты для PyQt5 и опциональных модулей
hiddenimports = [
    'PyQt5.QtCore',
    'PyQt5.QtGui',
    'PyQt5.QtWidgets',
    'PyQt5.sip',
]

# Опциональные зависимости (если установлены — попадут в бандл)
for mod in ('psutil', 'kafka'):
    try:
        __import__(mod)
        hiddenimports.append(mod)
        print(f'Adding optional: {mod}')
    except ImportError:
        pass

a = Analysis(
    ['convmac.py'],
    pathex=[],
    binaries=binaries,
    datas=[],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ConvMac',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=True,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='ConvMac',
)

# macOS .app bundle
app = BUNDLE(
    coll,
    name='ConvMac.app',
    icon=None,
    bundle_identifier='com.convmac.app',
    info_plist={
        'CFBundleName': 'ConvMac',
        'CFBundleDisplayName': 'ConvMac',
        'CFBundleExecutable': 'ConvMac',
        'CFBundleIdentifier': 'com.convmac.app',
        'CFBundleVersion': '1.0.0',
        'CFBundleShortVersionString': '1.0.0',
        'NSHighResolutionCapable': True,
        'NSRequiresAquaSystemAppearance': False,
    },
)
