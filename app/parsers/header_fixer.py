import re
import json
from pathlib import Path
import logging
from io import TextIOWrapper

from pymorphy3 import MorphAnalyzer

from config import config

# Импортируем из новой системы конфигурации


logging.getLogger("pymorphy3").setLevel(logging.WARNING)
logging.getLogger("pymorphy3_dicts_ru").setLevel(logging.WARNING)

class HeaderFixer:
    """
    Класс для удаления переносов '\n' в заголовках с учетом морфологии
    и ручного маппинга с автоматическим сохранением новых кейсов.
    """
    # Используем launcher из новой системы
    DEFAULT_MAP_FILE = config.MANUAL_MAP_PATH

    def __init__(self, map_file: str = None):
        self.morph = MorphAnalyzer()
        self.map_file = Path(map_file) if map_file else config.MANUAL_MAP_PATH
        self.manual_map = self._load_manual_map()
        self._new_cases: dict[str, str] = {}

    def _load_manual_map(self) -> dict:
        if self.map_file.exists():
            try:
                with self.map_file.open('r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        return {}

    def _save_manual_map(self):
        if not self._new_cases:
            return
        self.manual_map.update(self._new_cases)
        self.map_file.parent.mkdir(parents=True, exist_ok=True)

        with self.map_file.open('w', encoding='utf-8') as f:  # type: TextIOWrapper
            json.dump(self.manual_map, f, ensure_ascii=False, indent=2)
        print(f"[HeaderFixer] Saved manual_map to {self.map_file}")

    def fix(self, text: str) -> str:
        if text in self.manual_map and self.manual_map[text] not in ('join', 'space'):
            return self.manual_map[text]

        parts = str(text).split('\n')
        if not parts:
            return ''
        result = [parts[0]]
        for part in parts[1:]:
            prev = result[-1].split()[-1] if result else ''
            curr = part.split()[0] if part else ''
            combo = prev + curr

            if combo in self.manual_map:
                if self.manual_map[combo] == 'join':
                    result[-1] += part
                else:
                    result.append(part)
            else:
                is_known = any(
                    self.morph.word_is_known(w)
                    for w in (combo, combo.lower(), combo.capitalize())
                )
                action = 'join' if is_known else 'space'
                if is_known:
                    result[-1] += part
                else:
                    result.append(part)
                if combo:
                    self._new_cases[combo] = action

        merged = ' '.join(result)
        merged = re.sub(r'(\S)\s+(\S)', r'\1 \2', merged)
        return merged

    def finalize(self):
        """
        Сохраняет накопленные новые случаи в файл маппинга
        """
        self._save_manual_map()

_fixer = HeaderFixer()

def fix_header(text: str) -> str:
    """
    Убирает переносы строк в заголовке
    с учётом морфологии и ручного маппинга
    """
    result = _fixer.fix(text)
    return result

def finalize_header_fixing():
    """
    Сохраняет накопленные новые случаи в маппинг
    """
    _fixer.finalize()