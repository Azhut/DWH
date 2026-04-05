import json
import logging
import re
from functools import lru_cache
from io import TextIOWrapper
from pathlib import Path

from pymorphy3 import MorphAnalyzer

from config import config

logging.getLogger("pymorphy3").setLevel(logging.WARNING)
logging.getLogger("pymorphy3_dicts_ru").setLevel(logging.WARNING)

# Символы после которых \n — точно пробел
_PUNCT_BEFORE = frozenset(',.;:!?)»–—…"\'')
# Символы после которых \n — точно join
_HYPHEN = frozenset('-­')  # обычный дефис + мягкий перенос


def _build_word_set(morph: MorphAnalyzer) -> frozenset:
    """Извлекает все известные словоформы из внутреннего словаря pymorphy3."""
    try:
        dawg = morph._dictionary.words
        return frozenset(dawg.keys())
    except AttributeError:
        # fallback: пустой сет, алгоритм просто перейдёт к pymorphy3
        return frozenset()


def _heuristic(prev_tail: str, curr_head: str) -> str | None:
    """
    Быстрая эвристика по символам вокруг \n.
    Возвращает 'join' | 'space' | None (неоднозначно).
    """
    if not prev_tail and not curr_head:
        return "space"

    last_char = prev_tail[-1] if prev_tail else ""
    first_char = curr_head[0] if curr_head else ""

    # Пробел до или после \n — точно граница слов
    if last_char == " " or first_char == " ":
        return "space"

    # Пунктуация перед \n — точно граница
    if last_char in _PUNCT_BEFORE:
        return "space"

    # Заглавная буква после \n — точно новое слово
    if first_char.isupper():
        return "space"

    # Цифра с любой стороны — граница (напр. "стр.\n5")
    if last_char.isdigit() or first_char.isdigit():
        return "space"

    # Дефис перед \n — мягкий перенос, склеиваем
    if last_char in _HYPHEN:
        return "join"

    # Обе стороны — строчные буквы → вероятно разрыв внутри слова
    if last_char.isalpha() and last_char.islower() and first_char.isalpha() and first_char.islower():
        return None  # неоднозначно, идём дальше

    return None


class HeaderFixer:
    """
    Удаляет переносы '\n' в заголовках с учётом морфологии
    и ручного маппинга с автоматическим сохранением новых кейсов.

    Порядок приоритетов для каждого разрыва:
      1. manual_map        — явные ручные override-ы
      2. Эвристика         — символьный контекст вокруг \n (O(1))
      3. Словарь-сет       — проверка склеенного слова (O(1))
      4. pymorphy3 + cache — морфологический fallback
    """

    def __init__(self, map_file: str = None):
        self.morph = MorphAnalyzer()
        self.map_file = Path(map_file) if map_file else config.MANUAL_MAP_PATH
        self.manual_map = self._load_manual_map()
        self._word_set = _build_word_set(self.morph)
        self._new_cases: dict[str, str] = {}

    def _load_manual_map(self) -> dict:
        if self.map_file.exists():
            try:
                with self.map_file.open("r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        return {}

    def _save_manual_map(self):
        if not self._new_cases:
            return
        self.manual_map.update(self._new_cases)
        self.map_file.parent.mkdir(parents=True, exist_ok=True)

        with self.map_file.open("w", encoding="utf-8") as f:  # type: TextIOWrapper
            json.dump(self.manual_map, f, ensure_ascii=False, indent=2)
        print(f"[HeaderFixer] Saved manual_map to {self.map_file}")

    @lru_cache(maxsize=4096)
    def _morph_is_known(self, word: str) -> bool:
        """Морфологическая проверка с кешем."""
        return any(
            self.morph.word_is_known(w)
            for w in (word, word.lower(), word.capitalize())
        )

    def _resolve(self, combo: str, prev_tail: str, curr_head: str) -> str:
        """
        Определяет действие для одного разрыва \n.
        Возвращает 'join' | 'space'.
        """
        # 1. manual_map — ручные исключения, высший приоритет
        if combo in self.manual_map:
            return self.manual_map[combo]

        # 2. Эвристика по символам
        heuristic_result = _heuristic(prev_tail, curr_head)
        if heuristic_result is not None:
            return heuristic_result

        # 3. Словарь-сет — O(1) lookup
        if self._word_set:
            for candidate in (combo, combo.lower(), combo.capitalize()):
                if candidate in self._word_set:
                    return "join"
            # Слово не найдено в словаре — скорее граница
            # Но всё же передаём в pymorphy3 для точности

        # 4. pymorphy3 + lru_cache — fallback только для неоднозначных случаев
        action = "join" if self._morph_is_known(combo) else "space"

        # Сохраняем только то, что дошло до pymorphy3 — реальные edge cases
        if combo:
            self._new_cases[combo] = action

        return action

    def fix(self, text: str) -> str:
        parts = str(text).strip().split("\n")
        if not parts:
            return ""

        result = [parts[0].strip()]
        for part in parts[1:]:
            stripped_part = part.strip()
            if not stripped_part:
                continue

            prev_tail = result[-1].split()[-1] if result[-1].split() else ""
            curr_head = stripped_part.split()[0] if stripped_part.split() else ""
            combo = prev_tail + curr_head

            action = self._resolve(combo, prev_tail, curr_head)

            if action == "join":
                result[-1] += stripped_part
            else:
                result.append(stripped_part)

        merged = " ".join(result)
        merged = re.sub(r"\s{2,}", " ", merged).strip()
        return merged

    def finalize(self):
        """Сохраняет накопленные новые случаи в файл маппинга."""
        self._save_manual_map()


_fixer = HeaderFixer()


def fix_header(text: str) -> str:
    """Убирает переносы строк в заголовке с учётом морфологии и ручного маппинга."""
    return _fixer.fix(text)


def finalize_header_fixing():
    """Сохраняет накопленные новые случаи в маппинг."""
    _fixer.finalize()