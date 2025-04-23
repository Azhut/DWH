import re
from pymorphy3 import MorphAnalyzer

morph = MorphAnalyzer()


def fix_header(text: str) -> str:
    parts = text.split('\n')
    if not parts:
        return ""

    result = [parts[0]]
    for p in parts[1:]:
        # Берем последний фрагмент из предыдущего результата
        prev_part = result[-1].split()[-1] if result else ""
        # Берем первый фрагмент текущей части
        curr_part = p.split()[0] if p else ""

        # Проверяем, образуют ли они корректное слово при объединении
        combined = prev_part + curr_part
        is_valid_word = any(
            morph.word_is_known(word)
            for word in [combined, combined.lower(), combined.capitalize()]
        )

        if is_valid_word:
            result[-1] += p
        else:
            result.append(p)

    # Удаляем лишние пробелы и объединяем
    result= re.sub(r'\s+', ' ', ' '.join(result)).strip()
    return result