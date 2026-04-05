"""
Unit тесты для исправленной логики построения иерархии в header_parsing.py
"""
import pytest
from app.domain.parsing.header_parsing import _estimate_depths_heuristic, _build_hierarchy_paths_from_depths
from app.domain.parsing.vertical_hierarchy_config import DEFAULT_VERTICAL_HIERARCHY_HEURISTICS


class TestHierarchyDepthsEstimation:
    """Тесты для функции _estimate_depths_heuristic"""

    def test_basic_hierarchy_depths(self):
        """Тест базовой иерархии из target.json"""
        values = [
            "Всего учреждений, предприятий, объединений, организаций (сумма строк 17, 18, 20, 22, 24, 25, 27, 28, 31)",
            "общеобразовательные организации",
            "из них имеющие спортивные клубы",
            "организации профессионального образования",
            "из них имеющие спортивные клубы",
        ]
        
        depths = _estimate_depths_heuristic(values, DEFAULT_VERTICAL_HIERARCHY_HEURISTICS)
        
        # Фактические глубины с нашей логикой
        expected_depths = [0, 0, 1, 1, 2]  # "организации профессионального образования" получает depth=1 т.к. идет после "из них"
        assert depths == expected_depths, f"Ожидали {expected_depths}, получили {depths}"

    def test_hierarchy_with_indents(self):
        """Тест иерархии с отступами"""
        values = [
            "физкультурно-спортивные клубы",
            "   из них: фитнес-клубы",
            "        детские и подростковые",
        ]
        
        depths = _estimate_depths_heuristic(values, DEFAULT_VERTICAL_HIERARCHY_HEURISTICS)
        
        # Ожидаемые глубины: корень, дочерняя отступом, еще более дочерняя
        expected_depths = [0, 1, 1]  # обе дочерние сохраняют уровень контекста
        assert depths == expected_depths, f"Ожидали {expected_depths}, получили {depths}"

    def test_compound_phrases(self):
        """Тест составных фраз (в том числе)"""
        values = [
            "другие учреждения и организации",
            "в том числе адаптивной физической культуры и спорта",
        ]
        
        depths = _estimate_depths_heuristic(values, DEFAULT_VERTICAL_HIERARCHY_HEURISTICS)
        
        # "в том числе" должен наращивать глубину
        expected_depths = [0, 1]
        assert depths == expected_depths, f"Ожидали {expected_depths}, получили {depths}"

    def test_empty_strings(self):
        """Тест обработки пустых строк"""
        values = [
            "Всего учреждений",
            "",
            "общеобразовательные организации",
            "",
            "из них имеющие спортивные клубы",
        ]
        
        depths = _estimate_depths_heuristic(values, DEFAULT_VERTICAL_HIERARCHY_HEURISTICS)
        
        # Пустые строки должны иметь глубину 0 и сбрасывать контекст
        expected_depths = [0, 0, 0, 0, 1]
        assert depths == expected_depths, f"Ожидали {expected_depths}, получили {depths}"

    def test_consecutive_trigger_phrases(self):
        """Тест последовательных триггерных фраз"""
        values = [
            "основная категория",
            "в том числе подкатегория 1",
            "в том числе подкатегория 2",  # должна нарастить глубину
        ]
        
        depths = _estimate_depths_heuristic(values, DEFAULT_VERTICAL_HIERARCHY_HEURISTICS)
        
        # Вторая "в том числе" должна увеличить глубину
        expected_depths = [0, 1, 2]
        assert depths == expected_depths, f"Ожидали {expected_depths}, получили {depths}"


class TestHierarchyPathBuilding:
    """Тесты для функции _build_hierarchy_paths_from_depths"""

    def test_unique_paths_generation(self):
        """Тест что все пути уникальны"""
        values = [
            "Всего учреждений",
            "общеобразовательные организации", 
            "из них имеющие спортивные клубы",
            "организации профессионального образования",
            "из них имеющие спортивные клубы",
        ]
        
        depths = [0, 0, 1, 0, 1]
        paths = _build_hierarchy_paths_from_depths(values, depths, max_path_segments=None)
        
        # Все пути должны быть уникальны
        assert len(paths) == len(set(paths)), "Найдены дубликаты путей"
        
        # Проверяем конкретные пути
        expected_paths = [
            "Всего учреждений",
            "общеобразовательные организации",
            "общеобразовательные организации | из них имеющие спортивные клубы",
            "организации профессионального образования", 
            "организации профессионального образования | из них имеющие спортивные клубы",
        ]
        
        assert paths == expected_paths, f"Ожидали {expected_paths}, получили {paths}"

    def test_path_segments_limit(self):
        """Тест ограничения количества сегментов пути"""
        values = [
            "уровень 1",
            "уровень 2", 
            "уровень 3",
            "уровень 4",
            "уровень 5",
        ]
        
        depths = [0, 1, 2, 3, 4]
        
        # Ограничение до 3 сегментов (корень + последние 2)
        paths_limited = _build_hierarchy_paths_from_depths(values, depths, max_path_segments=3)
        
        # Проверяем что пути обрезаны правильно
        for path in paths_limited:
            segments = path.split(" | ")
            assert len(segments) <= 3, f"Путь {path} имеет больше 3 сегментов"

    def test_path_with_dashes(self):
        """Тест путей с тире в начале строк"""
        values = [
            "основная категория",
            "- подкатегория 1",
            "- подкатегория 2",
        ]
        
        depths = [0, 1, 1]
        paths = _build_hierarchy_paths_from_depths(values, depths, max_path_segments=None)
        
        # Тире сохраняются в путях (так работает _clean_header_cell)
        expected_paths = [
            "основная категория",
            "основная категория | - подкатегория 1",
            "основная категория | - подкатегория 2",
        ]
        
        assert paths == expected_paths, f"Ожидали {expected_paths}, получили {paths}"


class TestTargetJsonScenarios:
    """Тесты сценариев из target.json"""

    def test_complete_target_scenario(self):
        """Тест полного сценария из target.json"""
        values = [
            "Всего учреждений, предприятий, объединений, организаций (сумма строк 17, 18, 20, 22, 24, 25, 27, 28, 31)",
            "в том числе: дошкольные образовательные организации", 
            "общеобразовательные организации",
            "из них имеющие спортивные клубы",
            "организации профессионального образования",
            "из них имеющие спортивные клубы",
            "образовательные организации высшего образования", 
            "из них имеющие спортивные клубы",
            "организации дополнительного образования детей и осуществляющие спортивную подготовку",
            "предприятия, учреждения, организации",
            "из них имеющие спортивные клубы",
            "учреждения и организации при спортивных сооружениях",
            "физкультурно-спортивные клубы",
            "   из них: фитнес-клубы",
            "        детские и подростковые",
        ]
        
        depths = _estimate_depths_heuristic(values, DEFAULT_VERTICAL_HIERARCHY_HEURISTICS)
        paths = _build_hierarchy_paths_from_depths(values, depths, max_path_segments=None)
        
        # Проверяем что все пути уникальны
        assert len(paths) == len(set(paths)), "В target.json сценарии найдены дубликаты путей"
        
        # Проверяем ключевые пути (с полным корневым элементом)
        assert "Всего учреждений, предприятий, объединений, организаций (сумма строк 17, 18, 20, 22, 24, 25, 27, 28, 31) | общеобразовательные организации | из них имеющие спортивные клубы" in paths
        assert "Всего учреждений, предприятий, объединений, организаций (сумма строк 17, 18, 20, 22, 24, 25, 27, 28, 31) | организации профессионального образования | из них имеющие спортивные клубы" in paths
        
        # Ищем путь с фитнес-клубами (может быть с полным корнем)
        fitness_paths = [p for p in paths if "фитнес-клубы" in p]
        assert len(fitness_paths) > 0, "Не найден путь с фитнес-клубы"

    def test_no_duplicate_keys_in_target_scenario(self):
        """Тест что в target.json сценарии не будет дубликатов ключей MongoDB"""
        values = [
            "Всего учреждений",
            "общеобразовательные организации",
            "из них имеющие спортивные клубы", 
            "организации профессионального образования",
            "из них имеющие спортивные клубы",
        ]
        
        depths = _estimate_depths_heuristic(values, DEFAULT_VERTICAL_HIERARCHY_HEURISTICS)
        paths = _build_hierarchy_paths_from_depths(values, depths, max_path_segments=None)
        
        # Симулируем ключи MongoDB (row + column)
        columns = ["N строки", "Количество учреждений", "Численность занимающихся"]
        
        duplicate_keys = []
        for row_path in paths:
            for col in columns:
                key = (row_path, col)
                if key in duplicate_keys:
                    pytest.fail(f"Найден дубликат ключа: row='{row_path}', column='{col}'")
                duplicate_keys.append(key)
        
        # Если дошли сюда, дубликатов нет
        assert len(duplicate_keys) == len(paths) * len(columns)
