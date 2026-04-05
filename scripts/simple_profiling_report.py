#!/usr/bin/env python3
"""
Отчет профилирования с текстовой визуализацией.
Работает стабильно в Windows консоли без matplotlib.
"""

import json
from pathlib import Path
from typing import Dict, List, Any

class SimpleProfilingAnalyzer:
    """Анализатор метрик профилирования с текстовой визуализацией."""
    
    def __init__(self, data_dir: str = "../profiling_data"):
        self.data_dir = Path(data_dir)
        
    def load_all_sessions(self) -> List[Dict]:
        """Загружает все сессии профилирования."""
        all_metrics = []
        
        if not self.data_dir.exists():
            print(f"Директория {self.data_dir} не найдена")
            return all_metrics
            
        session_files = sorted(self.data_dir.glob("session_*.json"))
        
        if not session_files:
            print(f"Файлы сессий не найдены в {self.data_dir}")
            return all_metrics
            
        print(f"Загрузка {len(session_files)} файлов сессий...")
        
        for session_file in session_files:
            try:
                with open(session_file, 'r', encoding='utf-8') as f:
                    session_metrics = json.load(f)
                    all_metrics.extend(session_metrics)
                    print(f"  Загружено {len(session_metrics)} метрик из {session_file.name}")
            except Exception as e:
                print(f"  Ошибка загрузки {session_file}: {e}")
                
        return all_metrics
    
    def analyze_metrics(self, metrics: List[Dict]) -> Dict[str, Any]:
        """Анализирует метрики и возвращает статистику."""
        if not metrics:
            return {}
            
        # Разделяем метрики по типам
        step_metrics = [m for m in metrics if m.get('type') == 'step']
        pipeline_metrics = [m for m in metrics if m.get('type') == 'pipeline']
        
        analysis = {
            'total_sessions': len(set(m.get('timestamp', '').split('T')[0] for m in metrics)),
            'total_steps': len(step_metrics),
            'total_pipelines': len(pipeline_metrics),
            'steps_analysis': {},
            'pipelines_analysis': {},
            'slowest_steps': [],
            'memory_intensive_steps': []
        }
        
        # Анализ шагов
        if step_metrics:
            step_stats = {}
            for metric in step_metrics:
                step_name = metric.get('step', 'Unknown')
                if step_name not in step_stats:
                    step_stats[step_name] = {
                        'durations': [],
                        'memory_deltas': [],
                        'count': 0
                    }
                
                step_stats[step_name]['durations'].append(metric.get('duration_seconds', 0))
                step_stats[step_name]['memory_deltas'].append(metric.get('memory_delta_mb', 0))
                step_stats[step_name]['count'] += 1
            
            # Считаем статистику для каждого шага
            for step_name, stats in step_stats.items():
                durations = stats['durations']
                memory_deltas = stats['memory_deltas']
                
                analysis['steps_analysis'][step_name] = {
                    'avg_duration': sum(durations) / len(durations),
                    'max_duration': max(durations),
                    'min_duration': min(durations),
                    'avg_memory': sum(memory_deltas) / len(memory_deltas),
                    'max_memory': max(memory_deltas),
                    'count': len(durations),
                    'total_time': sum(durations)
                }
            
            # Находим самые медленные шаги
            analysis['slowest_steps'] = sorted(
                [(name, stats['avg_duration'], stats['count'], stats['total_time']) 
                 for name, stats in analysis['steps_analysis'].items()],
                key=lambda x: x[1], reverse=True
            )
            
            # Находим шаги с большим потреблением памяти
            analysis['memory_intensive_steps'] = sorted(
                [(name, stats['avg_memory'], stats['count']) 
                 for name, stats in analysis['steps_analysis'].items()],
                key=lambda x: x[1], reverse=True
            )
        
        # Анализ pipeline
        if pipeline_metrics:
            pipeline_stats = {}
            for metric in pipeline_metrics:
                pipeline_name = metric.get('pipeline', 'Unknown')
                if pipeline_name not in pipeline_stats:
                    pipeline_stats[pipeline_name] = {
                        'durations': [],
                        'step_counts': [],
                        'peak_memories': []
                    }
                
                pipeline_stats[pipeline_name]['durations'].append(metric.get('total_duration_seconds', 0))
                pipeline_stats[pipeline_name]['step_counts'].append(metric.get('step_count', 0))
                pipeline_stats[pipeline_name]['peak_memories'].append(metric.get('peak_memory_mb', 0))
            
            for pipeline_name, stats in pipeline_stats.items():
                analysis['pipelines_analysis'][pipeline_name] = {
                    'avg_duration': sum(stats['durations']) / len(stats['durations']),
                    'max_duration': max(stats['durations']),
                    'min_duration': min(stats['durations']),
                    'avg_step_count': sum(stats['step_counts']) / len(stats['step_counts']),
                    'avg_peak_memory': sum(stats['peak_memories']) / len(stats['peak_memories']),
                    'count': len(stats['durations'])
                }
        
        return analysis
    
    def print_bar_chart(self, data: List[tuple], title: str, width: int = 50):
        """Печатает текстовую гистограмму."""
        print(f"\n{title}")
        print("=" * len(title))
        
        if not data:
            print("Нет данных для отображения")
            return
        
        # Находим максимальное значение для масштабирования
        max_value = max(item[1] for item in data)
        
        for name, value, *rest in data:
            # Масштабируем бар
            bar_length = int((value / max_value) * width) if max_value > 0 else 0
            bar = "█" * bar_length
            
            # Форматируем метку
            if len(rest) >= 1:
                count = rest[0]
                label = f"{name:20s} | {value:8.3f}s ({count:2d} раз) | {bar}"
            else:
                label = f"{name:20s} | {value:8.3f}s | {bar}"
            
            print(label)
    
    def print_memory_chart(self, data: List[tuple], title: str, width: int = 50):
        """Печатает текстовую гистограмму для памяти."""
        print(f"\n{title}")
        print("=" * len(title))
        
        if not data:
            print("Нет данных для отображения")
            return
        
        max_value = max(item[1] for item in data)
        
        for name, value, count in data:
            bar_length = int((value / max_value) * width) if max_value > 0 else 0
            bar = "▓" * bar_length
            print(f"{name:20s} | {value:8.1f}MB ({count:2d} раз) | {bar}")
    
    def print_timeline_chart(self, metrics: List[Dict], title: str):
        """Печатает временную шкалу выполнения."""
        print(f"\n{title}")
        print("=" * len(title))
        
        # Сортируем метрики по времени
        sorted_metrics = sorted(metrics, key=lambda x: x.get('timestamp', ''))
        
        current_time = None
        for metric in sorted_metrics[:20]:  # Показываем первые 20
            timestamp = metric.get('timestamp', '')
            if timestamp:
                # Извлекаем время
                time_part = timestamp.split('T')[1][:8] if 'T' in timestamp else timestamp[:8]
                
                if time_part != current_time:
                    print(f"\n{time_part}:")
                    current_time = time_part
                
                step_name = metric.get('step', metric.get('pipeline', 'Unknown'))
                duration = metric.get('duration_seconds', metric.get('total_duration_seconds', 0))
                memory = metric.get('memory_delta_mb', metric.get('peak_memory_mb', 0))
                
                print(f"  {step_name:25s} {duration:6.3f}s  {memory:+6.1f}MB")
    
    def print_report(self, analysis: Dict[str, Any]):
        """Выводит детальный отчет с текстовой визуализацией."""
        print("\n" + "="*80)
        print("ОТЧЕТ ПРОФИЛИРОВАНИЯ ПРОИЗВОДИТЕЛЬНОСТИ")
        print("="*80)
        
        print(f"\nОБЩАЯ СТАТИСТИКА:")
        print(f"  Сессий профилирования: {analysis.get('total_sessions', 0)}")
        print(f"  Шагов выполнено: {analysis.get('total_steps', 0)}")
        print(f"  Pipeline выполнено: {analysis.get('total_pipelines', 0)}")
        
        if not analysis.get('steps_analysis'):
            print("\nНет данных для анализа шагов")
            return
        
        # Текстовая визуализация самых медленных шагов
        slowest_data = [(name, avg_duration, count) for name, avg_duration, count, total_time in analysis['slowest_steps'][:10]]
        self.print_bar_chart(slowest_data, "ТОП-10 САМЫХ МЕДЛЕННЫХ ШАГОВ")
        
        # Текстовая визуализация потребления памяти
        memory_data = analysis['memory_intensive_steps'][:10]
        self.print_memory_chart(memory_data, "ТОП-10 ШАГОВ ПО ПОТРЕБЛЕНИЮ ПАМЯТИ")
        
        # Детальная статистика
        print(f"\nДЕТАЛЬНАЯ СТАТИСТИКА ШАГОВ:")
        print("   Шаг                    | Сред. время | Макс. время | Кол-во | Общ. время")
        print("   ------------------------|-------------|-------------|--------|------------")
        
        for step_name, avg_duration, count, total_time in analysis['slowest_steps'][:10]:
            max_duration = analysis['steps_analysis'][step_name]['max_duration']
            print(f"   {step_name:22s} | {avg_duration:11.3f}s | {max_duration:11.3f}s | {count:6d} | {total_time:10.3f}s")
        
        # Статистика pipeline
        if analysis.get('pipelines_analysis'):
            print(f"\nСТАТИСТИКА ПО PIPELINE:")
            for pipeline_name, stats in analysis['pipelines_analysis'].items():
                print(f"  {pipeline_name}:")
                print(f"    Среднее время: {stats['avg_duration']:.3f}s (мин: {stats['min_duration']:.3f}s, макс: {stats['max_duration']:.3f}s)")
                print(f"    Средние шаги: {stats['avg_step_count']:.1f}")
                print(f"    Пиковая память: {stats['avg_peak_memory']:.1f}MB")
                print(f"    Выполнено: {stats['count']} раз")
        
        # Распределение времени
        total_time = sum(stats['total_time'] for stats in analysis['steps_analysis'].values())
        if total_time > 0:
            print(f"\nРАСПРЕДЕЛЕНИЕ ОБЩЕГО ВРЕМЕНИ ПО ШАГАМ:")
            print("   Шаг                    | Общ. время | Доля от общего")
            print("   ------------------------|------------|---------------")
            
            sorted_by_total = sorted(
                [(name, stats['total_time']) for name, stats in analysis['steps_analysis'].items()],
                key=lambda x: x[1], reverse=True
            )
            
            for step_name, step_time in sorted_by_total[:10]:
                percentage = (step_time / total_time * 100)
                bar_length = int(percentage / 2)  # 2 символа на 1%
                bar = "░" * bar_length
                print(f"   {step_name:22s} | {step_time:10.3f}s | {percentage:13.1f}% {bar}")
        
        # Рекомендации
        print(f"\nРЕКОМЕНДАЦИИ ПО ОПТИМИЗАЦИИ:")
        
        slow_steps = [step for step in analysis['slowest_steps'] if step[1] > 2.0]
        if slow_steps:
            print(f"  ВНИМАНИЕ: Следующие шаги работают медленно (>2с):")
            for step_name, avg_duration, count, total_time in slow_steps[:3]:
                print(f"    - {step_name}: {avg_duration:.3f}s (рекомендуется оптимизация)")
        
        memory_heavy = [step for step in analysis['memory_intensive_steps'] if step[1] > 50]
        if memory_heavy:
            print(f"  ВНИМАНИЕ: Следующие шаги используют много памяти (>50MB):")
            for step_name, avg_memory, count in memory_heavy[:3]:
                print(f"    - {step_name}: {avg_memory:.1f}MB (проверьте утечки памяти)")
        
        if not slow_steps and not memory_heavy:
            print(f"  Производительность выглядит хорошей!")
            print(f"  Все шаги работают <2с и используют <50MB памяти")
        
        print(f"\nСЛЕДУЮЩИЕ ШАГИ:")
        print(f"  1. Фокусируйтесь на шагах с временем >2с")
        print(f"  2. Проверьте шаги с потреблением памяти >50MB")
        print(f"  3. Сравните производительность до/после оптимизации")
        print(f"  4. Используйте данные для планирования улучшений")


def main():
    print("АНАЛИЗАТОР ПРОФИЛИРОВАНИЯ С ТЕКСТОВОЙ ВИЗУАЛИЗАЦИЕЙ")
    print("="*60)
    
    analyzer = SimpleProfilingAnalyzer()
    metrics = analyzer.load_all_sessions()
    
    if not metrics:
        print("\nНет данных для анализа")
        print("Убедитесь что:")
        print("  1. ENABLE_PROFILING=true в .env")
        print("  2. Приложение запущено")
        print("  3. Вы загрузили хотя бы один файл")
        return
    
    print(f"\nЗагружено {len(metrics)} метрик")
    
    analysis = analyzer.analyze_metrics(metrics)
    
    # Временная шкала
    analyzer.print_timeline_chart(metrics, "ВРЕМЕННАЯ ШКАЛА ВЫПОЛНЕНИЯ")
    
    # Основной отчет
    analyzer.print_report(analysis)
    
    print(f"\nАнализ завершен!")
    print(f"Данные сохранены в: profiling_data/")


if __name__ == "__main__":
    main()
