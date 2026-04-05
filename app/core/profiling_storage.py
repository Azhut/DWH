"""Хранение и анализ метрик профилирования."""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

class ProfilingStorage:
    """Хранит метрики профилирования в JSON файле."""
    
    def __init__(self, storage_dir: str = "profiling_data"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.current_session_file = self.storage_dir / f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.metrics = []
    
    def add_step_metric(self, step_name: str, duration: float, memory_delta: int, pipeline_name: str):
        """Добавляет метрику шага."""
        metric = {
            "timestamp": datetime.now().isoformat(),
            "pipeline": pipeline_name,
            "step": step_name,
            "duration_seconds": round(duration, 3),
            "memory_delta_mb": memory_delta,
            "type": "step"
        }
        self.metrics.append(metric)
        self._save_to_file()
    
    def add_pipeline_metric(self, pipeline_name: str, total_duration: float, step_count: int, peak_memory: int):
        """Добавляет метрику всего pipeline."""
        metric = {
            "timestamp": datetime.now().isoformat(),
            "pipeline": pipeline_name,
            "total_duration_seconds": round(total_duration, 3),
            "step_count": step_count,
            "peak_memory_mb": peak_memory,
            "type": "pipeline"
        }
        self.metrics.append(metric)
        self._save_to_file()
    
    def _save_to_file(self):
        """Сохраняет метрики в файл."""
        with open(self.current_session_file, 'w', encoding='utf-8') as f:
            json.dump(self.metrics, f, indent=2, ensure_ascii=False)
    
    def get_all_sessions(self) -> List[Path]:
        """Возвращает все файлы сессий."""
        return sorted(self.storage_dir.glob("session_*.json"))
    
    def load_session(self, file_path: Path) -> List[Dict]:
        """Загружает метрики из файла сессии."""
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

# Глобальный экземпляр для хранения метрик
profiling_storage = ProfilingStorage()
