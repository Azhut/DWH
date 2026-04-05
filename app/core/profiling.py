"""Модуль профилирования производительности pipeline шагов."""
import logging
import time
from functools import wraps
from typing import Any, Callable

import psutil

logger = logging.getLogger(__name__)
from app.core.profiling_storage import profiling_storage


def get_memory_usage() -> int:
    """Возвращает использование памяти в MB."""
    try:
        process = psutil.Process()
        return process.memory_info().rss // 1024 // 1024  # Convert to MB
    except Exception:
        return 0


def get_step_name(func, step_name, args):
    """Определяет имя шага из функции и аргументов."""
    if step_name:
        return step_name
    
    # Проверяем __qualname__ для получения имени класса
    if hasattr(func, '__qualname__'):
        qualname = func.__qualname__
        if '.' in qualname:
            class_name = qualname.split('.')[0]
            if class_name != 'execute':
                return class_name
    
    # Если имя все еще execute, пытаемся получить класс из args
    if args:
        for arg in args:
            if hasattr(arg, '__class__'):
                class_name = arg.__class__.__name__
                if 'Step' in class_name:
                    return class_name
    
    return func.__name__


def get_pipeline_name(args):
    """Определяет имя pipeline из аргументов."""
    if not args:
        return "Unknown"
    
    arg = args[0]
    if not hasattr(arg, '__class__'):
        return "Unknown"
    
    class_name = arg.__class__.__name__
    class_lower = class_name.lower()
    module_name = arg.__class__.__module__ if hasattr(arg.__class__, '__module__') else ''
    
    if 'upload' in class_lower or 'upload' in module_name.lower():
        return "Upload"
    elif 'parsing' in class_lower or 'parsing' in module_name.lower():
        return "Parsing"
    
    return "Unknown"


def profile_step(step_name: str | None = None):
    """
    Декоратор для профилирования времени выполнения и использования памяти.
    
    Args:
        step_name: Имя шага для логирования. Если None, используется имя функции.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            # Проверяем несколько источников для ENABLE_PROFILING
            enable_profiling = False
            
            # 1. Переменная окружения
            import os
            if os.environ.get('ENABLE_PROFILING', '').lower() in ('true', '1', 'yes'):
                enable_profiling = True
            
            # 2. Конфиг (fallback)
            if not enable_profiling:
                try:
                    from config.config import config
                    if getattr(config, 'ENABLE_PROFILING', False):
                        enable_profiling = True
                except Exception:
                    pass
            
            # Пропускаем профилирование если выключено
            if not enable_profiling:
                return await func(*args, **kwargs)
            
            # Определяем имя шага и pipeline
            name = get_step_name(func, step_name, args)
            pipeline_name = get_pipeline_name(args)
            
            start_memory = get_memory_usage()
            start_time = time.perf_counter()
            
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                end_memory = get_memory_usage()
                
                duration = end_time - start_time
                memory_delta = end_memory - start_memory
                
                logger.info(
                    "[PROFILING] %s: %.3fs, memory %s%dMB (pipeline: %s)",
                    name,
                    duration,
                    "+" if memory_delta > 0 else "",
                    memory_delta,
                    pipeline_name
                )
                
                # Сохраняем метрику в файл с правильным именем pipeline
                try:
                    profiling_storage.add_step_metric(name, duration, memory_delta, pipeline_name)
                    print(f"[DEBUG] Saved step metric: {name}, {duration:.3f}s, {memory_delta}MB, pipeline: {pipeline_name}")
                except Exception as e:
                    # Если сохранение не удалось, просто логируем ошибку
                    logger.error(f"Failed to save profiling metric: {e}")
                    logger.error(f"Metric data: name={name}, duration={duration}, memory={memory_delta}, pipeline={pipeline_name}")
                    print(f"[ERROR] Failed to save: {e}")
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            # Проверяем несколько источников для ENABLE_PROFILING
            enable_profiling = False
            
            # 1. Переменная окружения
            import os
            if os.environ.get('ENABLE_PROFILING', '').lower() in ('true', '1', 'yes'):
                enable_profiling = True
            
            # 2. Конфиг (fallback)
            if not enable_profiling:
                try:
                    from config.config import config
                    if getattr(config, 'ENABLE_PROFILING', False):
                        enable_profiling = True
                except Exception:
                    pass
            
            # Пропускаем профилирование если выключено
            if not enable_profiling:
                return func(*args, **kwargs)
            
            # Определяем имя шага и pipeline
            name = get_step_name(func, step_name, args)
            pipeline_name = get_pipeline_name(args)
            
            start_memory = get_memory_usage()
            start_time = time.perf_counter()
            
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                end_memory = get_memory_usage()
                
                duration = end_time - start_time
                memory_delta = end_memory - start_memory
                
                logger.info(
                    "[PROFILING] %s: %.3fs, memory %s%dMB (pipeline: %s)",
                    name,
                    duration,
                    "+" if memory_delta > 0 else "",
                    memory_delta,
                    pipeline_name
                )
                
                # Сохраняем метрику в файл с правильным именем pipeline
                try:
                    profiling_storage.add_step_metric(name, duration, memory_delta, pipeline_name)
                    print(f"[DEBUG] Saved step metric: {name}, {duration:.3f}s, {memory_delta}MB, pipeline: {pipeline_name}")
                except Exception as e:
                    # Если сохранение не удалось, просто логируем ошибку
                    logger.error(f"Failed to save profiling metric: {e}")
                    logger.error(f"Metric data: name={name}, duration={duration}, memory={memory_delta}, pipeline={pipeline_name}")
                    print(f"[ERROR] Failed to save: {e}")
        
        # Возвращаем appropriate wrapper в зависимости от типа функции
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


class PipelineProfiler:
    """Профилировщик всего pipeline для сбора общей статистики."""
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.start_time: float | None = None
        self.start_memory: int | None = None
        self.step_count = 0
    
    def start(self) -> None:
        """Начинает профилирование pipeline."""
        # Проверяем несколько источников для ENABLE_PROFILING
        enable_profiling = False
        
        # 1. Переменная окружения
        import os
        if os.environ.get('ENABLE_PROFILING', '').lower() in ('true', '1', 'yes'):
            enable_profiling = True
        
        # 2. Конфиг (fallback)
        if not enable_profiling:
            try:
                from config.config import config
                if getattr(config, 'ENABLE_PROFILING', False):
                    enable_profiling = True
            except Exception:
                pass
        
        if not enable_profiling:
            return
            
        self.start_time = time.perf_counter()
        self.start_memory = get_memory_usage()
        self.step_count = 0
        
        logger.info("[PROFILING] %s pipeline started", self.pipeline_name)
    
    def increment_step(self) -> None:
        """Инкрементирует счетчик шагов."""
        self.step_count += 1
    
    def finish(self) -> None:
        """Завершает профилирование и логирует результаты."""
        if self.start_time is None:
            return
        
        # Проверяем несколько источников для ENABLE_PROFILING
        enable_profiling = False
        
        # 1. Переменная окружения
        import os
        if os.environ.get('ENABLE_PROFILING', '').lower() in ('true', '1', 'yes'):
            enable_profiling = True
        
        # 2. Конфиг (fallback)
        if not enable_profiling:
            try:
                from config.config import config
                if getattr(config, 'ENABLE_PROFILING', False):
                    enable_profiling = True
            except Exception:
                pass
        
        if not enable_profiling:
            return
            
        end_time = time.perf_counter()
        end_memory = get_memory_usage()
        
        total_duration = end_time - self.start_time
        memory_delta = end_memory - self.start_memory if self.start_memory else 0
        
        logger.info(
            "[PROFILING] %s pipeline completed: %.3fs total, %d steps, peak memory %s%dMB",
            self.pipeline_name,
            total_duration,
            self.step_count,
            "+" if memory_delta > 0 else "",
            memory_delta
        )
        
        # Сохраняем метрику pipeline в файл
        try:
            profiling_storage.add_pipeline_metric(self.pipeline_name, total_duration, self.step_count, memory_delta)
        except Exception as e:
            logger.error(f"Failed to save pipeline metric: {e}")
