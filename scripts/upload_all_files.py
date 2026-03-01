import os
import time

import requests
from tqdm import tqdm


def send_all_files_to_server(to_prod:bool):
    """
    Отправляет все файлы из указанной директории (включая вложенные папки)
    с отображением прогресса выполнения
    """
    if to_prod:
        url = "http://5.165.236.240:2700/api/v2/upload"
    else:
        url = "http://localhost:2700/api/v2/upload"

    file_objects = []
    start_time = time.time()  # Начало замера времени
    directory = r"C:\Users\Egor\Desktop\Projects\Min_sport\SPORT files"
    recursive=True

    if not os.path.exists(directory):
        print(f"Ошибка: Директория '{directory}' не существует")
        return
    if not os.path.isdir(directory):
        print(f"Ошибка: '{directory}' не является директорией")
        return

    file_paths = []
    try:
        if recursive:
            for root, _, files in os.walk(directory):
                for file in files:
                    full_path = os.path.join(root, file)
                    if os.path.isfile(full_path):
                        file_paths.append(full_path)
        else:
            for item in os.listdir(directory):
                full_path = os.path.join(directory, item)
                if os.path.isfile(full_path):
                    file_paths.append(full_path)

        if not file_paths:
            print("Нет файлов для отправки")
            return

        file_objects = []
        errors = []

        with tqdm(total=len(file_paths), desc="Подготовка файлов", unit="file") as pbar:
            for path in file_paths:
                try:
                    file = open(path, 'rb')
                    file_objects.append(('files', file))
                    pbar.set_postfix(file=os.path.basename(path)[:20])
                    pbar.update(1)
                except Exception as e:
                    errors.append(f"{path} - {str(e)}")
                    pbar.update(1)
                    continue


        if errors:
            print("\nОшибки при открытии файлов:")
            for error in errors:
                print(f"  • {error}")

        if not file_objects:
            print("Нет доступных файлов для отправки")
            return


        with tqdm(total=len(file_objects), desc="Отправка файлов", unit="file") as pbar:
            try:
                response = requests.post(url, files=file_objects)
                pbar.update(len(file_objects))
            except Exception as e:
                print(f"\nОшибка при отправке: {str(e)}")
                return


        if response.status_code == 200:
            print("\nУспешная отправка. Ответ сервера:")
            print(response.json())
        else:
            print(f"\nОшибка {response.status_code}: {response.text}")


        assert response.status_code == 200
        assert 'message' in response.json()

    finally:
        elapsed_time = time.time() - start_time
        print(f"\nОбщее время выполнения: {elapsed_time:.2f} секунд")
        for _, file in file_objects:
            try:
                file.close()
            except Exception as e:
                print(f"Ошибка при закрытии файла: {str(e)}")

    assert response.status_code == 200
    assert 'message' in response.json()

if __name__ == "__main__":
    to_production=True
    send_all_files_to_server(to_production)