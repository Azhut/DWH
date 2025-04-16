import os
import requests
from tqdm import tqdm


def test_send_files_to_server():
    """
    Отправляет все файлы из указанной директории (включая вложенные папки)
    с отображением прогресса выполнения
    """
    directory = r"C:\Users\Egor\Desktop\to_server"
    url = "http://5.165.236.240:2700/api/v2/upload"
    recursive=False

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

        for _, file in file_objects:
            try:
                file.close()
            except Exception as e:
                print(f"Ошибка при закрытии файла: {str(e)}")

    assert response.status_code == 200
    assert 'message' in response.json()

