import os
import requests
from tqdm import tqdm


def send_files_to_server():
    directory = r"C:\Users\Egor\Desktop\Projects\Min_sport\SPORT files"
    url = "http://5.165.236.240:2700/api/v2/upload"
    recursive = True

    if not os.path.exists(directory):
        print(f"Ошибка: Директория '{directory}' не существует")
        return

    file_paths = []
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

    errors = []
    successful_uploads = 0
    total_files = len(file_paths)

    # Объединенный прогресс-бар для всех операций
    with tqdm(total=total_files, desc="Обработка файлов", unit="file") as pbar:
        for path in file_paths:
            try:
                # Открытие файла
                with open(path, 'rb') as file:
                    # Формируем данные для одного файла
                    files = [('files', file)]  # [[1]][[6]]

                    # Отправка запроса
                    response = requests.post(url, files=files)

                    # Проверка ответа
                    if response.status_code == 200:
                        successful_uploads += 1
                    else:
                        errors.append(f"{path} - {response.status_code}: {response.text}")

                # Обновление прогресса
                pbar.set_postfix(file=os.path.basename(path)[:20])
                pbar.update(1)

            except Exception as e:
                errors.append(f"{path} - {str(e)}")
                pbar.update(1)
                continue

    # Итоговый отчет
    print(f"\nУспешно отправлено: {successful_uploads}/{total_files}")
    if errors:
        print("\nОшибки:")
        for error in errors:
            print(f"  • {error}")
    else:
        print("\nВсе файлы отправлены успешно")

send_files_to_server()