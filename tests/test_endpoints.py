# test_main.py
import requests

# Путь до API
url = "http://localhost:8000/api/v1/files/upload"


def test_file_upload():
    # Открываем два файла для отправки
    files = [
        ('files', open('data/АЛАПАЕВСК 2020.xlsx', 'rb')),  # Путь к первому файлу
        ('files', open('data/ИРБИТ 2023.xls', 'rb')),   # Путь ко второму файлу
    ]

    # Отправляем запрос на сервер
    response = requests.post(url, files=files)


    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Ошибка: {response.status_code} - {response.text}")
    # Проверяем ответ от сервера
    assert response.status_code == 200
    assert 'message' in response.json()

    # Проверяем ответ от сервера


    # Закрываем файлы после отправки
    for file in files:
        file[1].close()
