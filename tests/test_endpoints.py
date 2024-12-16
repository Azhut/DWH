# test_main.py
import requests


url = "http://192.168.0.200:8080/api/v1/files/upload"


def test_file_upload():

    files = [
        ('files', open('data/АЛАПАЕВСК 2020.xlsx', 'rb')),
        ('files', open('data/ИРБИТ 2023.xls', 'rb')),
    ]


    response = requests.post(url, files=files)


    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Ошибка: {response.status_code} - {response.text}")
    assert response.status_code == 200
    assert 'message' in response.json()


    for file in files:
        file[1].close()