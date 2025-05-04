import requests

url = "http://localhost:2700/api/v2/upload"
# url = "http://5.165.236.240:2700/api/v2/upload"

def test_file_upload():

    files = [
        # ('files', open('data/АЛАПАЕВСК 2020.xls', 'rb')),
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