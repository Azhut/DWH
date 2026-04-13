# Архитектура проекта на виртуальной машине

## 1. Общая информация

**Тип системы:** Linux (вероятно Ubuntu/Debian)
**Контейнеризация:** Docker + Docker Compose
**Веб-сервер:** Nginx (в контейнере)
**Frontend:** React (сборка через Node)
**Backend:** Python (в контейнере `dwh-app`)
**База данных:** MongoDB

---

## 2. Структура директорий

```bash
/home/user/dashboards
├── log.txt
├── dashboard.sh
├── DWH/
│   └── (код backend, docker-compose, Dockerfile и т.д.)
└── frontend/
    ├── docker-compose.yaml
    ├── nginx.conf
    ├── react-app-build.Dockerfile
    └── public/   # сюда складывается билд фронта
```

---

## 3. Скрипт деплоя

**Файл:** `/home/user/dashboards/dashboard.sh`

```bash
#!/bin/sh
docker stop $(docker ps -q)
docker rm $(docker ps -aq)
docker image rm dwh-app

cd /home/user/dashboards

rm -rf frontend/public/*

cd DWH
sudo -u user git pull origin main
docker compose up -d

cd ../frontend
docker compose up -d
```

### Что делает:

1. Останавливает и удаляет все контейнеры
2. Удаляет образ backend (`dwh-app`)
3. Очищает frontend/public
4. Обновляет backend через git
5. Поднимает backend (DWH)
6. Поднимает frontend

---

## 4. Backend (DWH)

**Путь:** `/home/user/dashboards/DWH`

### Что происходит:

* директория полностью обновляется через `git pull`
* собирается Docker-образ `dwh-app`
* запускаются контейнеры:

  * `sport_api` — backend
  * `mongodb` — база данных

### Порты:

* backend: `2700`
* MongoDB: стандартный `27017` (внутри docker)

---

## 5. Frontend

**Путь:** `/home/user/dashboards/frontend`

---

### 5.1 docker-compose.yaml

```yaml
services:
  react-app-build:
    build:
      context: .
      dockerfile: react-app-build.Dockerfile
    environment:
      - REPO_URI=https://github.com/katerinasakhar/tables.git
      - API=
    volumes:
      - ./public:/react_app
    
  react-app-server:
    image: nginx
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./public:/react_app
    ports:
      - 8080:8080
    depends_on:
      react-app-build:
        condition: service_completed_successfully
```

---

### 5.2 react-app-build.Dockerfile

```dockerfile
FROM node:22

WORKDIR /react_app

ENTRYPOINT mkdir tmp; cd tmp; git clone -b main --depth=1 ${REPO_URI}; cd $(ls); rm -rf dist node_modules .parcel-cache; npm install; npm run build; cp dist/* /react_app; cd /react_app; rm -rf tmp
```

### Что делает:

1. Клонирует frontend репозиторий
2. Устанавливает зависимости
3. Собирает проект (`npm run build`)
4. Копирует `dist` → `/react_app` (volume → public)

---

### 5.3 nginx.conf

```nginx
events {}

http {
    include mime.types;
    
    client_max_body_size 0;
    
    server {
        listen 8080;

        location / { 
            root /react_app;

            proxy_request_buffering off; 
            try_files $uri /index.html;
        }

        location /api {
            proxy_pass http://172.17.0.1:2700;
        }
    }   
}
```

---

## 6. Поток запросов

### 6.1 Пользователь → Frontend

```text
Браузер → http://SERVER_IP:8080
↓
Nginx (react-app-server)
↓
Статика из /react_app (public)
```

---

### 6.2 Frontend → Backend

```text
Frontend → /api/...
↓
Nginx (proxy)
↓
http://172.17.0.1:2700
↓
Backend (sport_api)
```

---

## 7. Docker-контейнеры

После запуска:

### Backend (DWH)

* `sport_api`
* `mongodb`

### Frontend

* `frontend-react-app-build-1` (одноразовый)
* `frontend-react-app-server-1` (nginx)

---

## 8. Логика запуска

1. Очищается frontend/public
2. Backend пересобирается и запускается
3. Frontend:

   * билд-контейнер собирает React
   * кладёт файлы в volume
   * завершает работу
4. Nginx:

   * стартует
   * читает файлы из public
   * проксирует API

---

## 9. log.txt (пример поведения)

```text
Container frontend-react-app-build-1 Exited
Container frontend-react-app-server-1 Started
```

### Интерпретация:

* билд контейнер завершился
* nginx запущен
* frontend должен быть в `/public`

---

## 10. Итоговая схема

```text
                ┌────────────────────┐
                │      Browser       │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │      Nginx         │  (port 8080)
                │ react-app-server   │
                └───────┬────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌──────────────┐                ┌──────────────┐
│  React build │                │   Backend    │
│   (static)   │                │ sport_api    │
│ /react_app   │                │ port 2700    │
└──────────────┘                └──────┬───────┘
                                      │
                                      ▼
                                ┌──────────────┐
                                │   MongoDB    │
                                └──────────────┘
```

---

## 11. Ключевые особенности текущей архитектуры

* frontend билдится внутри контейнера при каждом деплое
* nginx раздаёт статику и проксирует API
* backend и frontend живут в разных docker-compose
* связь между ними через IP `172.17.0.1`
* нет общей docker-сети
* деплой полностью пересоздаёт окружение

---

## 12. Потенциальные узкие места

* нестабильный билд frontend
* зависимость от сети (git clone, npm install)
* race condition между build и nginx
* отсутствие healthcheck
* использование host IP внутри docker

---
