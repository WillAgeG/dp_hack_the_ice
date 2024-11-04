# dp_hack_the_ice
DP Project

### Spark (prediction-app)
```
spark://spark-master:7077
```

Spark Master UI: Откройте в браузере http://localhost:8080 для доступа к Spark Master UI.
Spark Worker UI: Откройте в браузере http://localhost:8081 для доступа к Spark Worker UI.

### Запуск сборки контейнера и запуск docker-контейнера
```
docker compose build
docker compose up
```

### Работа в фоновом режиме Dockerfile

```bash
docker compose up --build -d
docker compose down --rmi all
```

Файловая структура проекта
```
dp_hack_the_ice/
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── poetry.lock
├── prediction/
│   └── main.py
└── tests/
    └── spark/
        └── test_main.py
```





### Создание новых зависимостей python
```bash
poetry add <package-name>
```