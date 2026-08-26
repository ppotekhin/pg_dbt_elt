# ELT Pipeline (Airflow + dbt + postgres)

## 📌 Обзор проекта
Дата-пайплайн для сбора, хранения и трансформации аналитических данных на примере [данных онлайн заказов](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

Проект спроектирован по современной архитектуре **ELT (Extract-Load-Transform)**.

[Итоговый DAG](images/dag_succes.png)

[Пример визуализации витрины](images/pbi.png)

---

## 🛠 Технологический стек
*   **Оркестрация:** Apache Airflow 3.3.0 (TaskFlow API, `airflow.sdk`)
*   **Трансформация данных:** dbt Core 1.12 + `dbt-postgres`
*   **СУБД / Аналитическое хранилище:** PostgreSQL
*   **Интеграция dbt и Airflow:** Astronomer Cosmos (`DbtTaskGroup`)
*   **Среда развертывания:** Docker, Docker Compose (PostgreSQL в качестве бэкенда метастора Airflow)

---

## 📂 Структура репозитория

```text
├── airflow
│   ├── dags/
│   │   ├── generic_tasks.py      # Переиспользуемые таски для конвейеров данных
│   │   ├── kaggle_dag.py         # Главный DAG Airflow (TaskFlow API, Airflow 3)
│   │   ├── resources.py          # Датаклассы метаданных датасетов (DatasetInfo)
│   │   └── settings.py           # Централизованные конфигурации и StrEnum подключений
│   ├── dbt_project/              # Проект dbt (Модели трансформации)
│   │   ├── models/
│   │   │   ├── intermediate/     # Промежуточный слой для типовых преобразований (Представления)
│   │   │   ├── staging/          # Staging-слой для переименования столбцов и преобразования типов (Представления)
│   │   │   └── marts/            # Финальные витрины данных (Схема звезда)
│   │   └── dbt_project.yml       # Конфигурация dbt проекта
│   └── Dockerfile                # Сборка Airflow 3 образа с venv для dbt
└── docker-compose.yml            # Инфраструктура (Airflow Web, Scheduler, Init, Postgres Meta)
```

---

## 🛠 Как запустить проект локально

### 1. Подготовка окружения
Клонируйте репозиторий и создайте в `/airflow` файл конфигурации.

### 2. Запуск инфраструктуры
Соберите кастомный образ Airflow 3 и поднимите контейнеры (Postgres в качестве бэкенда инициализируется автоматически):
```bash
docker compose up -d --build
```

### 3. Настройка подключений в Airflow Web UI (localhost:18080)
*   **Admin -> Connections -> `dwh`** (Тип: `Postgres`, Host: `db`, Port: `5432`)

### 4. Запуск
Включите и запустите DAG `kaggle_dag`.
