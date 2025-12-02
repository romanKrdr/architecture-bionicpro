# BionicPRO — проектная работа 9 спринта (безопасность + сервис отчётов)

Репозиторий содержит решение проектной работы 9 спринта по кейсу **BionicPRO**:

1. **Задание 1. Повышение безопасности системы и SSO**
2. **Задание 2. Разработка сервиса отчётов**

Все необходимые компоненты запускаются через **один `docker-compose.yaml`**.

---

## Структура репозитория (важные файлы)

### Диаграммы

- `BionicPRO_auth_security.drawio`  
  Архитектура аутентификации и управления учётными данными (Задание 1, Задача 1).

- `BionicPRO_reports_architecture.drawio`  
  Архитектура сервиса отчётности и ETL-процесса (Задание 2, Задача 1).

### PKCE и фронтенд

- `frontend/src/App.tsx`  
  Настройка Keycloak-клиента с **PKCE (S256)** для SPA.

- `frontend/src/components/ReportPage.tsx`  
  Страница с кнопкой получения отчёта (`/reports`), доступна только авторизованному пользователю.

### Keycloak

- `keycloak/realm-export.json`  
  Экспорт настроенного realm с:
    - клиентом фронтенда, включённым **PKCE (S256)**;
    - настройками SSO под архитектуру из диаграммы.

### Сервис отчётов (backend)

- `backend/app/main.py`  
  FastAPI-приложение для сервиса отчётов. Основной эндпоинт:
    - `GET /reports` — выдаёт отчёт по текущему пользователю, данные берутся из ClickHouse (OLAP витрина).

- `backend/requirements.txt`  
  Зависимости (FastAPI, ClickHouse driver, JWT).

- `backend/Dockerfile`  
  Docker-образ сервиса отчётов.

### Airflow и OLAP (ClickHouse)

- `airflow/dags/bionicpro_reports_etl.py`  
  DAG `bionicpro_reports_etl`:
    - извлечение данных из CRM (PostgreSQL);
    - извлечение телеметрии (PostgreSQL);
    - загрузка в стейдж-таблицы ClickHouse;
    - построение витрины `mart_user_telemetry` по пользователям.

- `airflow/sql/clickhouse_staging_and_mart.sql`  
  DDL ClickHouse:
    - `stg_crm_users` — стейдж с данными CRM;
    - `stg_telemetry` — стейдж с телеметрией;
    - `mart_user_telemetry` — витрина для API отчётов.

### Docker-compose

- `docker-compose.yaml`  
  Поднимает все нужные компоненты:

    - `keycloak_db` — Postgres для Keycloak
    - `keycloak` — IdP (SSO)
    - `frontend` — React SPA
    - `clickhouse` — OLAP БД
    - `reports-backend` — сервис отчётов (FastAPI)
    - `airflow_db` — Postgres для Airflow
    - `airflow-webserver` — UI и REST API Airflow
    - `airflow-scheduler` — планировщик DAG’ов

---

## Задание 1. Повышение безопасности системы

### Задача 1. Архитектура управления учётными данными

Реализовано в диаграмме `BionicPRO_auth_security.drawio`.

Ключевые моменты архитектуры:

- **Унификация доступа:**  
  Все клиентские приложения (мобильное приложение, веб-приложение, внутренняя CRM) используют единый IdP (Keycloak) в качестве центрального управления учётными данными.

- **Внешние IdP по странам:**  
  Для разных стран и регуляторных требований BionicPRO может интегрировать внешние удостоверяющие службы (например, национальные IdP).  
  Keycloak выступает как **broker**: внешний IdP выдаёт токен Keycloak, а бизнес-системы общаются только с Keycloak.

- **Соблюдение локального хранения персональных/медицинских данных:**
    - Внешний IdP хранит только необходимый минимум (идентификатор, базовые атрибуты).
    - Персональные и медицинские данные пациентов остаются в локальных БД (CRM/EMR) страны, как того требуют законы.

- **Безопасная работа с access/refresh токенами:**
    - Фронтенд получает только **access token**, предназначенный для доступа к API.
    - **Refresh-токены IdP не передаются на фронтенд.**  
      Обновление сессии выполняется через redirect-flow/hidden iframe (как это делает Keycloak), либо через backend (паттерн BFF).
    - Сервис отчётов и другие backend-сервисы доверяют только токенам Keycloak и берут `user_id` из поля `sub`.

### Задача 2. Замена Code Grant на PKCE

**Что сделано:**

1. В `frontend/src/App.tsx`:
    - Включён PKCE:

      ```ts
      pkceMethod: 'S256'
      ```

    - Фронтенд работает как SPA с Authorization Code + PKCE, что защищает от атак на перехват кода.

2. В `keycloak/realm-export.json`:
    - Клиент фронтенда настроен с включённым PKCE (`S256`).
    - Разрешены нужные redirect-uri и web-origin.

В итоге:

- SSO реализован через Keycloak.
- Используется **PKCE Code Grant** для SPA, что соответствует рекомендациям OAuth 2.1.

---

## Задание 2. Разработка сервиса отчётов

### Задача 1. Архитектура решения

Отражено в `BionicPRO_reports_architecture.drawio`.

Архитектура:

- **Источники данных:**
    - CRM (PostgreSQL) — сведения о клиентах и их протезах.
    - Телеметрия протезов (PostgreSQL/другая БД) — события, время реакции, ошибки.

- **ETL (Apache Airflow):**
    - DAG `bionicpro_reports_etl`:
        - `extract_crm` — загрузка данных клиентов и протезов в `stg_crm_users`.
        - `extract_telemetry` — загрузка телеметрии в `stg_telemetry`.
        - `build_mart` — агрегация в `mart_user_telemetry` по `user_id + day`.
        - `cleanup_staging` — очистка стейдж-таблиц (опционально).

- **OLAP (ClickHouse):**
    - Стейдж-таблицы:
        - `stg_crm_users`
        - `stg_telemetry`
    - Витрина:
        - `mart_user_telemetry(user_id, day, events_count, avg_response_ms, p95_response_ms, errors_count)`

- **Сервис отчётов (backend):**
    - FastAPI-приложение (`backend/app/main.py`), которое читает только из витрины.

- **Приложение (frontend):**
    - React SPA, авторизующаяся через Keycloak (PKCE) и запрашивающая отчёт у `/reports`.

### Задача 2. Airflow DAG и витрина

**DAG `bionicpro_reports_etl`**:

- Период запуска — раз в час (`0 * * * *`).
- Использует два PostgreSQL-подключения:
    - `crm_postgres` — CRM данные.
    - `telemetry_postgres` — телеметрия.
- Использует ClickHouse client для записи в OLAP.

**Витрина `mart_user_telemetry`:**

- Совмещает данные:
    - из CRM (связь user ↔ prosthesis_id),
    - и аггрегированную телеметрию:
        - количество событий;
        - среднее время реакции;
        - P95 по времени реакции;
        - количество ошибок.

Структура витрины подобрана так, чтобы:

- быстро получать отчёт **по конкретному пользователю** за диапазон дней;
- опираться только на уже подготовленные агрегаты (без тяжёлых запросов в реальном времени).

### Задача 3. Backend API /reports

Реализовано в `backend/app/main.py` (FastAPI):

- `GET /health` → `{ "status": "ok" }`
- `GET /reports?from_date=YYYY-MM-DD&to_date=YYYY-MM-DD`

**Авторизация:**

- Используется `Authorization: Bearer <access_token>`.
- Токен — JWT от Keycloak.

**Извлечение пользователя:**

- Функция `get_current_user()`:
    - достаёт токен через `HTTPBearer`;
    - читает `sub` из JWT (по упрощённой схеме без проверки подписи);
    - использует `sub` как `user_id` для запросов в OLAP.

**Логика `/reports`:**

- Не принимает `userId` в параметрах — отчёт всегда только по `sub` из токена.
- Делает `SELECT` из `mart_user_telemetry`:

    - `WHERE user_id = :sub`
    - `AND day BETWEEN :from_date AND :to_date`
    - результат сортируется по `day`.

- Дополнительно:
    - Находит `max(day)` в витрине и, если запрошенный `to_date` больше — **обрезает его**.  
      Это выполняет требование: *«генерация отчётов только за период, который уже обработан Airflow»*.

### Задача 4. Ограничение доступа к отчётам

Требования:

> «Доступ к отчёту по пользователю должен предоставляться только в отношении себя.»

Реализация:

- `user_id` никогда не приходит с фронтенда.
- Вся логика опирается на `sub` из access token.
- Запрос в ClickHouse всегда фильтрует по `user_id = sub`.

Таким образом, даже при попытке подделать параметры, пользователь не может получить чужой отчёт.

### Задача 5. UI: кнопка получения отчёта

Реализовано в `frontend/src/components/ReportPage.tsx`.

Функциональность:

- Используется `useKeycloak()` для получения access token.
- Кнопка **«Получить отчёт»**:
    - доступна только если Keycloak инициализирован;
    - при нажатии:
        - проверяет, что пользователь авторизован (`keycloak.token` не `null`);
        - формирует период: последние 7 дней;
        - вызывает `GET ${REACT_APP_API_URL}/reports?from_date=...&to_date=...`  
          с заголовком `Authorization: Bearer <token>`.

- Обработка ошибок:
    - если пользователь не авторизован — показывается сообщение «Пользователь не авторизован»;
    - если backend вернул `401` — сообщение о истекшей/недействительной сессии;
    - если другие ошибки — соответствующее уведомление.

- При успешном ответе:
    - данные отчёта отображаются в таблице:
        - день;
        - количество событий;
        - среднее время реакции;
        - P95;
        - количество ошибок.

---

## Запуск проекта

### 1. Запуск всех сервисов одной командой

Из корня репозитория:

```bash
cd architecture-bionicpro-main
COMPOSE_HTTP_TIMEOUT=600 docker compose up --build
```

Команда поднимет:

* `keycloak_db` — Postgres для Keycloak
* `keycloak` — IdP (SSO), импортирует `realm-export.json`
* `frontend` — React SPA
* `clickhouse` — OLAP БД
* `reports-backend` — сервис отчётов (FastAPI)
* `airflow_db` — Postgres для Airflow
* `airflow-init` — одноразовая инициализация БД Airflow и пользователя `admin/admin`
* `airflow-webserver` — UI Airflow
* `airflow-scheduler` — планировщик DAG’а `bionicpro_reports_etl`

### 2. Проверка работы сервисов

После успешного запуска:

1. **Сервис отчётов (health-check)**

   ```text
   http://localhost:8000/health
   ```

   Ответ:

   ```json
   { "status": "ok" }
   ```

2. **Keycloak (админка)**

   ```text
   http://localhost:8080/admin/master/console/
   ```

   Логин: `admin`
   Пароль: `admin`

3. **Airflow UI**

   ```text
   http://localhost:8081
   ```

   Логин: `admin`
   Пароль: `admin`
   В списке DAG’ов должен быть `bionicpro_reports_etl` без статуса *Broken*.

4. **Frontend (приложение отчётов)**

   ```text
   http://localhost:3000
   ```
   Поведение:
    * При первом заходе приложение запросит авторизацию в Keycloak.
    * Для теста можно использовать пользователя из `realm-export.json`, например:
        * Логин: `prothetic1`
        * Пароль: `prothetic123`
    * После логина:
        * отобразится кнопка **«Получить отчёт»**;
        * при нажатии фронтенд вызовет `/reports` на backend с токеном пользователя;
        * backend вернёт отчёт только по текущему пользователю (либо пустой список, если данных в витрине нет).

---