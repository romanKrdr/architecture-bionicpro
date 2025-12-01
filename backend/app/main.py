from datetime import date
from typing import Any, Dict, List, Optional
import os

from fastapi import Depends, FastAPI, HTTPException, Query, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from clickhouse_driver import Client, errors as ch_errors
from jose import jwt, JWTError

app = FastAPI(title="BionicPRO Reports API")

origins = [
    os.getenv("FRONTEND_ORIGIN", "http://localhost:3000"),
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

security = HTTPBearer(auto_error=True)


def get_clickhouse_client() -> Client:
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    database = os.getenv("CLICKHOUSE_DB", "bionicpro")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    return Client(host=host, port=port, database=database, user=user, password=password)


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> Dict[str, Any]:
    """
    Достаём user_id из JWT access token.
    В бою нужно проверять подпись по JWKS Keycloak.
    Для учебного проекта используем get_unverified_claims.
    """
    token = credentials.credentials
    try:
        claims = jwt.get_unverified_claims(token)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )

    sub = claims.get("sub")
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has no subject",
        )

    return {"user_id": sub, "claims": claims}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/reports")
def get_report(
    current_user: Dict[str, Any] = Depends(get_current_user),
    from_date: date = Query(..., description="Начало периода (включительно)"),
    to_date: date = Query(..., description="Конец периода (включительно)"),
) -> Dict[str, Any]:
    """
    Возвращает отчёт только для текущего пользователя.
    user_id берём из токена (sub) и НЕ позволяем передавать его в параметрах.

    Витрина mart_user_telemetry может отсутствовать (ещё не создали/не заполнили) —
    в этом случае просто возвращаем пустой отчёт без 500 ошибки.
    """
    user_id: str = current_user["user_id"]

    if to_date < from_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="to_date must be greater or equal to from_date",
        )

    client = get_clickhouse_client()

    # Попробуем узнать максимальный день из витрины.
    # Если витрина не создана (нет таблицы) — просто вернём пустой отчёт.
    try:
        max_day_row: Optional[List[Any]] = client.execute(
            "SELECT max(day) FROM mart_user_telemetry"
        )
    except ch_errors.Error:
        # Таблица не существует или другая ошибка ClickHouse => пустой отчёт.
        return {
            "userId": user_id,
            "from": str(from_date),
            "to": str(to_date),
            "items": [],
        }

    max_day = (
        max_day_row[0][0]
        if max_day_row and max_day_row[0][0] is not None
        else None
    )

    # Не отдаём данные за период, которого ещё нет в витрине
    if max_day is not None and to_date > max_day:
        to_date = max_day

    # Если max_day None (витрина пустая) — тоже возвращаем пустой отчёт
    if max_day is None:
        return {
            "userId": user_id,
            "from": str(from_date),
            "to": str(to_date),
            "items": [],
        }

    try:
        rows = client.execute(
            """
            SELECT
                day,
                events_count,
                avg_response_ms,
                p95_response_ms,
                errors_count
            FROM mart_user_telemetry
            WHERE user_id = %(user_id)s
              AND day BETWEEN %(from)s AND %(to)s
            ORDER BY day
            """,
            {"user_id": user_id, "from": from_date, "to": to_date},
        )
    except ch_errors.Error:
        # На случай, если в момент запроса таблицу удалили/переименовали.
        rows = []

    items = [
        {
            "day": str(row[0]),
            "eventsCount": int(row[1]),
            "avgResponseMs": float(row[2]),
            "p95ResponseMs": float(row[3]),
            "errorsCount": int(row[4]),
        }
        for row in rows
    ]

    return {
        "userId": user_id,
        "from": str(from_date),
        "to": str(to_date),
        "items": items,
    }