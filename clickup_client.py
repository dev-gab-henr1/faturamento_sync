"""
Cliente leve para a API do ClickUp.
Busca tasks com paginação, suporta date_updated_gt para delta sync.
Aceita transform para slim de tasks durante o fetch (economia de memória).
"""
import time
import logging
from typing import Callable

import requests

from config import CLICKUP_TOKEN, CLICKUP_BASE_URL, CLICKUP_LIST_IDS
from stats import stats

logger = logging.getLogger(__name__)

_SESSION: requests.Session | None = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "Authorization": CLICKUP_TOKEN,
            "Content-Type": "application/json",
        })
    return _SESSION


def reset_session() -> None:
    """Fecha e descarta a session HTTP. Próxima chamada cria uma nova."""
    global _SESSION
    if _SESSION is not None:
        try:
            _SESSION.close()
        except Exception:
            pass
        _SESSION = None
    logger.info("ClickUp session resetada.")


def fetch_tasks(
    list_id: str,
    *,
    include_closed: bool = True,
    date_updated_gt: int | None = None,
    page_limit: int = 100,
    transform: Callable[[dict], dict] | None = None,
) -> list[dict]:
    session = _get_session()
    all_tasks: list[dict] = []
    page = 0

    while True:
        params: dict = {
            "page": page,
            "limit": page_limit,
            "include_closed": str(include_closed).lower(),
            "subtasks": "true",
        }
        if date_updated_gt is not None:
            params["date_updated_gt"] = str(date_updated_gt)

        url = f"{CLICKUP_BASE_URL}/list/{list_id}/task"

        for attempt in range(4):
            try:
                resp = session.get(url, params=params, timeout=30)
                stats.clickup_requests += 1

                if resp.status_code in (500, 502, 503):
                    wait = 2 ** attempt
                    logger.warning(
                        "ClickUp %s (attempt %d), retry in %ds",
                        resp.status_code, attempt + 1, wait,
                    )
                    time.sleep(wait)
                    continue

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", "30"))
                    logger.warning(
                        "ClickUp rate limit 429, aguardando %ds", retry_after,
                    )
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()
                break
            except requests.exceptions.RequestException as exc:
                stats.clickup_requests += 1
                if attempt == 3:
                    logger.error("ClickUp request failed: %s", exc)
                    raise
                time.sleep(2 ** attempt)
        else:
            break

        data = resp.json()
        tasks = data.get("tasks", [])

        # Slim cada task imediatamente (antes de acumular na lista)
        if transform:
            tasks = [transform(t) for t in tasks]

        all_tasks.extend(tasks)
        stats.clickup_tasks_fetched += len(tasks)

        if len(tasks) < page_limit:
            break
        page += 1
        time.sleep(0.3)

    return all_tasks


def fetch_all_tasks(
    *,
    include_closed: bool = True,
    date_updated_gt: int | None = None,
    transform: Callable[[dict], dict] | None = None,
) -> list[dict]:
    all_tasks: list[dict] = []
    for list_id in CLICKUP_LIST_IDS:
        logger.info("Fetching list %s ...", list_id)
        tasks = fetch_tasks(
            list_id,
            include_closed=include_closed,
            date_updated_gt=date_updated_gt,
            transform=transform,
        )
        logger.info("  → %d tasks", len(tasks))
        all_tasks.extend(tasks)
    return all_tasks