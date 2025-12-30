# Учебные мини-игры (edu-minigames)

Веб‑платформа с набором образовательных мини‑игр и единым личным кабинетом:
- авторизация по одноразовому коду через Telegram Gateway (или mock‑режим для отладки),
- сохранения прогресса (save/load),
- статистика (последний результат и рекорд),
- логирование игровых сессий (raw‑события в Object Storage + агрегаты в YDB).

Подробное описание архитектуры, API, структуры репозитория и CI/CD — в документе **«Документация_0_3.docx»**.

## Быстрый старт (локальная разработка)

Требования:
- Node.js **18+**

Запуск:
```bash
npm ci
npm run dev
```

Локальный dev‑сервер поднимает статические файлы и проксирует `/api/*` на локальную реализацию обработчика (см. `local/dev-server.mjs`).

## Основные компоненты

- **Frontend (static)**: `public/` (главная `index.html`, игры `public/games/*.html`, системные страницы `policy.html`, `error.html`, ассеты).
- **Platform scripts**:
  - `public/platform/client.js` — клиент API (JWT в `localStorage`).
  - `public/platform/bridge.js` — оверлей «Сохранить/Выйти» в игре + save/load + start/finish сессии.
- **Backend**: Cloud Function Node.js (`backend/function/app.mjs`) + API Gateway (`openapi.yaml`).
- **Хранилища**:
  - YDB — пользователи, коды, сохранения, статистика, сессии.
  - Object Storage — статический сайт и `raw`‑события сессий.

## Контракт интеграции игры (обязательно)

Каждая игра должна экспортировать объект `window.GameInstance` со следующими методами:
- `exportState()` → объект состояния для сохранения
- `importState(state)` → восстановление состояния
- `getSessionSummary()` → сводка (например, `stars_total`, `stars_by_level`, `level_reached`)

Также игра подключает:
```html
<script src="/platform/client.js"></script>
<script src="/platform/bridge.js"></script>
```

## API (кратко)

- `POST /api/auth/start` `{ phone }` — отправить код (Telegram Gateway / mock)
- `POST /api/auth/verify` `{ phone, code }` — получить JWT
- `GET /api/me` — профиль + список игр
- `PUT /api/games/{gameId}/save` `{ save }` — сохранить
- `GET /api/games/{gameId}/save` — получить сохранение
- `POST /api/games/{gameId}/session/start` — старт сессии
- `POST /api/games/{gameId}/session/{sessionId}/finish` — завершение сессии (summary + events)

## Деплой (CI/CD)

В репозитории используются GitHub Actions:

- `.github/workflows/deploy-static.yml` — синхронизация `public/` в Object Storage (`STATIC_BUCKET`).
- `.github/workflows/deploy-api.yml` — деплой Cloud Function и обновление API Gateway.

### GitHub Secrets

Обязательные:
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — доступ к Object Storage
- `JWT_SECRET` — подпись JWT
- `YDB_DB_NAME` — имя базы YDB (в формате из консоли YDB)
- `TELEGRAM_GATEWAY_TOKEN` — API‑токен Telegram Gateway для отправки кода
- `YC_SA_KEY_JSON_B64` — key в base64 для `yc` CLI
- `YC_FUNCTION_SA_ID`, `YC_GATEWAY_SA_ID` — service account IDs

### GitHub Variables

Ключевые:
- `YC_FOLDER_ID`, `YC_FUNCTION_ID`, `YC_API_GW_ID`
- `STATIC_BUCKET` — бакет со статикой
- `RAW_BUCKET` — бакет для raw‑событий
- `S3_ENDPOINT` (обычно `https://storage.yandexcloud.net`), `AWS_REGION` (`ru-central1`)
- `YDB_DB_NAME`, `YDB_TABLE_PREFIX`, `LOGS_PREFIX`
- `SMS_MODE` (`gateway`/`mock`) — режим доставки кода:
  - `gateway`: отправляем код через Telegram Gateway (номер должен быть привязан к Telegram).
  - `mock`: код всегда `0000`, запрос в Telegram Gateway не отправляется (для отладки).
- `DEBUG_OTP` (`1`/`0`) — если `1`, backend вернёт `debug_code` в ответе `POST /api/auth/start`.
- `TELEGRAM_GATEWAY_TTL` (сек, опционально; по умолчанию `600`) — TTL кода на стороне Telegram Gateway.

## Статический сайт с «красивым» URL

Если включить **Website hosting** на бакете со статикой, фронтенд будет доступен как:
`https://<bucket>.website.yandexcloud.net/`

Для этого в настройках бакета нужно указать:
- **Index document**: `index.html`
- **Error document**: `error.html`

> Важно: при разделении доменов (bucket website ≠ API Gateway) потребуется CORS и/или настройка базового URL для API в `client.js/bridge.js`.

## Политика ПДн

Проект собирает номер телефона для авторизации. Текст политики: `policy.html`.

---

**Документация:** `Документация_0_3.docx`
