import os

# ─── Обязательные переменные (задаются в .env или docker-compose.yml) ──────────
BOT_TOKEN  = os.environ["BOT_TOKEN"]          # токен @BotFather
ADMIN_IDS  = [
    int(x.strip())
    for x in os.environ["ADMIN_IDS"].split(",")   # "111111,222222"
]
CHANNEL_ID  = os.environ["CHANNEL_ID"]        # "@mychannel" или "-100xxxxxxxxxx"
CHANNEL_URL = os.environ["CHANNEL_URL"]       # "https://t.me/mychannel"

# ─── Опциональные переменные (есть дефолты) ────────────────────────────────────
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Moscow")

# Хэштеги барахолки
HASHTAGS: list[str] = [
    tag.strip()
    for tag in os.environ.get(
        "HASHTAGS",
        "#продаю,#куплю,#обмен,#отдам,#ищу",
    ).split(",")
    if tag.strip()
]
