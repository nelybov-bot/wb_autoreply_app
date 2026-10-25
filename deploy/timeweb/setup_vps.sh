#!/usr/bin/env bash
# Автоматическая настройка Ubuntu VPS под WB Автоответчик (Timeweb и любой другой VPS).
# Запуск: от root на сервере, из корня клонированного репозитория:
#   sudo bash deploy/timeweb/setup_vps.sh
# Или с указанием каталога:
#   sudo bash deploy/timeweb/setup_vps.sh /var/www/wb_autoreply_app
#
# Перед запуском (опционально, для готового .env и домена):
#   export WB_DOMAIN=app.пример.ru
#   export WB_REPO_URL=https://github.com/ВАС/ wb_autoreply_app.git   # только если каталога ещё нет
#
set -euo pipefail

APP_ROOT="${1:-$(pwd)}"
APP_ROOT="$(cd "$APP_ROOT" && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Запустите от root: sudo bash $0"
  exit 1
fi

if [[ ! -f "$APP_ROOT/app/web/server.py" ]]; then
  echo "Не найден app/web/server.py в $APP_ROOT"
  echo "Склонируйте репозиторий и перейдите в его корень, либо передайте путь первым аргументом."
  if [[ -n "${WB_REPO_URL:-}" ]]; then
    mkdir -p "$(dirname "$APP_ROOT")"
    git clone "$WB_REPO_URL" "$APP_ROOT"
    cd "$APP_ROOT"
  else
    exit 1
  fi
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip nginx git curl openssl

mkdir -p "$APP_ROOT/data" "$APP_ROOT/logs"
chown -R www-data:www-data "$APP_ROOT"

sudo -u www-data bash -c "
  cd '$APP_ROOT'
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -r requirements-web.txt
"

ENV_FILE="/etc/wb_autoreply.env"
if [[ ! -f "$ENV_FILE" ]]; then
  SECRET="$(openssl rand -hex 32)"
  DOMAIN="${WB_DOMAIN:-}"
  if [[ -z "$DOMAIN" ]]; then
    read -r -p "Ваш домен для сайта (например app.example.ru), Enter — только IP без HTTPS-настроек: " DOMAIN
  fi
  CORS="https://${DOMAIN}"
  if [[ -z "${DOMAIN// /}" ]]; then
    CORS=""
    echo "SESSION_SECRET=$SECRET" >"$ENV_FILE"
    echo "# CORS_ORIGINS= — задайте позже, когда будет домен" >>"$ENV_FILE"
    echo "# COOKIE_SECURE=1 — включите с HTTPS" >>"$ENV_FILE"
  else
    {
      echo "SESSION_SECRET=$SECRET"
      echo "CORS_ORIGINS=$CORS"
      echo "COOKIE_SECURE=1"
    } >"$ENV_FILE"
  fi
  chmod 600 "$ENV_FILE"
  echo "Создан $ENV_FILE (права 600). При необходимости отредактируйте: nano $ENV_FILE"
else
  echo "Уже есть $ENV_FILE — не перезаписываю."
fi

UNIT="/etc/systemd/system/wb-autoreply.service"
sed -e "s|__APP_ROOT__|$APP_ROOT|g" "$SCRIPT_DIR/wb-autoreply.service.template" >"$UNIT"
chmod 644 "$UNIT"
systemctl daemon-reload
systemctl enable wb-autoreply
systemctl restart wb-autoreply

NGX_TMPL="$SCRIPT_DIR/nginx-site.conf.template"
if [[ -n "${WB_DOMAIN:-}" ]] || grep -q '^CORS_ORIGINS=https://' "$ENV_FILE" 2>/dev/null; then
  DOM="${WB_DOMAIN:-}"
  if [[ -z "$DOM" ]]; then
    DOM=$(grep '^CORS_ORIGINS=' "$ENV_FILE" | sed 's/^CORS_ORIGINS=https:\/\///' | tr -d '\r')
  fi
  if [[ -n "${DOM// /}" ]]; then
    NGX_SITE="/etc/nginx/sites-available/wb-autoreply"
    sed -e "s/__DOMAIN__/$DOM/g" "$NGX_TMPL" >"$NGX_SITE"
    ln -sf "$NGX_SITE" /etc/nginx/sites-enabled/wb-autoreply
    rm -f /etc/nginx/sites-enabled/default
    nginx -t
    systemctl reload nginx
    echo ""
    echo "Nginx настроен на домен: $DOM"
    echo "Убедитесь, что A-запись домена указывает на IP этого сервера, затем:"
    echo "  apt-get install -y certbot python3-certbot-nginx"
    echo "  certbot --nginx -d $DOM"
  fi
else
  echo ""
  echo "Nginx для домена не настраивал (нет домена). Вручную позже:"
  echo "  sed -e \"s/__DOMAIN__/ВАШ_ДОМЕН/g\" $NGX_TMPL > /etc/nginx/sites-available/wb-autoreply"
  echo "  ln -sf /etc/nginx/sites-available/wb-autoreply /etc/nginx/sites-enabled/"
  echo "  nginx -t && systemctl reload nginx"
fi

echo ""
echo "Сервис: systemctl status wb-autoreply"
echo "Логи:   journalctl -u wb-autoreply -f"
