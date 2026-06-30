#!/usr/bin/env bash
# Минимальный HTTP-прокси в РФ для Render → pub.fsa.gov.ru (только ФСА).
# Запуск на Ubuntu VPS (Timeweb / Selectel и т.д.), от root:
#   sudo bash deploy/fsa-proxy/setup_ru_proxy.sh
#
# После установки скопируйте строку FSA_PROXY_URL в Render → Environment.
#
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Запустите от root: sudo bash $0"
  exit 1
fi

PROXY_USER="${FSA_PROXY_USER:-fsa_render}"
PROXY_PASS="${FSA_PROXY_PASS:-$(openssl rand -base64 18 | tr -d '/+=' | head -c 20)}"
PROXY_PORT="${FSA_PROXY_PORT:-3128}"

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq squid apache2-utils

PASSFILE="/etc/squid/passwords"
htpasswd -b -c "$PASSFILE" "$PROXY_USER" "$PROXY_PASS" 2>/dev/null || htpasswd -b "$PASSFILE" "$PROXY_USER" "$PROXY_PASS"
chown proxy:proxy "$PASSFILE"
chmod 640 "$PASSFILE"

CFG="/etc/squid/squid.conf"
if [[ -f "$CFG" && ! -f "${CFG}.bak.fsa" ]]; then
  cp "$CFG" "${CFG}.bak.fsa"
fi

cat >"$CFG" <<EOF
# FSA proxy for Render (wb_autoreply_app) — minimal config
http_port ${PROXY_PORT}

auth_param basic program /usr/lib/squid/basic_ncsa_auth ${PASSFILE}
auth_param basic realm FSA Proxy
acl authenticated proxy_auth REQUIRED
http_access allow authenticated
http_access deny all

# Только исходящие HTTPS (ФСА и др.)
acl SSL_ports port 443
acl CONNECT method CONNECT
http_access deny CONNECT !SSL_ports

dns_v4_first on
forwarded_for off
via off

cache deny all
EOF

systemctl enable squid
systemctl restart squid

PUBLIC_IP="$(curl -4 -s --max-time 5 ifconfig.me || curl -4 -s --max-time 5 icanhazip.com || hostname -I | awk '{print $1}')"

echo ""
echo "=============================================="
echo " HTTP-прокси для ФСА установлен (squid)"
echo "=============================================="
echo ""
echo "Порт:     ${PROXY_PORT}"
echo "Логин:    ${PROXY_USER}"
echo "Пароль:   ${PROXY_PASS}"
echo ""
echo "В Render → Environment добавьте:"
echo ""
echo "FSA_PROXY_URL=http://${PROXY_USER}:${PROXY_PASS}@${PUBLIC_IP}:${PROXY_PORT}"
echo ""
echo "Проверка с VPS:"
echo "  curl -x http://${PROXY_USER}:${PROXY_PASS}@127.0.0.1:${PROXY_PORT} -I https://pub.fsa.gov.ru/login"
echo ""
echo "Сохраните пароль — он больше не будет показан."
echo "=============================================="
