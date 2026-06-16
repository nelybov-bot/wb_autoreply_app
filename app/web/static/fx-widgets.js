(function () {
  'use strict';

  const COW_DISMISS_QUIET_MS = 20000;
  let cowDismissedUntil = 0;
  let cowVisible = false;

  function isDarkTheme() {
    try {
      return localStorage.getItem('marketai_ui_theme') === 'dark';
    } catch (_) {
      return false;
    }
  }

  function syncLampVisual(lamp) {
    if (!lamp) return;
    lamp.classList.toggle('fx-lamp--on', isDarkTheme());
    lamp.setAttribute('aria-pressed', isDarkTheme() ? 'true' : 'false');
  }

  function toggleThemeFromLamp() {
    if (typeof window.marketaiToggleTheme === 'function') {
      window.marketaiToggleTheme();
      return;
    }
    try {
      const dark = isDarkTheme();
      localStorage.setItem('marketai_ui_theme', dark ? 'light' : 'dark');
      document.body.classList.toggle('theme-dark', !dark);
    } catch (_) {}
    syncLampVisual(document.getElementById('fx-lamp'));
    const cb = document.getElementById('theme-dark');
    if (cb) cb.checked = isDarkTheme();
  }

  function pullLampCord() {
    const lamp = document.getElementById('fx-lamp');
    if (!lamp) return;
    lamp.classList.add('fx-lamp--pull');
    setTimeout(() => {
      toggleThemeFromLamp();
      lamp.classList.remove('fx-lamp--pull');
      syncLampVisual(lamp);
    }, 380);
  }

  function initLamp() {
    const lamp = document.getElementById('fx-lamp');
    const cord = document.getElementById('fx-lamp-cord');
    if (!lamp || !cord) return;
    syncLampVisual(lamp);
    cord.addEventListener('click', pullLampCord);
    cord.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        pullLampCord();
      }
    });
    document.addEventListener('marketai-theme-change', () => syncLampVisual(lamp));
  }

  function hideCow(opts) {
    const el = document.getElementById('fx-cow-screen');
    if (!el) return;
    el.classList.remove('visible');
    document.body.classList.remove('fx-cow-screen-open');
    cowVisible = false;
    if (!opts || !opts.success) {
      cowDismissedUntil = Date.now() + COW_DISMISS_QUIET_MS;
    } else {
      cowDismissedUntil = 0;
    }
  }

  /** Полноэкранная корова — только потеря соединения (Failed to fetch). */
  function showCowOffline(message) {
    if (Date.now() < cowDismissedUntil) return;

    const el = document.getElementById('fx-cow-screen');
    const text = document.getElementById('fx-cow-screen-text');
    if (!el || !text) return;

    text.textContent = message || 'Не удалось связаться с сервером. Проверьте интернет, адрес API или подождите, если сервер на Render «спит».';
    el.classList.add('visible');
    document.body.classList.add('fx-cow-screen-open');
    cowVisible = true;
  }

  function initCow() {
    const retry = document.getElementById('fx-cow-retry');
    const close = document.getElementById('fx-cow-close');
    if (retry) {
      retry.addEventListener('click', () => {
        hideCow();
        cowDismissedUntil = 0;
        window.location.reload();
      });
    }
    if (close) close.addEventListener('click', hideCow);
  }

  window.MarketAIFx = {
    showCowOffline,
    hideCow,
    syncLampVisual,
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      initLamp();
      initCow();
    });
  } else {
    initLamp();
    initCow();
  }
})();
