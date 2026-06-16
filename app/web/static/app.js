(function () {
  'use strict';

  const API = '/api';
  const STORAGE_API_BASE = 'wb_autoreply_api_base';
  const STORAGE_UI_COMPACT = 'marketai_ui_compact';
  const STORAGE_UI_DIM_BG = 'marketai_ui_dim_bg';
  const STORAGE_UI_REDUCE_MOTION = 'marketai_ui_reduce_motion';
  const STORAGE_UI_BG_MOTION = 'marketai_ui_bg_motion';
  const STORAGE_UI_THEME = 'marketai_ui_theme';
  const STORAGE_UI_TOAST_MS = 'marketai_ui_toast_ms';
  const STORAGE_UI_CONFIRM_DANGER = 'marketai_ui_confirm_danger';
  const STORAGE_UI_PREFS_VERSION = 'marketai_ui_prefs_version';
  const UI_PREFS_VERSION = '2';

  function migrateUiPrefsIfNeeded() {
    try {
      const current = localStorage.getItem(STORAGE_UI_PREFS_VERSION) || '';
      if (current === UI_PREFS_VERSION) return;
      // Reset old visual prefs once to avoid inheriting heavy/legacy look
      localStorage.removeItem(STORAGE_UI_COMPACT);
      localStorage.removeItem(STORAGE_UI_DIM_BG);
      localStorage.removeItem(STORAGE_UI_REDUCE_MOTION);
      localStorage.removeItem(STORAGE_UI_BG_MOTION);
      localStorage.removeItem(STORAGE_UI_THEME);
      localStorage.setItem(STORAGE_UI_PREFS_VERSION, UI_PREFS_VERSION);
    } catch (_) {}
  }

  function getUiToastMs() {
    const v = parseInt(localStorage.getItem(STORAGE_UI_TOAST_MS) || '4000', 10);
    return Number.isFinite(v) ? Math.max(1500, Math.min(15000, v)) : 4000;
  }

  function applyUiPrefs() {
    try {
      document.body.classList.toggle('ui-compact', localStorage.getItem(STORAGE_UI_COMPACT) === '1');
      document.body.classList.toggle('ui-dim-bg', localStorage.getItem(STORAGE_UI_DIM_BG) === '1');
      document.body.classList.toggle('ui-reduce-motion', localStorage.getItem(STORAGE_UI_REDUCE_MOTION) === '1');
      document.body.classList.toggle('ui-bg-motion', localStorage.getItem(STORAGE_UI_BG_MOTION) === '1');
      document.body.classList.toggle('theme-dark', localStorage.getItem(STORAGE_UI_THEME) === 'dark');
    } catch (_) {}
  }

  function toggleUiTheme() {
    try {
      const dark = localStorage.getItem(STORAGE_UI_THEME) === 'dark';
      localStorage.setItem(STORAGE_UI_THEME, dark ? 'light' : 'dark');
      applyUiPrefs();
      const cb = document.getElementById('theme-dark');
      if (cb) cb.checked = !dark;
      document.dispatchEvent(new CustomEvent('marketai-theme-change', { detail: { dark: !dark } }));
      if (window.MarketAIFx && window.MarketAIFx.syncLampVisual) {
        window.MarketAIFx.syncLampVisual(document.getElementById('fx-lamp'));
      }
    } catch (_) {}
  }
  window.marketaiToggleTheme = toggleUiTheme;

  function confirmDanger(message) {
    const need = (localStorage.getItem(STORAGE_UI_CONFIRM_DANGER) || '1') === '1';
    if (!need) return true;
    return confirm(message || 'Вы уверены?');
  }

  let bgParallaxMainEl = null;
  let bgParallaxHandler = null;
  function syncBgParallaxListener() {
    try {
      if (!bgParallaxMainEl) bgParallaxMainEl = document.querySelector('.main');
      if (!bgParallaxMainEl) return;
      if (!bgParallaxHandler) {
        bgParallaxHandler = () => {
          const v = bgParallaxMainEl.scrollTop || 0;
          document.body.style.setProperty('--bg-parallax-y', (-v * 0.03) + 'px');
        };
      }
      const enabled = document.body.classList.contains('ui-bg-motion');
      if (enabled) {
        bgParallaxMainEl.addEventListener('scroll', bgParallaxHandler, { passive: true });
        bgParallaxHandler();
      } else {
        bgParallaxMainEl.removeEventListener('scroll', bgParallaxHandler);
        document.body.style.setProperty('--bg-parallax-y', '0px');
      }
    } catch (_) {}
  }

  function getApiBase() {
    return (localStorage.getItem(STORAGE_API_BASE) || '').trim().replace(/\/$/, '');
  }

  async function api(path, options = {}) {
    const base = getApiBase();
    const url = path.startsWith('http') ? path : (base ? base + '/api' + path : API + path);
    const timeoutMs = Number(options.timeoutMs) || 0;
    const { timeoutMs: _timeoutMs, ...fetchOptions } = options;
    let timer = null;
    let controller = null;
    if (timeoutMs > 0 && !fetchOptions.signal) {
      controller = new AbortController();
      fetchOptions.signal = controller.signal;
      timer = setTimeout(() => controller.abort(), timeoutMs);
    }
    let res;
    try {
      res = await fetch(url, {
        ...fetchOptions,
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          ...fetchOptions.headers,
        },
      });
    } catch (e) {
      if (e && e.name === 'AbortError') {
        throw new Error('Превышено время ожидания ответа сервера. На Render первый запрос после простоя может занять до минуты — подождите и нажмите «Обновить» ещё раз.');
      }
      if (window.MarketAIFx && window.MarketAIFx.showCowOffline) {
        window.MarketAIFx.showCowOffline('Соединение с сервером потеряно. Проверьте сеть или адрес API в настройках.');
      }
      const baseHint = base ? `\nПроверь «Адрес API (ПК)»: сейчас = ${base}` : '';
      throw new Error('Не удалось подключиться к серверу (Failed to fetch).' + baseHint + '\nЧастые причины: неверный адрес API, CORS, смешанный контент (http/https), сервер спит на Render.');
    } finally {
      if (timer) clearTimeout(timer);
    }
    if (res.ok && window.MarketAIFx && window.MarketAIFx.hideCow) {
      window.MarketAIFx.hideCow({ success: true });
    }
    if (!res.ok) {
      const text = await res.text();
      let err;
      try {
        err = JSON.parse(text);
      } catch (_) {
        err = { detail: text };
      }
      const msg = Array.isArray(err.detail) ? err.detail.map(d => d.msg || d).join(' ') : (err.detail || res.statusText);
      if (res.status === 401) {
        if (!location.pathname.startsWith('/login')) {
          window.location.href = '/login';
        }
      }
      if (res.status === 409) {
        throw new Error(msg || 'Магазин занят другой задачей. Дождитесь завершения или остановите её.');
      }
      throw new Error(msg);
    }
    if (res.status === 204 || res.headers.get('content-length') === '0') return null;
    return res.json();
  }

  function toast(message, type = 'success') {
    let container = document.getElementById('toast-container');
    if (!container) {
      container = document.createElement('div');
      container.id = 'toast-container';
      container.className = 'toast-container';
      container.setAttribute('aria-live', 'polite');
      document.body.appendChild(container);
    }
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    const icon = document.createElement('span');
    icon.className = 'toast-icon';
    icon.innerHTML = type === 'success' ? '✓' : (type === 'info' ? 'i' : '✕');
    const text = document.createElement('span');
    text.className = 'toast-text';
    text.textContent = String(message || '');
    el.appendChild(icon);
    el.appendChild(text);
    container.appendChild(el);
    const max = 5;
    while (container.children.length > max) {
      container.removeChild(container.firstChild);
    }
    setTimeout(() => el.remove(), getUiToastMs());
  }

  function setButtonBusy(btn, busy, busyLabel) {
    if (!btn) return;
    if (busy) {
      if (!btn.dataset.origText) btn.dataset.origText = btn.textContent;
      btn.disabled = true;
      btn.classList.add('is-busy');
      if (busyLabel) btn.textContent = busyLabel;
    } else {
      btn.disabled = false;
      btn.classList.remove('is-busy');
      if (btn.dataset.origText) {
        btn.textContent = btn.dataset.origText;
        delete btn.dataset.origText;
      }
    }
  }

  function setPanelOpsBusy(panelPrefix, busy) {
    const suffix = panelPrefix === 'reviews' ? 'reviews' : 'questions';
    ['load', 'generate', 'send'].forEach((action) => {
      document.querySelectorAll(`#btn-${action}-${suffix}, #btn-${action}-${suffix}-2`).forEach((btn) => {
        setButtonBusy(btn, busy, busy ? 'Выполняется…' : '');
      });
    });
  }

  function closeAllModals() {
    document.querySelectorAll('.modal-backdrop.visible').forEach((m) => m.classList.remove('visible'));
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeAllModals();
  });

  let _modalItemCtx = null;

  function applyTabVisibility() {
    const canSettings = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_settings')));
    const canLog = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_log')));
    const canOpsLog = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_ops_log')));
    const isAdmin = currentUser && currentUser.role === 'admin';
    document.querySelectorAll('.nav-link[data-tab="auto"]').forEach(el => {
      el.style.display = canSettings ? '' : 'none';
    });
    document.querySelectorAll('.nav-menu-settings').forEach(el => {
      el.style.display = canSettings ? '' : 'none';
    });
    document.querySelectorAll('.nav-link[data-tab="log"]').forEach(el => {
      el.style.display = (canLog || canOpsLog) ? '' : 'none';
    });
    document.querySelectorAll('#nav-settings-users, #settings-seg-users').forEach(el => {
      el.style.display = isAdmin ? '' : 'none';
    });
    const importCfgBtn = document.getElementById('btn-config-import');
    if (importCfgBtn) importCfgBtn.style.display = isAdmin ? '' : 'none';
    const panelSettings = document.getElementById('panel-settings');
    const panelAuto = document.getElementById('panel-auto');
    const panelLog = document.getElementById('panel-log');
    if (panelSettings) panelSettings.style.display = canSettings ? '' : 'none';
    if (panelAuto) panelAuto.style.display = canSettings ? '' : 'none';
    if (panelLog) panelLog.style.display = (canLog || canOpsLog) ? '' : 'none';
    const logModeSel = document.getElementById('log-mode');
    if (logModeSel) {
      const devOpt = logModeSel.querySelector('option[value="dev"]');
      if (devOpt) devOpt.hidden = !canLog;
      if (!canLog && logModeSel.value === 'dev') logModeSel.value = 'ops';
    }
  }

  let ozonChatsFilter = 'buyers';
  let ozonActionsSection = 'list';
  let settingsSection = 'connection';

  function setNavActive(tabId, opts = {}) {
    const chatFilter = opts.chatFilter || ozonChatsFilter;
    const actionsSection = opts.actionsSection || ozonActionsSection;
    const settingsSec = opts.settingsSection || settingsSection;

    document.querySelectorAll('.nav-link[data-tab]').forEach(el => {
      el.classList.toggle('active', el.getAttribute('data-tab') === tabId);
    });

    document.querySelectorAll('.nav-dd-item[data-tab]').forEach(el => {
      const matchTab = el.getAttribute('data-tab') === tabId;
      const cf = el.getAttribute('data-chat-filter') || '';
      const as = el.getAttribute('data-actions-section') || '';
      const ss = el.getAttribute('data-settings-section') || '';
      let active = false;
      if (tabId === 'ozon-chats' && cf) {
        active = matchTab && cf === chatFilter;
      } else if (tabId === 'ozon-actions' && as) {
        active = matchTab && as === actionsSection;
      } else if (tabId === 'settings' && ss) {
        active = matchTab && ss === settingsSec;
      } else {
        active = matchTab && !cf && !as && !ss;
      }
      el.classList.toggle('active', active);
    });

    document.querySelectorAll('.nav-menu').forEach(menu => {
      const hit = [...menu.querySelectorAll('.nav-dd-item[data-tab]')].some(el => {
        if (el.getAttribute('data-tab') !== tabId) return false;
        const cf = el.getAttribute('data-chat-filter') || '';
        const as = el.getAttribute('data-actions-section') || '';
        const ss = el.getAttribute('data-settings-section') || '';
        if (tabId === 'ozon-chats' && cf) return cf === chatFilter;
        if (tabId === 'ozon-actions' && as) return as === actionsSection;
        if (tabId === 'settings' && ss) return ss === settingsSec;
        return !cf && !as && !ss;
      });
      const chatsActive = menu.classList.contains('nav-menu-chats') && (tabId === 'wb-chats' || tabId === 'ozon-chats');
      const ozonPromoActive = menu.classList.contains('nav-menu-ozon-promo') && tabId === 'ozon-actions';
      const settingsActive = menu.classList.contains('nav-menu-settings') && tabId === 'settings';
      menu.classList.toggle('active', hit || chatsActive || ozonPromoActive || settingsActive);
    });
  }

  function closeNavMenus(exceptMenu) {
    document.querySelectorAll('.nav-menu.open').forEach(menu => {
      if (exceptMenu && menu === exceptMenu) return;
      menu.classList.remove('open');
      const trigger = menu.querySelector('.nav-trigger');
      if (trigger) trigger.setAttribute('aria-expanded', 'false');
    });
  }

  function scrollToOzonActionsSection(section) {
    const map = { list: 'ozon-actions-list', settings: 'ozon-actions-settings', manual: 'ozon-actions-manual' };
    const id = map[section] || map.list;
    const el = document.getElementById(id);
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function syncSettingsSectionUI() {
    const isAdmin = currentUser && currentUser.role === 'admin';
    if (settingsSection === 'users' && !isAdmin) settingsSection = 'connection';
    document.querySelectorAll('#settings-filter [data-settings-section]').forEach(btn => {
      if (btn.style.display === 'none') return;
      btn.classList.toggle('active', btn.getAttribute('data-settings-section') === settingsSection);
    });
    document.querySelectorAll('.settings-section').forEach(sec => {
      const id = sec.id.replace('settings-section-', '');
      sec.hidden = id !== settingsSection;
    });
  }

  function activatePanel(tabId, opts = {}) {
    if (!tabId) return;
    const panel = document.getElementById('panel-' + tabId);
    if (!panel || panel.style.display === 'none') return;
    if (opts.chatFilter) {
      ozonChatsFilter = opts.chatFilter;
      syncOzonChatsFilterUI();
    }
    if (opts.actionsSection) {
      ozonActionsSection = opts.actionsSection;
    }
    if (opts.settingsSection) {
      settingsSection = opts.settingsSection;
    }
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    panel.classList.add('active');
    setNavActive(tabId, opts);
    closeNavMenus();
    if (tabId === 'summary') loadStats();
    if (tabId === 'stores') loadStores();
    if (tabId === 'reviews') { loadReviews(); resumePanelTask('reviews'); }
    if (tabId === 'questions') { loadQuestions(); resumePanelTask('questions'); }
    if (tabId === 'wb-chats') loadWbChatsPanel();
    if (tabId === 'ozon-chats') loadOzonChatsPanel();
    if (tabId === 'ozon-actions') {
      loadOzonActionsPanel();
      if (opts.actionsSection) {
        setTimeout(() => scrollToOzonActionsSection(ozonActionsSection), 80);
      }
    }
    if (tabId === 'card-links') loadCardLinksPanel();
    if (tabId === 'auto') loadAutoSchedulePanel();
    if (tabId === 'agent') loadAgentPanel();
    if (tabId === 'settings') {
      syncSettingsSectionUI();
      loadSettings();
    }
    if (tabId === 'log') loadLog();
    if (tabId === 'card-errors') loadCardErrors();
    if (tabId === 'ozon-alerts') loadOzonAlerts();
  }

  function wireAppNav() {
    document.querySelectorAll('.nav-link[data-tab], .nav-dd-item[data-tab]').forEach(el => {
      el.addEventListener('click', () => {
        const tab = el.getAttribute('data-tab');
        const chatFilter = el.getAttribute('data-chat-filter') || '';
        const actionsSection = el.getAttribute('data-actions-section') || '';
        const settingsSec = el.getAttribute('data-settings-section') || '';
        activatePanel(tab, {
          chatFilter: chatFilter || undefined,
          actionsSection: actionsSection || undefined,
          settingsSection: settingsSec || undefined,
        });
      });
    });
    document.querySelectorAll('.nav-menu').forEach(menu => {
      const trigger = menu.querySelector('.nav-trigger');
      if (!trigger) return;
      trigger.addEventListener('click', (e) => {
        if (window.matchMedia('(hover: hover)').matches) return;
        e.preventDefault();
        const open = menu.classList.contains('open');
        closeNavMenus();
        if (!open) {
          menu.classList.add('open');
          trigger.setAttribute('aria-expanded', 'true');
        }
      });
    });
    document.addEventListener('click', (e) => {
      if (!e.target.closest('.nav-menu')) closeNavMenus();
    });
  }

  function _activeTaskKey(panelPrefix) {
    return 'activeTask_' + panelPrefix;
  }

  function setActiveTask(panelPrefix, taskId) {
    try {
      if (taskId) localStorage.setItem(_activeTaskKey(panelPrefix), String(taskId));
      else localStorage.removeItem(_activeTaskKey(panelPrefix));
    } catch (_) {}
  }

  function getActiveTask(panelPrefix) {
    try { return localStorage.getItem(_activeTaskKey(panelPrefix)) || ''; } catch (_) { return ''; }
  }

  async function resumePanelTask(panelPrefix) {
    const taskId = getActiveTask(panelPrefix);
    if (!taskId) return;
    try {
      const state = await api('/tasks/' + taskId);
      if (!state || !state.status) {
        setActiveTask(panelPrefix, '');
        return;
      }
      if (state.status === 'running') {
        setPanelOpsBusy(panelPrefix, true);
        pollTask(taskId, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', () => {
          setActiveTask(panelPrefix, '');
          setPanelOpsBusy(panelPrefix, false);
          if (panelPrefix === 'reviews') loadReviews();
          else if (panelPrefix === 'questions') loadQuestions();
        }, panelPrefix);
      } else {
        setActiveTask(panelPrefix, '');
      }
    } catch (err) {
      if (err && String(err.message || '').includes('не найдена')) {
        setActiveTask(panelPrefix, '');
      }
    }
  }

  // ---- Navigation ----
  wireAppNav();

  // ---- Сводка ----
  function wireSummaryOpsToggle() {
    const btn = document.getElementById('btn-summary-ops-toggle');
    const body = document.getElementById('summary-ops-body');
    const wrap = document.getElementById('summary-ops-collapsible');
    if (!btn || !body) return;
    const setOpen = (open) => {
      btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      body.hidden = !open;
      if (wrap) wrap.classList.toggle('is-open', open);
    };
    setOpen(false);
    btn.addEventListener('click', () => {
      setOpen(btn.getAttribute('aria-expanded') !== 'true');
    });
  }

  wireSummaryOpsToggle();

  async function loadStats() {
    try {
      const s = await api('/stats');
      const q = s.queue || {};
      const set = (id, v) => {
        const el = document.getElementById(id);
        if (el) el.textContent = v;
      };
      set('stat-today', s.sent_today ?? 0);
      set('stat-queue-reviews', q.new_reviews ?? 0);
      set('stat-queue-questions', q.new_questions ?? 0);
      set('stat-wb-chats-today', s.wb_chat_sent_today ?? 0);
      set('stat-ozon-chats-today', s.ozon_chat_sent_today ?? 0);
      const metaEl = document.getElementById('summary-ops-toggle-meta');
      if (metaEl) {
        const sent = Number(s.sent_today) || 0;
        const queue = (Number(q.new_reviews) || 0) + (Number(q.new_questions) || 0);
        metaEl.textContent = `сегодня ${sent} · в очереди ${queue}`;
      }
      const storesMeta = s.stores || {};
      let auto = null;
      try { auto = await api('/auto-schedule/status'); } catch (_) {}
      const autoPhaseRu = {
        idle: 'ожидание',
        load_new: 'загрузка новых',
        generate: 'генерация',
        send: 'отправка',
        wb_chats: 'чаты WB',
        ozon_chats: 'чаты Ozon',
        ozon_actions: 'акции Ozon',
        idle_items: 'без отзывов/вопросов',
        done: 'завершено',
        cancelled: 'остановлено',
        error: 'ошибка',
      };
      const summary = [
        { k: 'Сгенерировано (отзывы)', v: String(q.generated_reviews ?? 0) },
        { k: 'Сгенерировано (вопросы)', v: String(q.generated_questions ?? 0) },
        { k: 'Магазины активные', v: `${storesMeta.active ?? 0} / ${storesMeta.total ?? 0}` },
        { k: 'Автозапуск', v: auto ? `${auto.running ? 'идёт' : 'ожидание'} · ${autoPhaseRu[auto.phase] || auto.phase || '—'}` : '—' },
        { k: 'Следующий слот (MSK)', v: auto?.next_slot || '—' },
        ...(auto && auto.schedule_hint ? [{ k: 'Авто: внимание', v: auto.schedule_hint }] : []),
      ];
      const wrap = document.getElementById('summary-grid');
      if (wrap) {
        wrap.innerHTML = summary.map(x => `<div class="summary-item"><div class="k">${escapeHtml(x.k)}</div><div class="v">${escapeHtml(x.v)}</div></div>`).join('');
      }
      void loadQualityMetrics(false);
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  let _qualityLoading = false;

  function formatPercentValue(v) {
    const n = Number(v);
    if (Number.isNaN(n)) return '—';
    const abs = Math.abs(n);
    let shown;
    if (abs >= 10) {
      shown = n.toFixed(1).replace(/\.0$/, '');
    } else if (abs >= 1) {
      shown = n.toFixed(2).replace(/\.?0+$/, '');
    } else {
      shown = n.toFixed(2).replace(/\.?0+$/, '');
    }
    return shown + '%';
  }

  function formatQualityMetricValue(m) {
    if (!m || m.value == null || m.value === '') return '—';
    const v = Number(m.value);
    if (Number.isNaN(v)) return '—';
    if (m.unit === 'stars') {
      const stars = v.toFixed(2).replace(/\.00$/, '') + ' ★';
      return m.extra === '≈' ? '≈ ' + stars : stars;
    }
    if (m.unit === 'percent') {
      let s = formatPercentValue(v);
      if (m.key === 'error_index' && m.extra) s += ' ' + m.extra;
      return s;
    }
    return String(v);
  }

  const QUALITY_OZON_COLUMNS = [
    { key: 'cancellation', label: 'Отмены', title: 'Рейтинг · % отмен (риск блокировки ~2%)' },
    { key: 'overdue', label: 'Просрочки', title: 'Рейтинг · % просрочек отгрузки (~5%)' },
    { key: 'error_index', label: 'Индекс', title: 'Индекс ошибок за 14 дней · плата за ошибки' },
  ];

  function qualityMetricByKey(row, key) {
    return (row.metrics || []).find(m => m.key === key) || null;
  }

  function renderQualityCell(m, col) {
    if (!m) return '<td class="quality-td quality-td--empty">—</td>';
    const lvl = m.level || 'na';
    const tip = [m.hint, col.title, m.extra].filter(Boolean).join(' · ');
    const titleAttr = tip ? ` title="${escapeHtml(tip)}"` : '';
    return `<td class="quality-td"${titleAttr}>
      <span class="quality-cell quality-cell--${escapeHtml(lvl)}">${escapeHtml(formatQualityMetricValue(m))}</span>
    </td>`;
  }

  function renderQualityTable(wrapId, rows, columns, emptyLabel, opts = {}) {
    const wrap = document.getElementById(wrapId);
    if (!wrap) return;
    if (!rows || !rows.length) {
      wrap.innerHTML = `<div class="quality-empty">${escapeHtml(emptyLabel)}</div>`;
      return;
    }
    const groupHead = opts.groupHead
      ? `<tr class="quality-table-group-row">${opts.groupHead}</tr>`
      : '';
    const head = columns.map(c => `<th title="${escapeHtml(c.title || '')}">${escapeHtml(c.label)}</th>`).join('');
    const body = rows.map(row => {
      if (row.error && !row.metrics?.length) {
        const errTip = escapeHtml(row.error);
        return `<tr class="quality-tr quality-tr--error">
          <td class="quality-td quality-td--store">${escapeHtml(row.store_name || '')}</td>
          <td class="quality-td quality-td--error-msg" colspan="${columns.length}" title="${errTip}">${errTip}</td>
        </tr>`;
      }
      const cells = columns.map(c => renderQualityCell(qualityMetricByKey(row, c.key), c)).join('');
      const trClass = row.error ? 'quality-tr quality-tr--warn' : 'quality-tr';
      const trTitle = row.error ? ` title="${escapeHtml(row.error)}"` : '';
      return `<tr class="${trClass}"${trTitle}>
        <td class="quality-td quality-td--store">${escapeHtml(row.store_name || '')}</td>
        ${cells}
      </tr>`;
    }).join('');
    wrap.innerHTML = `
      <div class="quality-table-wrap">
        <table class="quality-table">
          <thead>
            ${groupHead}
            <tr><th>Магазин</th>${head}</tr>
          </thead>
          <tbody>${body}</tbody>
        </table>
      </div>`;
  }

  async function loadQualityMetrics(refresh = false) {
    if (_qualityLoading) return;
    _qualityLoading = true;
    const hint = document.getElementById('quality-updated-hint');
    const btn = document.getElementById('btn-refresh-quality');
    if (hint) hint.textContent = 'Загрузка показателей…';
    if (btn) btn.disabled = true;
    try {
      const q = refresh ? '?refresh=1' : '';
      const data = await api('/quality-metrics' + q, { timeoutMs: 120000 });
      renderQualityTable(
        'quality-ozon-stores',
        data.ozon || [],
        QUALITY_OZON_COLUMNS,
        'Нет активных магазинов Ozon',
        {
          groupHead:
            '<th></th>' +
            '<th colspan="2" class="quality-th-group">Рейтинг (блокировка)</th>' +
            '<th class="quality-th-group">Индекс ошибок</th>',
        },
      );
      const ttl = Number(data.cache_ttl_sec) || 1800;
      const ttlMin = Math.round(ttl / 60);
      if (hint) {
        hint.textContent = refresh
          ? `Обновлено только что · кэш ${ttlMin} мин`
          : `Данные с маркетплейсов · кэш до ${ttlMin} мин`;
      }
    } catch (err) {
      if (hint) hint.textContent = 'Не удалось загрузить показатели';
      toast(err.message || 'Ошибка загрузки показателей', 'error');
    } finally {
      _qualityLoading = false;
      if (btn) btn.disabled = false;
    }
  }

  document.getElementById('btn-refresh-quality')?.addEventListener('click', () => {
    void loadQualityMetrics(true);
  });

  // ---- Stores ----
  let stores = [];
  let storesLoadPromise = null;

  async function ensureStoresLoaded(opts = {}) {
    const force = !!opts.force;
    const timeoutMs = Number(opts.timeoutMs) > 0 ? Number(opts.timeoutMs) : 45000;
    if (!force && stores.length) return stores;
    if (force) invalidateStoresCache();
    if (!storesLoadPromise) {
      storesLoadPromise = api('/stores', { timeoutMs })
        .then(list => {
          stores = Array.isArray(list) ? list : [];
          return stores;
        })
        .catch(err => {
          storesLoadPromise = null;
          throw err;
        });
    }
    return storesLoadPromise;
  }

  function retryStoresLoad() {
    storesLoadPromise = null;
  }

  function selectFirstStoreOption(sel) {
    if (!sel || !sel.options || !sel.options.length) return;
    for (let i = 0; i < sel.options.length; i++) {
      if (String(sel.options[i].value || '').trim()) {
        sel.selectedIndex = i;
        return;
      }
    }
  }

  /** Только селект чатов WB — без сброса выбора при каждом обновлении списка. */
  function syncWbChatsStoreSelect(preferredId) {
    const wbSel = document.getElementById('wb-chats-store');
    if (!wbSel) return;
    const wb = storesForMarketplace('wb');
    const prev = preferredId != null
      ? String(preferredId).trim()
      : String(wbSel.value || '').trim();
    const wantIds = wb.map(s => String(s.id));
    const haveIds = Array.from(wbSel.options || [])
      .map(o => String(o.value || '').trim())
      .filter(Boolean);
    const needRebuild = wantIds.length !== haveIds.length
      || wantIds.some(id => !haveIds.includes(id));
    if (!needRebuild && wb.length && prev && wantIds.includes(prev)) {
      if (String(wbSel.value || '').trim() !== prev) {
        wbChatsSuppressSelectChange = true;
        wbSel.value = prev;
        setTimeout(() => { wbChatsSuppressSelectChange = false; }, 0);
      }
      return;
    }
    wbChatsSuppressSelectChange = true;
    wbSel.innerHTML = wb.length
      ? wb.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('')
      : '<option value="">Нет магазинов WB</option>';
    if (wb.length) {
      const ids = new Set(wantIds);
      if (prev && ids.has(prev)) wbSel.value = prev;
      else selectFirstStoreOption(wbSel);
    }
    setTimeout(() => { wbChatsSuppressSelectChange = false; }, 0);
  }

  function setWbChatsStoreSelectValue(storeId) {
    const wbSel = document.getElementById('wb-chats-store');
    if (!wbSel || storeId == null) return;
    const v = String(storeId).trim();
    if (!v || String(wbSel.value || '').trim() === v) return;
    wbChatsSuppressSelectChange = true;
    wbSel.value = v;
    setTimeout(() => { wbChatsSuppressSelectChange = false; }, 0);
  }

  function setPanelLoading(id, visible, message) {
    const wrap = document.getElementById(id);
    if (!wrap) return;
    wrap.hidden = !visible;
    wrap.classList.toggle('visible', !!visible);
    wrap.style.removeProperty('display');
    if (message) {
      const t = wrap.querySelector('.progress-text') || wrap.querySelector('.progress-label');
      if (t) t.textContent = message;
    } else if (visible) {
      const t = wrap.querySelector('.progress-text') || wrap.querySelector('.progress-label');
      if (t) t.textContent = 'Загрузка';
    }
  }

  function setChatStatusBar(barId, kind, message) {
    const el = document.getElementById(barId);
    if (!el) return;
    const msg = (message || '').trim();
    if (!msg) {
      el.hidden = true;
      el.className = 'chat-status-bar';
      el.innerHTML = '';
      return;
    }
    el.hidden = false;
    el.className = 'chat-status-bar' + (kind ? ` is-${kind}` : '');
    if (kind === 'loading') {
      el.innerHTML = `<span class="chat-status-spinner" aria-hidden="true"></span><span class="chat-status-text">${escapeHtml(msg)}</span>`;
    } else {
      el.innerHTML = `<span class="chat-status-text">${escapeHtml(msg)}</span>`;
    }
  }

  function getStoreNameById(storeId) {
    const s = stores.find(x => Number(x.id) === Number(storeId));
    return s ? s.name : '';
  }

  function setChatToolbarBusy(panelPrefix, busy) {
    const ids = [
      `btn-${panelPrefix}-refresh`,
      `btn-${panelPrefix}-mass`,
      `btn-${panelPrefix}-load-thread`,
      `btn-${panelPrefix}-generate`,
      `btn-${panelPrefix}-send`,
      `btn-${panelPrefix}-more-history`,
    ];
    ids.forEach(id => {
      const el = document.getElementById(id);
      if (el) el.disabled = !!busy;
    });
    const sel = document.getElementById(`${panelPrefix}-store`);
    if (sel) sel.disabled = !!busy;
  }

  function marketplaceExtra() {
    const m = document.querySelector('#form-store select[name="marketplace"]').value;
    document.getElementById('wrap-business-id').style.display = m === 'yam' ? 'block' : 'none';
    document.getElementById('wrap-client-id').style.display = m === 'ozon' ? 'block' : 'none';
  }

  document.querySelector('#form-store select[name="marketplace"]').addEventListener('change', marketplaceExtra);

  document.getElementById('form-store').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const body = {
      marketplace: fd.get('marketplace'),
      name: fd.get('name'),
      api_key: fd.get('api_key'),
      active: fd.get('active') === 'on',
    };
    if (body.marketplace === 'yam') body.business_id = parseInt(fd.get('business_id'), 10) || null;
    if (body.marketplace === 'ozon') body.client_id = fd.get('client_id') || null;
    try {
      await api('/stores', { method: 'POST', body: JSON.stringify(body) });
      toast('Магазин добавлен');
      e.target.reset();
      await reloadStoresIntoSelects();
    } catch (err) {
      toast(err.message, 'error');
    }
  });

  function mpPillHtml(marketplace) {
    const mp = String(marketplace || '').toLowerCase();
    const labels = { wb: 'WB', yam: 'ЯМ', ozon: 'Ozon' };
    const cls = mp === 'wb' ? 'mp-wb' : mp === 'yam' ? 'mp-yam' : mp === 'ozon' ? 'mp-ozon' : '';
    const label = labels[mp] || escapeHtml(marketplace);
    return `<span class="mp-pill ${cls}">${label}</span>`;
  }

  function rolePillHtml(role) {
    const r = String(role || '').toLowerCase();
    const cls = r === 'admin' ? 'role-admin' : 'role-user';
    const label = r === 'admin' ? 'Админ' : escapeHtml(role);
    return `<span class="role-pill ${cls}">${label}</span>`;
  }

  function ozonAlertCategoryBadge(row) {
    const cat = String(row.alert_category || '').toLowerCase();
    const amt = String(row.amount || '');
    if (amt && amt !== '—' && /\d/.test(amt)) {
      return '<span class="ozon-alert-cat ozon-cat-fine">💸 Штраф</span>';
    }
    const map = {
      cert_request: ['ozon-cat-cert', '📄 Документы'],
      product_hidden: ['ozon-cat-hidden', '🚫 Снято'],
      threat: ['ozon-cat-threat', '⚠️ Угроза'],
    };
    const [cls, label] = map[cat] || ['ozon-cat-other', 'ℹ️ Важное'];
    return `<span class="ozon-alert-cat ${cls}">${label}</span>`;
  }

  function renderStores() {
    const wrap = document.getElementById('stores-list');
    if (!stores.length) {
      wrap.innerHTML = '<p class="empty-state">Нет магазинов. Добавьте первый выше.</p>';
      return;
    }
    wrap.innerHTML = stores.map(s => `
        <div class="store-card${s.active ? '' : ' inactive'}" data-store-id="${s.id}">
          <div class="store-card-head">
            <h3>${escapeHtml(s.name)} ${mpPillHtml(s.marketplace)}</h3>
          </div>
          <div class="meta">ID ${s.id}${s.api_key_set ? ' · ключ задан' : ''}${s.active ? '' : ' · неактивен'}</div>
          <div class="actions">
            <button type="button" class="btn btn-secondary btn-sm btn-edit-store" data-id="${s.id}">Изменить</button>
            <button type="button" class="btn btn-danger btn-sm btn-delete-store" data-id="${s.id}">Удалить</button>
          </div>
        </div>`).join('');

    wrap.querySelectorAll('.btn-edit-store').forEach(btn => {
      btn.addEventListener('click', () => openEditStore(Number(btn.getAttribute('data-id'))));
    });
    wrap.querySelectorAll('.btn-delete-store').forEach(btn => {
      btn.addEventListener('click', () => deleteStore(Number(btn.getAttribute('data-id'))));
    });
  }

  function openEditStore(storeId) {
    const s = stores.find(x => x.id === storeId);
    if (!s) return;
    const form = document.getElementById('form-store-edit');
    form.querySelector('[name="store_id"]').value = s.id;
    form.querySelector('[name="name"]').value = s.name;
    form.querySelector('[name="api_key"]').value = '';
    form.querySelector('[name="active"]').checked = s.active;
    const wrapB = document.getElementById('wrap-edit-business-id');
    const wrapC = document.getElementById('wrap-edit-client-id');
    wrapB.style.display = s.marketplace === 'yam' ? 'block' : 'none';
    wrapC.style.display = s.marketplace === 'ozon' ? 'block' : 'none';
    const bid = form.querySelector('[name="business_id"]');
    const cid = form.querySelector('[name="client_id"]');
    if (bid) bid.value = s.business_id ?? '';
    if (cid) cid.value = s.client_id ?? '';
    document.getElementById('modal-store').classList.add('visible');
  }

  document.getElementById('form-store-edit').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const storeId = Number(fd.get('store_id'));
    const s = stores.find(x => x.id === storeId);
    const body = {
      name: fd.get('name'),
      active: fd.get('active') === 'on',
    };
    const key = fd.get('api_key');
    if (key) body.api_key = key;
    if (s && s.marketplace === 'yam') body.business_id = parseInt(fd.get('business_id'), 10) || null;
    if (s && s.marketplace === 'ozon') body.client_id = fd.get('client_id') || null;
    try {
      await api(`/stores/${storeId}`, { method: 'PATCH', body: JSON.stringify(body) });
      toast('Магазин обновлён');
      document.getElementById('modal-store').classList.remove('visible');
      await reloadStoresIntoSelects();
    } catch (err) {
      toast(err.message, 'error');
    }
  });

  document.getElementById('btn-modal-cancel').addEventListener('click', () => {
    document.getElementById('modal-store').classList.remove('visible');
  });

  const modalItemDetail = document.getElementById('modal-item-detail');
  if (modalItemDetail) {
    document.getElementById('btn-modal-item-close').addEventListener('click', () => modalItemDetail.classList.remove('visible'));
    modalItemDetail.addEventListener('click', (e) => { if (e.target === modalItemDetail) modalItemDetail.classList.remove('visible'); });
    const btnSaveAnswer = document.getElementById('btn-modal-item-save');
    if (btnSaveAnswer) {
      btnSaveAnswer.addEventListener('click', async () => {
        if (!_modalItemCtx) return;
        const text = (document.getElementById('modal-item-answer-input')?.value || '').trim();
        if (!text) {
          toast('Введите текст ответа', 'error');
          return;
        }
        setButtonBusy(btnSaveAnswer, true, 'Сохранение…');
        try {
          const updated = await api(`/items/${_modalItemCtx.id}/answer`, {
            method: 'PATCH',
            body: JSON.stringify({ generated_text: text }),
          });
          toast('Ответ сохранён');
          const list = _modalItemCtx.prefix === 'reviews' ? reviews : questions;
          const idx = list.findIndex((i) => i.id === _modalItemCtx.id);
          if (idx >= 0) {
            list[idx] = { ...list[idx], generated_text: updated.generated_text, status: updated.status };
          }
          renderItems(_modalItemCtx.prefix, list, _modalItemCtx.prefix === 'reviews');
          const errEl = document.getElementById('modal-item-send-error');
          if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
        } catch (err) {
          toast(err.message, 'error');
        } finally {
          setButtonBusy(btnSaveAnswer, false);
        }
      });
    }
  }

  const modalSendConfirm = document.getElementById('modal-send-confirm');
  let _sendConfirmCallback = null;
  if (modalSendConfirm) {
    document.getElementById('btn-send-confirm-cancel')?.addEventListener('click', () => {
      modalSendConfirm.classList.remove('visible');
      _sendConfirmCallback = null;
    });
    modalSendConfirm.addEventListener('click', (e) => {
      if (e.target === modalSendConfirm) {
        modalSendConfirm.classList.remove('visible');
        _sendConfirmCallback = null;
      }
    });
    document.getElementById('btn-send-confirm-ok')?.addEventListener('click', async () => {
      const cb = _sendConfirmCallback;
      modalSendConfirm.classList.remove('visible');
      _sendConfirmCallback = null;
      if (cb) await cb();
    });
  }

  function openSendConfirmModal(summaryHtml, onConfirm) {
    const body = document.getElementById('modal-send-confirm-body');
    if (body) body.innerHTML = summaryHtml;
    _sendConfirmCallback = onConfirm;
    modalSendConfirm?.classList.add('visible');
  }

  async function deleteStore(storeId) {
    if (!confirmDanger('Удалить магазин и все его отзывы/вопросы?')) return;
    try {
      await api(`/stores/${storeId}`, { method: 'DELETE' });
      toast('Магазин удалён');
      await reloadStoresIntoSelects();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function invalidateStoresCache() {
    stores = [];
    storesLoadPromise = null;
  }

  async function loadStores() {
    try {
      await ensureStoresLoaded();
      renderStores();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function storesForMarketplace(mp) {
    const want = String(mp || '').toLowerCase();
    return stores.filter(s => String(s.marketplace || '').toLowerCase() === want);
  }

  async function reloadStoresIntoSelects() {
    invalidateStoresCache();
    try {
      await ensureStoresLoaded({ force: true });
      fillStoreSelects();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function fillStoreSelects() {
    const opts = '<option value="">Все магазины</option>' + stores.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
    const rev = document.getElementById('reviews-store');
    const qu = document.getElementById('questions-store');
    if (rev) rev.innerHTML = opts;
    if (qu) qu.innerHTML = opts;
    syncWbChatsStoreSelect();
    const ozSel = document.getElementById('ozon-chats-store');
    if (ozSel) {
      const oz = storesForMarketplace('ozon');
      const prevOz = String(ozSel.value || '').trim();
      ozonChatsSuppressSelectChange = true;
      ozSel.innerHTML = oz.length
        ? oz.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('')
        : '<option value="">Нет магазинов Ozon — добавьте во вкладке «Магазины»</option>';
      if (oz.length) {
        const ids = new Set(oz.map(s => String(s.id)));
        if (prevOz && ids.has(prevOz)) ozSel.value = prevOz;
        else selectFirstStoreOption(ozSel);
      }
      setTimeout(() => { ozonChatsSuppressSelectChange = false; }, 0);
    }
    const logStoreSel = document.getElementById('log-store');
    if (logStoreSel) {
      const prevLog = String(logStoreSel.value || '').trim();
      logStoreSel.innerHTML = '<option value="">Все магазины</option>'
        + stores.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
      if (prevLog && stores.some(s => String(s.id) === prevLog)) logStoreSel.value = prevLog;
    }
    const cardErrStoreSel = document.getElementById('card-errors-store');
    if (cardErrStoreSel) {
      const prevCe = String(cardErrStoreSel.value || '').trim();
      cardErrStoreSel.innerHTML = '<option value="">Все магазины</option>'
        + stores.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
      if (prevCe && stores.some(s => String(s.id) === prevCe)) cardErrStoreSel.value = prevCe;
    }
    fillOzonAlertsStoreSelects();
    const ozActSel = document.getElementById('ozon-actions-store');
    if (ozActSel) {
      const oz = storesForMarketplace('ozon');
      const prevAct = String(ozActSel.value || '').trim();
      ozonActionsSuppressSelectChange = true;
      ozActSel.innerHTML = oz.length
        ? oz.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('')
        : '<option value="">Нет магазинов Ozon — добавьте во вкладке «Магазины»</option>';
      if (oz.length) {
        const ids = new Set(oz.map(s => String(s.id)));
        if (prevAct && ids.has(prevAct)) ozActSel.value = prevAct;
        else selectFirstStoreOption(ozActSel);
      }
      setTimeout(() => { ozonActionsSuppressSelectChange = false; }, 0);
    }
    syncCardLinksStoreSelect();
  }

  function renderAutoStoreList(selectedIds) {
    const wrap = document.getElementById('auto-store-list');
    if (!wrap) return;
    const sel = new Set((selectedIds || []).map(x => Number(x)));
    if (!stores.length) {
      wrap.innerHTML = '<div class="form-hint">Нет магазинов</div>';
      return;
    }
    wrap.innerHTML = stores.map(s => `
      <label class="auto-store-item">
        <input type="checkbox" value="${s.id}" ${sel.has(Number(s.id)) ? 'checked' : ''}>
        <span class="auto-store-label">${escapeHtml(s.name)} ${mpPillHtml(s.marketplace)}</span>
      </label>
    `).join('');
  }

  function getAutoSelectedStoreIds() {
    const wrap = document.getElementById('auto-store-list');
    if (!wrap) return [];
    return Array.from(wrap.querySelectorAll('input[type="checkbox"]:checked')).map(x => Number(x.value));
  }

  let _autoStatusTimer = null;
  async function refreshAutoStatus() {
    const el = document.getElementById('auto-status-text');
    if (!el) return;
    try {
      const s = await api('/auto-schedule/status');
      const phaseMap = {
        idle: 'ожидание',
        load_new: 'загрузка новых',
        generate: 'генерация',
        send: 'отправка',
        wb_chats: 'чаты WB',
        ozon_chats: 'чаты Ozon',
        ozon_alerts: 'уведомления Ozon',
        ozon_actions: 'акции Ozon',
        idle_items: 'без отзывов/вопросов',
        done: 'завершено',
        cancelled: 'остановлено',
        error: 'ошибка',
      };
      const phase = phaseMap[s.phase] || (s.phase || '—');
      const run = s.running ? 'Выполняется' : 'Не выполняется';
      const slot = s.slot ? `слот ${s.slot}` : '—';
      const storeProg = (s.running && s.store_count)
        ? ` · магазин ${s.store_index || '?'}/${s.store_count}${s.current_store_id ? ` (id ${s.current_store_id})` : ''}`
        : '';
      const next = s.next_slot ? `следующий ${s.next_slot}` : 'нет слотов';
      const err = s.last_error ? ` · ошибка: ${s.last_error}` : '';
      const hint = s.schedule_hint ? `\n${s.schedule_hint}` : '';
      el.textContent = `${run} · этап: ${phase}${storeProg} · ${slot} · ${next}${err}${hint}`;
      const stopBtn = document.getElementById('btn-stop-auto');
      if (stopBtn) stopBtn.disabled = !s.running;
      const runBtn = document.getElementById('btn-run-auto-now');
      if (runBtn) runBtn.disabled = !!s.running;
    } catch (e) {
      el.textContent = 'Не удалось получить статус автозапуска';
    }
  }

  function ensureAutoStatusPolling() {
    if (_autoStatusTimer) return;
    _autoStatusTimer = setInterval(() => {
      const panel = document.getElementById('panel-auto');
      if (panel && panel.classList.contains('active')) {
        refreshAutoStatus();
      }
    }, 3000);
  }

  // ---- WB buyer chats ----
  let wbChatsRaw = [];
  /** id магазина, для которого wbChatsRaw актуален; иначе список нельзя показывать */
  let wbChatsListStoreId = null;
  /** не реагировать на change во время fillStoreSelects (программный wbSel.value) */
  let wbChatsSuppressSelectChange = false;
  let wbChatSelectedId = null;
  let wbChatReplySign = '';
  let wbChatClientMessageKey = '';
  let wbChatThreadPages = 10;
  /** кэш переписки: ключ «storeId:chatId» */
  const wbChatThreadCache = new Map();
  /** защита от гонок: быстрая смена вкладки / чата / магазина */
  let wbChatsPanelGen = 0;
  let wbChatsListFetchGen = 0;
  let wbChatThreadFetchGen = 0;

  function getWbChatsStoreId() {
    const el = document.getElementById('wb-chats-store');
    const v = el && el.value ? String(el.value).trim() : '';
    return v ? Number(v) : null;
  }

  function wbGuessProductTitleFromRow(row) {
    if (!row || !row.goodCard) return '—';
    const g = row.goodCard;
    const keys = ['productName', 'name', 'title', 'subject'];
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i];
      if (g[k] && String(g[k]).trim()) return String(g[k]).trim();
    }
    const lm = row.lastMessage && row.lastMessage.text ? String(row.lastMessage.text) : '';
    const m = lm.match(/по\s+товару\s*"([^"]+)"/i);
    if (m) return m[1].trim();
    if (g.nmID != null && String(g.nmID).trim() !== '') return 'Товар nmID ' + g.nmID;
    return '—';
  }

  function wbChatThreadCacheKey(chatId, sid) {
    const storeId = sid != null ? sid : getWbChatsStoreId();
    const cid = String(chatId || '').trim();
    if (!storeId || !cid) return '';
    return `${storeId}:${cid}`;
  }

  function wbChatPreviewHintHtml() {
    return '';
  }

  function applyWbChatThreadData(t) {
    wbChatReplySign = (t.reply_sign || '').trim();
    wbChatClientMessageKey = (t.client_message_key || '').trim();
    const statusHint = document.getElementById('wb-chats-status-hint');
    if (statusHint) {
      if (t.already_replied) {
        statusHint.textContent = 'На последнее сообщение покупателя уже отвечали.';
        statusHint.style.color = '#b45309';
      } else if (t.skip_reason === 'before_cutoff') {
        statusHint.textContent = 'Сообщение раньше даты «отвечать с» из настроек.';
        statusHint.style.color = '#b45309';
      } else if (t.skip_reason === 'last_not_client') {
        const lastRole = (t.lines && t.lines.length) ? t.lines[t.lines.length - 1].role : '';
        statusHint.textContent = lastRole === 'seller'
          ? 'Последнее сообщение — ваш ответ. Ждём новое сообщение от покупателя.'
          : 'Последнее сообщение не от покупателя — автоответ не нужен.';
        statusHint.style.color = '';
      } else {
        statusHint.textContent = '';
      }
    }
    const titleEl = document.getElementById('wb-chats-product-title');
    if (titleEl) titleEl.textContent = t.product_title || '—';
    const threadEl = document.getElementById('wb-chats-thread');
    const lines = t.lines || [];
    if (threadEl) {
      threadEl.innerHTML = lines.length
        ? lines.map(l => {
          const lab = l.role === 'client' ? 'Покупатель' : l.role === 'seller' ? 'Вы' : (l.role === 'other' ? 'Неизвестно' : escapeHtml(l.role || ''));
          return `<div class="wb-chat-line"><span class="wb-chat-role">${lab}</span><span class="wb-chat-text">${escapeHtml(l.text)}</span></div>`;
        }).join('')
        + (t.has_more_history ? '<div class="form-hint" style="margin-top:10px;">Показана часть переписки. Нажмите «Больше истории» для более старых сообщений.</div>' : '')
        : '<div class="form-hint">Нет текста сообщений в выборке событий.</div>';
    }
    const hint = document.getElementById('wb-chats-hint');
    const body = document.getElementById('wb-chats-detail-body');
    if (hint) hint.style.display = 'none';
    if (body) body.style.display = '';
  }

  function restoreWbChatDetailForSelected() {
    if (!wbChatSelectedId) return;
    const sid = getWbChatsStoreId();
    const row = wbChatsRaw.find(c => String(c.chatID || '') === String(wbChatSelectedId));
    if (!row) return;
    wbChatShowDetailShell(row);
    const cached = wbChatThreadCache.get(wbChatThreadCacheKey(wbChatSelectedId, sid));
    if (cached) {
      applyWbChatThreadData(cached);
      setChatStatusBar('wb-chats-status-bar', 'ok', 'Переписка из кэша. «Обновить переписку» — заново с WB.');
    } else if (wbChatSelectedId) {
      void loadWbChatThread(wbChatSelectedId);
    }
  }

  function wbChatShowDetailShell(row) {
    const hint = document.getElementById('wb-chats-hint');
    const body = document.getElementById('wb-chats-detail-body');
    if (hint) hint.style.display = 'none';
    if (body) body.style.display = '';
    wbChatReplySign = (row && row.replySign) ? String(row.replySign).trim() : '';
    wbChatClientMessageKey = '';
    const titleEl = document.getElementById('wb-chats-product-title');
    if (titleEl) titleEl.textContent = wbGuessProductTitleFromRow(row);
    const threadEl = document.getElementById('wb-chats-thread');
    const lm = row && row.lastMessage && row.lastMessage.text ? String(row.lastMessage.text) : '';
    if (threadEl) {
      threadEl.innerHTML = lm
        ? `<div class="wb-chat-line"><span class="wb-chat-role">Список WB</span><span class="wb-chat-text">${escapeHtml(lm)}</span></div>${wbChatPreviewHintHtml()}`
        : `<div class="form-hint">Нет текста в списке WB.</div>${wbChatPreviewHintHtml()}`;
    }
    const ta = document.getElementById('wb-chats-draft');
    if (ta) ta.value = '';
  }

  function wbChatsSortKey(c) {
    const lm = c && c.lastMessage;
    if (lm && typeof lm === 'object') {
      const t = lm.addTimestamp ?? lm.addTime ?? lm.timestamp ?? lm.time;
      const n = Number(t);
      if (Number.isFinite(n) && n > 0) return n;
    }
    const u = c.chatUpdatedAt ?? c.updatedAt;
    const n2 = Number(u);
    return Number.isFinite(n2) && n2 > 0 ? n2 : 0;
  }

  function renderWbChatsList() {
    const wrap = document.getElementById('wb-chats-list');
    if (!wrap) return;
    const uiSid = getWbChatsStoreId();
    if (
      uiSid != null
      && wbChatsListStoreId != null
      && Number(uiSid) !== Number(wbChatsListStoreId)
    ) {
      wrap.innerHTML = '<div class="form-hint">Загружаю чаты выбранного магазина…</div>';
      void refreshWbChatsList(true, uiSid);
      return;
    }
    if (!wbChatsRaw.length) {
      wrap.innerHTML = '<div class="form-hint">Нет чатов. Нажмите «Обновить список чатов».</div>';
      return;
    }
    const rows = [...wbChatsRaw].sort((a, b) => wbChatsSortKey(b) - wbChatsSortKey(a));
    wrap.innerHTML = rows.map(c => {
      const id = String(c.chatID || '');
      const enc = encodeURIComponent(id);
      const name = escapeHtml(c.clientName || 'Покупатель');
      let raw = c.lastMessage && c.lastMessage.text ? String(c.lastMessage.text) : '';
      if (!raw && c.lastMessage && c.lastMessage.attachments) {
        const att = c.lastMessage.attachments;
        const imgs = att && att.images;
        if (Array.isArray(imgs) && imgs.length) raw = imgs.length === 1 ? '[Фото]' : `[Фото: ${imgs.length}]`;
      }
      const lm = escapeHtml(raw.slice(0, 140)) || '—';
      const active = wbChatSelectedId === id ? 'wb-chat-item active' : 'wb-chat-item';
      return `<button type="button" class="${active}" data-chat-id="${enc}"><div class="wb-chat-item-name">${name}</div><div class="wb-chat-item-preview">${lm}</div></button>`;
    }).join('');
    wrap.querySelectorAll('.wb-chat-item').forEach(btn => {
      btn.addEventListener('click', () => {
        const chatId = decodeURIComponent(btn.getAttribute('data-chat-id') || '');
        wbChatSelectedId = chatId;
        wbChatThreadPages = 10;
        renderWbChatsList();
        restoreWbChatDetailForSelected();
      });
    });
  }

  async function refreshWbChatsList(forceRefresh = true, storeIdOverride = null) {
    const requestSid = storeIdOverride != null ? Number(storeIdOverride) : getWbChatsStoreId();
    try {
      await ensureStoresLoaded();
      syncWbChatsStoreSelect(requestSid || undefined);
    } catch (err) {
      if (!requestSid && !getWbChatsStoreId()) {
        setChatStatusBar('wb-chats-status-bar', 'error', err.message || 'Не удалось загрузить магазины');
        setPanelLoading('wb-chats-loading', false);
        return;
      }
    }
    const sid = requestSid || getWbChatsStoreId();
    if (!sid) {
      const wrap = document.getElementById('wb-chats-list');
      if (wrap) {
        wrap.innerHTML = '<div class="form-hint">Нет магазина WB. Добавьте магазин Wildberries во вкладке «Магазины» и укажите ключ с правом «Чат с покупателями».</div>';
      }
      setChatStatusBar('wb-chats-status-bar', 'error', 'Выберите или добавьте магазин WB в «Магазины».');
      setPanelLoading('wb-chats-loading', false);
      return;
    }
    const gen = ++wbChatsListFetchGen;
    const q = forceRefresh ? '?refresh=1' : '';
    const storeLabel = getStoreNameById(sid) || `ID ${sid}`;
    const prevSelected = wbChatSelectedId;
    const prevStore = wbChatsListStoreId;
    const wrap = document.getElementById('wb-chats-list');
    if (wrap) wrap.innerHTML = '<div class="form-hint">Загрузка списка…</div>';
    const loadMsg = `Загружаю чаты WB «${storeLabel}»… Первый запрос может занять 30–90 секунд (лимиты WB).`;
    setChatStatusBar('wb-chats-status-bar', 'loading', loadMsg);
    setPanelLoading('wb-chats-loading', true, loadMsg);
    setChatToolbarBusy('wb-chats', true);
    const safetyClear = setTimeout(() => {
      if (gen !== wbChatsListFetchGen) return;
      setPanelLoading('wb-chats-loading', false);
      setChatToolbarBusy('wb-chats', false);
    }, 95000);
    try {
      const data = await api(`/wb/buyer-chats/${sid}${q}`, { timeoutMs: 90000 });
      if (gen !== wbChatsListFetchGen) return;
      if (Number(data.store_id) !== Number(sid)) {
        throw new Error('Ответ сервера не совпадает с выбранным магазином — обновите страницу.');
      }
      setWbChatsStoreSelectValue(sid);
      wbChatsRaw = data.chats || [];
      wbChatsListStoreId = sid;
      const keepSelection = !forceRefresh
        && Number(prevStore) === Number(sid)
        && prevSelected
        && wbChatsRaw.some(c => String(c.chatID || '') === String(prevSelected));
      if (keepSelection) {
        wbChatSelectedId = prevSelected;
      } else {
        wbChatSelectedId = null;
        wbChatReplySign = '';
      }
      renderWbChatsList();
      if (wbChatSelectedId) restoreWbChatDetailForSelected();
      const n = wbChatsRaw.length;
      const storeLabelDone = data.store_name || getStoreNameById(sid) || `ID ${sid}`;
      const keyHint = data.same_api_key_warning ? String(data.same_api_key_warning) : '';
      const staleHint = data.stale && data.warning ? String(data.warning) : '';
      const statusTail = keyHint || staleHint;
      if (!wbChatSelectedId) {
        setChatStatusBar(
          'wb-chats-status-bar',
          statusTail ? 'info' : (n ? 'ok' : 'info'),
          statusTail || (n
            ? `«${storeLabelDone}»: загружено чатов ${n}. Выберите чат слева.`
            : `«${storeLabelDone}»: чатов пока нет (или пустой ответ WB).`),
        );
        const hint = document.getElementById('wb-chats-hint');
        const body = document.getElementById('wb-chats-detail-body');
        if (hint) {
          hint.style.display = '';
          hint.textContent = 'Выберите чат слева.';
        }
        if (body) body.style.display = 'none';
      } else if (n) {
        setChatStatusBar(
          'wb-chats-status-bar',
          statusTail ? 'info' : 'ok',
          statusTail || `«${storeLabelDone}»: загружено чатов ${n}.`,
        );
      }
    } catch (err) {
      if (gen !== wbChatsListFetchGen) return;
      wbChatsRaw = [];
      wbChatsListStoreId = null;
      wbChatSelectedId = null;
      wbChatReplySign = '';
      const wrapErr = document.getElementById('wb-chats-list');
      if (wrapErr) {
        wrapErr.innerHTML = `<div class="form-hint" style="color:#b91c1c;">${escapeHtml(err.message || 'Ошибка загрузки')}</div>`;
      }
      setChatStatusBar('wb-chats-status-bar', 'error', err.message || 'Ошибка загрузки чатов WB');
      toast(err.message, 'error');
    } finally {
      clearTimeout(safetyClear);
      if (gen === wbChatsListFetchGen) {
        setPanelLoading('wb-chats-loading', false);
        setChatToolbarBusy('wb-chats', false);
      }
    }
  }

  async function loadWbChatThread(chatId) {
    const sid = getWbChatsStoreId();
    if (!sid || !chatId) return;
    const gen = ++wbChatThreadFetchGen;
    const pages = Math.max(1, Math.min(50, wbChatThreadPages || 10));
    const loadHint = pages > 15
      ? `Загружаю глубокую историю (${pages} стр. ленты WB)… до 5 минут`
      : 'Загружаю переписку… до 2 минут при холодном сервере';
    setChatStatusBar('wb-chats-status-bar', 'loading', loadHint);
    setPanelLoading('wb-chats-loading', true, loadHint);
    setChatToolbarBusy('wb-chats', true);
    try {
      const path = `/wb/buyer-chats/${sid}/${encodeURIComponent(chatId)}/thread?pages=${pages}`;
      const t = await api(path, { timeoutMs: 300000 });
      if (gen !== wbChatThreadFetchGen) return;
      if (Number(getWbChatsStoreId()) !== Number(sid)) return;
      if (String(wbChatSelectedId || '') !== String(chatId)) return;
      const cacheKey = wbChatThreadCacheKey(chatId, sid);
      if (cacheKey) wbChatThreadCache.set(cacheKey, t);
      applyWbChatThreadData(t);
      setChatStatusBar('wb-chats-status-bar', 'ok', 'Переписка загружена.');
      const ta = document.getElementById('wb-chats-draft');
      if (ta) ta.value = '';
    } catch (err) {
      if (gen !== wbChatThreadFetchGen) return;
      if (String(wbChatSelectedId || '') !== String(chatId)) return;
      toast(err.message, 'error');
      setChatStatusBar('wb-chats-status-bar', 'error', err.message || 'Ошибка загрузки переписки');
      const threadEl = document.getElementById('wb-chats-thread');
      if (threadEl) {
        const extra = 'Можно нажать «Сгенерировать» — контекст подтянется из списка чатов. Reply-sign уже из списка WB.';
        threadEl.innerHTML = `<div class="form-hint" style="color:#b91c1c;">${escapeHtml(err.message)}</div><div class="form-hint" style="margin-top:8px;">${escapeHtml(extra)}</div>`;
      }
      const body = document.getElementById('wb-chats-detail-body');
      if (body) body.style.display = '';
      const hint = document.getElementById('wb-chats-hint');
      if (hint) hint.style.display = 'none';
    } finally {
      if (gen === wbChatThreadFetchGen) {
        setPanelLoading('wb-chats-loading', false);
        setChatToolbarBusy('wb-chats', false);
      }
    }
  }

  async function loadWbChatsPanel() {
    const gen = ++wbChatsPanelGen;
    if (!stores.length) {
      setChatStatusBar('wb-chats-status-bar', 'loading', 'Подготавливаю список магазинов…');
    }
    try {
      await ensureStoresLoaded();
    } catch (e) {
      if (gen !== wbChatsPanelGen) return;
      const sidFallback = getWbChatsStoreId();
      if (sidFallback) {
        await refreshWbChatsList(false);
        return;
      }
      setChatStatusBar('wb-chats-status-bar', 'error', (e && e.message) ? e.message : 'Не удалось загрузить магазины');
      return;
    }
    if (gen !== wbChatsPanelGen) return;
    fillStoreSelects();
    if (gen !== wbChatsPanelGen) return;
    const sid = getWbChatsStoreId();
    const listMatches = sid != null && wbChatsListStoreId != null && Number(sid) === Number(wbChatsListStoreId);
    if (listMatches && wbChatsRaw.length) {
      renderWbChatsList();
      if (wbChatSelectedId) {
        restoreWbChatDetailForSelected();
      } else {
        setChatStatusBar('wb-chats-status-bar', 'ok', `Загружено чатов: ${wbChatsRaw.length}. Выберите чат слева.`);
      }
      return;
    }
    if (sid) {
      await refreshWbChatsList(false);
      return;
    }
    wbChatsRaw = [];
    wbChatsListStoreId = null;
    wbChatSelectedId = null;
    wbChatReplySign = '';
    renderWbChatsList();
    setChatStatusBar('wb-chats-status-bar', 'error', 'Нет магазинов WB — добавьте во вкладке «Магазины».');
  }

  async function wbChatsGenerateDraft() {
    const sid = getWbChatsStoreId();
    if (!sid || !wbChatSelectedId) {
      toast('Выберите чат слева', 'error');
      return;
    }
    try {
      const r = await api(`/wb/buyer-chats/${sid}/generate-draft`, {
        method: 'POST',
        body: JSON.stringify({ chat_id: wbChatSelectedId }),
      });
      const ta = document.getElementById('wb-chats-draft');
      if (ta) ta.value = (r.draft || '').trim();
      const titleEl = document.getElementById('wb-chats-product-title');
      if (titleEl && r.product_title) titleEl.textContent = r.product_title;
      toast('Черновик сгенерирован');
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  async function wbChatsMassGenerateSend() {
    const sid = getWbChatsStoreId();
    if (!sid) {
      toast('Выберите магазин WB', 'error');
      return;
    }
    const msg =
      'Обработаются только чаты, где последнее в переписке — сообщение покупателя.\n' +
      'Для каждого: ответ сгенерирует ИИ и сразу отправится в Wildberries (не больше 50 чатов за один запуск, пауза ~1 с между чатами).\n' +
      'Продолжить?';
    if (!confirm(msg)) return;
    const btn = document.getElementById('btn-wb-chats-mass');
    if (btn) btn.disabled = true;
    try {
      const r = await api(`/wb/buyer-chats/${sid}/mass-generate-send`, {
        method: 'POST',
        body: JSON.stringify({ max_chats: 50, event_pages: 6 }),
      });
      const sent = r.wb_chat_sent ?? 0;
      const genF = r.wb_chat_gen_failed ?? 0;
      const sendF = r.wb_chat_send_failed ?? 0;
      const skip = r.wb_chat_skipped_no_reply_sign ?? 0;
      const dup = r.wb_chat_skipped_already_replied ?? 0;
      const cutoff = r.wb_chat_skipped_before_cutoff ?? 0;
      const tooOld = r.wb_chat_skipped_too_old ?? 0;
      const ncl = r.wb_chat_skipped_last_not_client ?? 0;
      const elig = r.wb_chat_eligible ?? 0;
      const cand = r.wb_chat_candidates ?? 0;
      toast(
        `Отправлено: ${sent}. В партии: ${cand} из ${elig}. Уже отвечено: ${dup}, раньше даты: ${cutoff}, `
        + `старше лимита: ${tooOld}, не от покупателя: ${ncl}. Ошибки ИИ: ${genF}, отправки: ${sendF}, без reply_sign: ${skip}.`,
      );
      await refreshWbChatsList(true);
    } catch (err) {
      toast(err.message, 'error');
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function wbChatsSendMessage() {
    const sid = getWbChatsStoreId();
    if (!sid) {
      toast('Выберите магазин WB', 'error');
      return;
    }
    const ta = document.getElementById('wb-chats-draft');
    const msg = (ta && ta.value ? ta.value : '').trim();
    if (!msg) {
      toast('Введите текст ответа', 'error');
      return;
    }
    if (!wbChatReplySign) {
      toast('Нет reply_sign — нажмите «Обновить переписку»', 'error');
      return;
    }
    try {
      await api(`/wb/buyer-chats/${sid}/send`, {
        method: 'POST',
        body: JSON.stringify({
          reply_sign: wbChatReplySign,
          message: msg,
          chat_id: wbChatSelectedId || '',
          client_message_key: wbChatClientMessageKey || '',
        }),
      });
      toast('Сообщение отправлено');
      if (wbChatSelectedId) await loadWbChatThread(wbChatSelectedId);
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function wireWbChatsPanel() {
    if (wireWbChatsPanel._done) return;
    wireWbChatsPanel._done = true;
    const sel = document.getElementById('wb-chats-store');
    if (sel) {
      const onWbStorePick = () => {
        if (wbChatsSuppressSelectChange) return;
        const newSid = getWbChatsStoreId();
        if (!newSid) return;
        if (wbChatsListStoreId != null && Number(newSid) === Number(wbChatsListStoreId)) return;
        wbChatsRaw = [];
        wbChatsListStoreId = null;
        wbChatSelectedId = null;
        wbChatReplySign = '';
        wbChatThreadCache.clear();
        const hint = document.getElementById('wb-chats-hint');
        const body = document.getElementById('wb-chats-detail-body');
        if (hint) {
          hint.style.display = '';
          hint.textContent = 'Выберите чат слева.';
        }
        if (body) body.style.display = 'none';
        void refreshWbChatsList(true, newSid);
      };
      sel.addEventListener('change', onWbStorePick);
      sel.addEventListener('input', onWbStorePick);
    }
    const b1 = document.getElementById('btn-wb-chats-refresh');
    if (b1) {
      b1.addEventListener('click', () => {
        retryStoresLoad();
        setChatStatusBar('wb-chats-status-bar', 'loading', 'Запрос к WB… подождите, первый ответ может занять до 1–2 минут.');
        setPanelLoading('wb-chats-loading', true, 'Запрос к WB…');
        void refreshWbChatsList(true);
      });
    }
    const bMass = document.getElementById('btn-wb-chats-mass');
    if (bMass) bMass.addEventListener('click', () => { void wbChatsMassGenerateSend(); });
    const b2 = document.getElementById('btn-wb-chats-load-thread');
    if (b2) b2.addEventListener('click', () => {
      if (!wbChatSelectedId) {
        toast('Выберите чат', 'error');
        return;
      }
      wbChatThreadPages = 10;
      void loadWbChatThread(wbChatSelectedId);
    });
    const bMore = document.getElementById('btn-wb-chats-more-history');
    if (bMore) bMore.addEventListener('click', () => {
      if (!wbChatSelectedId) {
        toast('Выберите чат', 'error');
        return;
      }
      wbChatThreadPages = Math.min(50, (wbChatThreadPages || 10) + 12);
      void loadWbChatThread(wbChatSelectedId);
    });
    const b3 = document.getElementById('btn-wb-chats-generate');
    if (b3) b3.addEventListener('click', () => { void wbChatsGenerateDraft(); });
    const b4 = document.getElementById('btn-wb-chats-send');
    if (b4) b4.addEventListener('click', () => { void wbChatsSendMessage(); });
  }

  // ---- Ozon buyer chats ----
  let ozonChatsRaw = [];
  let ozonChatsListStoreId = null;
  let ozonChatsListFilter = null;
  let ozonChatsSuppressSelectChange = false;
  let ozonChatSelectedId = null;
  let ozonChatClientMessageKey = '';
  let ozonChatsPanelGen = 0;
  let ozonChatsListFetchGen = 0;
  let ozonChatThreadFetchGen = 0;

  function getOzonChatsStoreId() {
    const el = document.getElementById('ozon-chats-store');
    const v = el && el.value ? String(el.value).trim() : '';
    return v ? Number(v) : null;
  }

  function ozonChatRoleLabel(role) {
    if (role === 'client') return 'Покупатель';
    if (role === 'seller') return 'Вы';
    if (role === 'support') return 'Поддержка Ozon';
    if (role === 'crm') return 'Система Ozon';
    if (role === 'courier') return 'Курьер';
    return escapeHtml(role || '');
  }

  function ozonChatCategoryLabel(cat) {
    if (cat === 'buyer') return 'Покупатель';
    if (cat === 'support') return 'Поддержка';
    return 'Другое';
  }

  function syncOzonChatsFilterUI() {
    document.querySelectorAll('#ozon-chats-filter [data-filter]').forEach(btn => {
      btn.classList.toggle('active', btn.getAttribute('data-filter') === ozonChatsFilter);
    });
    const buyersOnly = ozonChatsFilter === 'buyers';
    const massBtn = document.getElementById('btn-ozon-chats-mass');
    if (massBtn) massBtn.style.display = buyersOnly ? '' : 'none';
  }

  function renderOzonChatsList() {
    const wrap = document.getElementById('ozon-chats-list');
    if (!wrap) return;
    if (!ozonChatsRaw.length) {
      wrap.innerHTML = '<div class="form-hint">Нет чатов. Нажмите «Обновить список чатов».</div>';
      return;
    }
    wrap.innerHTML = ozonChatsRaw.map(c => {
      const id = String(c.chat_id || '');
      const enc = encodeURIComponent(id);
      const active = ozonChatSelectedId === id ? 'wb-chat-item active' : 'wb-chat-item';
      const preview = escapeHtml(c.preview || '—');
      const cat = ozonChatCategoryLabel(c.category);
      const typeHint = c.chat_type ? ` · ${escapeHtml(String(c.chat_type))}` : '';
      const dateLabel = escapeHtml(c.last_activity_label || c.created_at_label || '—');
      return `<button type="button" class="${active}" data-chat-id="${enc}"><div class="wb-chat-item-name">${escapeHtml(cat)}${typeHint}</div><div class="wb-chat-item-preview">${preview}</div><div class="wb-chat-item-preview" style="opacity:.75;font-size:.82em;">${dateLabel}</div></button>`;
    }).join('');
    wrap.querySelectorAll('.wb-chat-item').forEach(btn => {
      btn.addEventListener('click', () => {
        const chatId = decodeURIComponent(btn.getAttribute('data-chat-id') || '');
        ozonChatSelectedId = chatId;
        renderOzonChatsList();
        ozonChatShowDetailShell();
        void loadOzonChatThread(chatId);
      });
    });
  }

  function ozonChatShowDetailShell() {
    const hint = document.getElementById('ozon-chats-hint');
    const body = document.getElementById('ozon-chats-detail-body');
    if (hint) hint.style.display = 'none';
    if (body) body.style.display = '';
    ozonChatClientMessageKey = '';
    const threadEl = document.getElementById('ozon-chats-thread');
    if (threadEl) threadEl.innerHTML = '<div class="form-hint">Подгружаю переписку…</div>';
    const ta = document.getElementById('ozon-chats-draft');
    if (ta) ta.value = '';
    const statusHint = document.getElementById('ozon-chats-status-hint');
    if (statusHint) statusHint.textContent = '';
    const sendBtn = document.getElementById('btn-ozon-chats-send');
    if (sendBtn) sendBtn.disabled = false;
    const replyTools = document.getElementById('ozon-chats-reply-tools');
    const readonlyHint = document.getElementById('ozon-chats-readonly-hint');
    if (replyTools) replyTools.style.display = ozonChatsFilter === 'buyers' ? '' : 'none';
    if (readonlyHint) readonlyHint.style.display = ozonChatsFilter === 'buyers' ? 'none' : '';
  }

  async function refreshOzonChatsList(forceRefresh = true) {
    try {
      await ensureStoresLoaded();
      fillStoreSelects();
    } catch (err) {
      if (!getOzonChatsStoreId()) {
        setChatStatusBar('ozon-chats-status-bar', 'error', err.message || 'Не удалось загрузить магазины');
        setPanelLoading('ozon-chats-loading', false);
        return;
      }
    }
    const sid = getOzonChatsStoreId();
    if (!sid) {
      const oz = storesForMarketplace('ozon');
      const msg = oz.length
        ? 'Выберите магазин Ozon в списке выше.'
        : 'Нет магазинов Ozon. Добавьте во вкладке «Магазины»: тип Ozon, Client-Id и Api-Key.';
      setChatStatusBar('ozon-chats-status-bar', 'error', msg);
      setPanelLoading('ozon-chats-loading', false);
      const wrap = document.getElementById('ozon-chats-list');
      if (wrap) wrap.innerHTML = `<div class="form-hint">${escapeHtml(msg)}</div>`;
      return;
    }
    const gen = ++ozonChatsListFetchGen;
    const filter = ozonChatsFilter || 'buyers';
    const qParts = [`filter=${encodeURIComponent(filter)}`];
    if (forceRefresh) qParts.push('refresh=1');
    const q = '?' + qParts.join('&');
    const storeLabel = getStoreNameById(sid) || `ID ${sid}`;
    const wrap = document.getElementById('ozon-chats-list');
    if (wrap) wrap.innerHTML = '<div class="form-hint">Загрузка списка…</div>';
    const loadMsg = `Загружаю чаты Ozon «${storeLabel}»… (1 запрос/с, может занять до минуты)`;
    setChatStatusBar('ozon-chats-status-bar', 'loading', loadMsg);
    setPanelLoading('ozon-chats-loading', true, loadMsg);
    setChatToolbarBusy('ozon-chats', true);
    try {
      const data = await api(`/ozon/buyer-chats/${sid}${q}`, { timeoutMs: 120000 });
      if (gen !== ozonChatsListFetchGen) return;
      if (Number(getOzonChatsStoreId()) !== Number(sid)) return;
      ozonChatsRaw = data.chats || [];
      ozonChatsListStoreId = sid;
      ozonChatsListFilter = filter;
      ozonChatSelectedId = null;
      renderOzonChatsList();
      if (data.unavailable) {
        setChatStatusBar('ozon-chats-status-bar', 'info', data.message || 'Чаты недоступны для этого магазина.');
        return;
      }
      const n = ozonChatsRaw.length;
      setChatStatusBar(
        'ozon-chats-status-bar',
        n ? 'ok' : 'info',
        n ? `Загружено чатов: ${n}. Выберите чат слева.` : 'Чатов пока нет (или нет доступа Premium Plus/Pro).',
      );
      const hint = document.getElementById('ozon-chats-hint');
      const body = document.getElementById('ozon-chats-detail-body');
      if (hint) {
        hint.style.display = '';
        hint.textContent = 'Выберите чат слева.';
      }
      if (body) body.style.display = 'none';
    } catch (err) {
      if (gen !== ozonChatsListFetchGen) return;
      ozonChatsRaw = [];
      ozonChatsListStoreId = null;
      renderOzonChatsList();
      setChatStatusBar('ozon-chats-status-bar', 'error', err.message || 'Ошибка загрузки чатов Ozon');
      toast(err.message, 'error');
    } finally {
      if (gen === ozonChatsListFetchGen) {
        setPanelLoading('ozon-chats-loading', false);
        setChatToolbarBusy('ozon-chats', false);
      }
    }
  }

  async function loadOzonChatThread(chatId) {
    const sid = getOzonChatsStoreId();
    if (!sid || !chatId) return;
    const gen = ++ozonChatThreadFetchGen;
    setPanelLoading('ozon-chats-loading', true, 'Загружаю переписку…');
    setChatToolbarBusy('ozon-chats', true);
    try {
      const t = await api(`/ozon/buyer-chats/${sid}/${encodeURIComponent(chatId)}/thread`);
      if (gen !== ozonChatThreadFetchGen) return;
      if (String(ozonChatSelectedId || '') !== String(chatId)) return;
      ozonChatClientMessageKey = (t.client_message_key || '').trim();
      const titleEl = document.getElementById('ozon-chats-product-title');
      if (titleEl) titleEl.textContent = t.product_title || '—';
      const threadEl = document.getElementById('ozon-chats-thread');
      const lines = t.lines || [];
      if (threadEl) {
        threadEl.innerHTML = lines.length
          ? lines.map(l => {
            const lab = ozonChatRoleLabel(l.role);
            const dt = l.created_at ? escapeHtml(String(l.created_at).slice(0, 19).replace('T', ' ')) : '';
            const dtHtml = dt ? `<span class="wb-chat-date" style="opacity:.7;font-size:.82em;margin-right:8px;">${dt}</span>` : '';
            return `<div class="wb-chat-line"><span class="wb-chat-role">${lab}</span>${dtHtml}<span class="wb-chat-text">${escapeHtml(l.text)}</span></div>`;
          }).join('')
          : '<div class="form-hint">Нет текста сообщений.</div>';
      }
      const statusHint = document.getElementById('ozon-chats-status-hint');
      const sendBtn = document.getElementById('btn-ozon-chats-send');
      const replyTools = document.getElementById('ozon-chats-reply-tools');
      const readonlyHint = document.getElementById('ozon-chats-readonly-hint');
      const canReply = !!t.can_reply && t.category === 'buyer';
      const replyBlocked = !!t.reply_window_blocked || !canReply;
      if (replyTools) replyTools.style.display = canReply ? '' : 'none';
      if (readonlyHint) readonlyHint.style.display = canReply ? 'none' : '';
      if (sendBtn) sendBtn.disabled = replyBlocked;
      if (statusHint) {
        if (!canReply) {
          statusHint.textContent = 'Только просмотр';
          statusHint.style.color = '#64748b';
        } else if (replyBlocked) {
          statusHint.textContent = t.reply_window_reason || 'Окно ответа Ozon закрыто — отправка недоступна.';
          statusHint.style.color = '#b91c1c';
        } else if (t.reply_window_warning) {
          statusHint.textContent = t.reply_window_warning;
          statusHint.style.color = '#b45309';
        } else if (t.already_replied) {
          statusHint.textContent = 'На последнее сообщение покупателя уже отвечали.';
          statusHint.style.color = '#b45309';
        } else if (t.skip_reason === 'before_cutoff') {
          statusHint.textContent = 'Сообщение раньше даты «отвечать с» из настроек.';
          statusHint.style.color = '#b45309';
        } else if (t.skip_reason === 'last_not_client') {
          statusHint.textContent = 'Последнее сообщение не от покупателя.';
          statusHint.style.color = '';
        } else {
          statusHint.textContent = '';
          statusHint.style.color = '';
        }
      }
    } catch (err) {
      if (gen !== ozonChatThreadFetchGen) return;
      toast(err.message, 'error');
    } finally {
      if (gen === ozonChatThreadFetchGen) {
        setPanelLoading('ozon-chats-loading', false);
        setChatToolbarBusy('ozon-chats', false);
      }
    }
  }

  async function loadOzonChatsPanel() {
    syncOzonChatsFilterUI();
    const gen = ++ozonChatsPanelGen;
    if (!stores.length) {
      setChatStatusBar('ozon-chats-status-bar', 'loading', 'Подготавливаю список магазинов…');
    }
    try {
      await ensureStoresLoaded();
    } catch (e) {
      if (gen !== ozonChatsPanelGen) return;
      if (getOzonChatsStoreId()) {
        await refreshOzonChatsList(false);
        return;
      }
      setChatStatusBar('ozon-chats-status-bar', 'error', (e && e.message) ? e.message : 'Не удалось загрузить магазины');
      return;
    }
    if (gen !== ozonChatsPanelGen) return;
    fillStoreSelects();
    if (gen !== ozonChatsPanelGen) return;
    const sid = getOzonChatsStoreId();
    if (!sid) {
      const oz = storesForMarketplace('ozon');
      const msg = oz.length
        ? 'Выберите магазин Ozon в списке.'
        : 'Нет магазинов Ozon — добавьте во вкладке «Магазины» (Client-Id + Api-Key).';
      setChatStatusBar('ozon-chats-status-bar', 'error', msg);
      ozonChatsRaw = [];
      renderOzonChatsList();
      return;
    }
    if (sid != null && ozonChatsListStoreId != null && Number(sid) === Number(ozonChatsListStoreId)
        && ozonChatsListFilter === ozonChatsFilter && ozonChatsRaw.length) {
      renderOzonChatsList();
      setChatStatusBar('ozon-chats-status-bar', 'ok', `Загружено чатов: ${ozonChatsRaw.length}.`);
      return;
    }
    await refreshOzonChatsList(false);
  }

  async function ozonChatsGenerateDraft() {
    const sid = getOzonChatsStoreId();
    if (!sid || !ozonChatSelectedId) {
      toast('Выберите чат слева', 'error');
      return;
    }
    try {
      const r = await api(`/ozon/buyer-chats/${sid}/generate-draft`, {
        method: 'POST',
        body: JSON.stringify({ chat_id: ozonChatSelectedId }),
      });
      const ta = document.getElementById('ozon-chats-draft');
      if (ta) ta.value = (r.draft || '').trim();
      toast('Черновик сгенерирован');
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  async function ozonChatsMassGenerateSend() {
    const sid = getOzonChatsStoreId();
    if (!sid) {
      toast('Выберите магазин Ozon', 'error');
      return;
    }
    if (!confirm('Обработаются чаты, где последнее сообщение от покупателя (с учётом даты и без повторов). Продолжить?')) return;
    const btn = document.getElementById('btn-ozon-chats-mass');
    if (btn) btn.disabled = true;
    try {
      const r = await api(`/ozon/buyer-chats/${sid}/mass-generate-send`, {
        method: 'POST',
        body: JSON.stringify({ max_chats: 50 }),
      });
      if (r.ozon_chat_skipped_no_access) {
        toast(r.ozon_chat_skip_reason === 'no_premium'
          ? 'У этого магазина нет Premium — чаты пропущены.'
          : 'Чаты недоступны для этого магазина — пропущено.');
        return;
      }
      toast(
        `Ozon: отправлено ${r.ozon_chat_sent ?? 0}, уже отвечено ${r.ozon_chat_skipped_already_replied ?? 0}, раньше даты ${r.ozon_chat_skipped_before_cutoff ?? 0}, окно закрыто ${r.ozon_chat_skipped_reply_window ?? 0}, поддержка ${r.ozon_chat_skipped_support ?? 0}.`,
      );
      await refreshOzonChatsList(true);
    } catch (err) {
      toast(err.message, 'error');
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function ozonChatsSendMessage() {
    const sid = getOzonChatsStoreId();
    if (!sid || !ozonChatSelectedId) {
      toast('Выберите чат', 'error');
      return;
    }
    const ta = document.getElementById('ozon-chats-draft');
    const msg = (ta && ta.value ? ta.value : '').trim();
    if (!msg) {
      toast('Введите текст ответа', 'error');
      return;
    }
    try {
      await api(`/ozon/buyer-chats/${sid}/send`, {
        method: 'POST',
        body: JSON.stringify({
          chat_id: ozonChatSelectedId,
          message: msg,
          client_message_key: ozonChatClientMessageKey || '',
        }),
      });
      toast('Сообщение отправлено');
      await loadOzonChatThread(ozonChatSelectedId);
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function wireOzonChatsPanel() {
    if (wireOzonChatsPanel._done) return;
    wireOzonChatsPanel._done = true;
    const sel = document.getElementById('ozon-chats-store');
    if (sel) {
      sel.addEventListener('change', () => {
        if (ozonChatsSuppressSelectChange) return;
        ozonChatsRaw = [];
        ozonChatsListStoreId = null;
        ozonChatsListFilter = null;
        ozonChatSelectedId = null;
        void refreshOzonChatsList();
      });
    }
    document.querySelectorAll('#ozon-chats-filter [data-filter]').forEach(btn => {
      btn.addEventListener('click', () => {
        const f = btn.getAttribute('data-filter') || 'buyers';
        if (f === ozonChatsFilter) return;
        ozonChatsFilter = f;
        ozonChatsListFilter = null;
        ozonChatSelectedId = null;
        syncOzonChatsFilterUI();
        setNavActive('ozon-chats', { chatFilter: f });
        void refreshOzonChatsList(false);
      });
    });
    document.getElementById('btn-ozon-chats-refresh')?.addEventListener('click', () => {
      retryStoresLoad();
      setChatStatusBar('ozon-chats-status-bar', 'loading', 'Запрос к Ozon…');
      setPanelLoading('ozon-chats-loading', true, 'Запрос к Ozon…');
      void refreshOzonChatsList(true);
    });
    document.getElementById('btn-ozon-chats-mass')?.addEventListener('click', () => { void ozonChatsMassGenerateSend(); });
    document.getElementById('btn-ozon-chats-load-thread')?.addEventListener('click', () => {
      if (ozonChatSelectedId) void loadOzonChatThread(ozonChatSelectedId);
    });
    document.getElementById('btn-ozon-chats-generate')?.addEventListener('click', () => { void ozonChatsGenerateDraft(); });
    document.getElementById('btn-ozon-chats-send')?.addEventListener('click', () => { void ozonChatsSendMessage(); });
  }

  // ---- Ozon actions (promotions) ----
  let ozonActionsRaw = [];
  let ozonActionsSuppressSelectChange = false;

  function getOzonActionsStoreId() {
    const el = document.getElementById('ozon-actions-store');
    if (!el) return null;
    const v = String(el.value || '').trim();
    if (!v) return null;
    const n = parseInt(v, 10);
    return Number.isFinite(n) && n > 0 ? n : null;
  }

  function parseWatchedActionIds(raw) {
    if (!raw || !String(raw).trim()) return [];
    return String(raw).split(/[,;\s]+/).map(x => x.trim()).filter(Boolean).map(x => parseInt(x, 10)).filter(n => Number.isFinite(n) && n > 0);
  }

  function formatOzonActionPeriod(a) {
    const s = (a.date_start || '').slice(0, 10);
    const e = (a.date_end || '').slice(0, 10);
    if (s && e) return `${s} — ${e}`;
    return s || e || '—';
  }

  function setOzonActionsStatus(msg) {
    const el = document.getElementById('ozon-actions-status');
    if (el) el.textContent = msg || '';
  }

  function renderOzonActionsTable() {
    const tbody = document.getElementById('ozon-actions-tbody');
    if (!tbody) return;
    if (!ozonActionsRaw.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="form-hint">Нет акций для отображения.</td></tr>';
      return;
    }
    tbody.innerHTML = ozonActionsRaw.map(a => {
      const badges = [];
      if (a.is_auto_add) badges.push('<span class="status-badge" title="Ozon сам добавляет товары">авто</span>');
      if (a.is_voucher_action) badges.push('<span class="status-badge">промокод</span>');
      if (a.with_targeting) badges.push('<span class="status-badge">таргет</span>');
      const part = a.is_participating ? 'да' : 'нет';
      const cnt = `${a.participating_products_count || 0} / ${a.potential_products_count || 0}`;
      return `<tr data-action-id="${a.id}">
        <td class="col-check"><input type="checkbox" class="ozon-action-check" value="${a.id}" ${a.participating_products_count > 0 ? '' : 'disabled'}></td>
        <td class="col-id">${a.id ?? '—'}</td>
        <td class="col-title">${escapeHtml(a.title || '')}${badges.length ? ' ' + badges.join(' ') : ''}</td>
        <td class="col-type">${escapeHtml(a.action_type || '—')}</td>
        <td class="col-period">${escapeHtml(formatOzonActionPeriod(a))}</td>
        <td class="col-count">${cnt}</td>
        <td class="col-part">${part}</td>
      </tr>`;
    }).join('');
    const all = document.getElementById('ozon-actions-check-all');
    if (all) {
      all.checked = false;
      all.indeterminate = false;
    }
  }

  function getSelectedOzonActionIds() {
    return Array.from(document.querySelectorAll('.ozon-action-check:checked'))
      .map(el => parseInt(el.value, 10))
      .filter(n => Number.isFinite(n) && n > 0);
  }

  async function loadOzonActionsSettings() {
    const sid = getOzonActionsStoreId();
    if (!sid) return;
    try {
      const cfg = await api(`/ozon/actions/settings/${sid}`);
      const onlyAuto = document.getElementById('ozon-actions-only-auto');
      const watched = document.getElementById('ozon-actions-watched-ids');
      if (onlyAuto) onlyAuto.checked = cfg.only_auto_add !== false;
      if (watched) watched.value = (cfg.watched_action_ids || []).join(', ');
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  async function saveOzonActionsSettings() {
    const sid = getOzonActionsStoreId();
    if (!sid) {
      toast('Выберите магазин Ozon', 'error');
      return;
    }
    const only_auto_add = !!document.getElementById('ozon-actions-only-auto')?.checked;
    const watched_action_ids = parseWatchedActionIds(document.getElementById('ozon-actions-watched-ids')?.value || '');
    try {
      await api(`/ozon/actions/settings/${sid}`, {
        method: 'POST',
        body: JSON.stringify({ only_auto_add, watched_action_ids, auto_remove_on_schedule: false }),
      });
      toast('Настройки акций сохранены');
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  async function loadOzonActionsList(refresh) {
    const sid = getOzonActionsStoreId();
    if (!sid) {
      setOzonActionsStatus('Нет магазина Ozon — добавьте во вкладке «Магазины».');
      ozonActionsRaw = [];
      renderOzonActionsTable();
      return;
    }
    setPanelLoading('ozon-actions-loading', true, 'Запрос к Ozon…');
    setOzonActionsStatus('Загрузка списка акций…');
    try {
      const r = await api(`/ozon/actions/${sid}`, refresh ? { timeoutMs: 120000 } : {});
      if (r.unavailable) {
        ozonActionsRaw = [];
        renderOzonActionsTable();
        setOzonActionsStatus(r.message || 'Акции недоступны для этого магазина.');
        return;
      }
      ozonActionsRaw = r.actions || [];
      renderOzonActionsTable();
      const autoCnt = ozonActionsRaw.filter(a => a.is_auto_add).length;
      const partCnt = ozonActionsRaw.filter(a => (a.participating_products_count || 0) > 0).length;
      setOzonActionsStatus(`Загружено акций: ${ozonActionsRaw.length}. Автоакций: ${autoCnt}. С вашими товарами: ${partCnt}.`);
    } catch (err) {
      ozonActionsRaw = [];
      renderOzonActionsTable();
      setOzonActionsStatus(err.message || 'Ошибка загрузки акций');
      toast(err.message, 'error');
    } finally {
      setPanelLoading('ozon-actions-loading', false);
    }
  }

  async function ozonActionsRemoveSelected() {
    const sid = getOzonActionsStoreId();
    if (!sid) {
      toast('Выберите магазин Ozon', 'error');
      return;
    }
    const action_ids = getSelectedOzonActionIds();
    if (!action_ids.length) {
      toast('Отметьте акции в таблице', 'error');
      return;
    }
    if (!confirm(`Удалить ваши товары из ${action_ids.length} акций? Это необратимо до повторного добавления вручную.`)) return;
    const btn = document.getElementById('btn-ozon-actions-remove-selected');
    if (btn) btn.disabled = true;
    setPanelLoading('ozon-actions-loading', true, 'Удаление из акций…');
    try {
      const r = await api(`/ozon/actions/${sid}/remove`, {
        method: 'POST',
        body: JSON.stringify({ action_ids, only_auto_add: false }),
        timeoutMs: 300000,
      });
      toast(`Удалено товаров: ${r.products_removed ?? 0}, акций: ${r.actions_processed ?? 0}, отклонено: ${r.products_rejected ?? 0}`);
      setOzonActionsStatus(`Готово: удалено ${r.products_removed ?? 0} позиций из ${r.actions_processed ?? 0} акций.`);
      await loadOzonActionsList(true);
    } catch (err) {
      toast(err.message, 'error');
    } finally {
      if (btn) btn.disabled = false;
      setPanelLoading('ozon-actions-loading', false);
    }
  }

  async function ozonActionsAutoRemoveNow() {
    const sid = getOzonActionsStoreId();
    if (!sid) {
      toast('Выберите магазин Ozon', 'error');
      return;
    }
    const onlyAuto = !!document.getElementById('ozon-actions-only-auto')?.checked;
    const hint = onlyAuto ? 'всех автоакций с вашими товарами' : 'выбранных/всех акций по настройкам';
    if (!confirm(`Запустить автоудаление (${hint})?`)) return;
    await saveOzonActionsSettings();
    const btn = document.getElementById('btn-ozon-actions-auto-remove');
    if (btn) btn.disabled = true;
    setPanelLoading('ozon-actions-loading', true, 'Автоудаление…');
    try {
      const r = await api(`/ozon/actions/${sid}/auto-remove`, {
        method: 'POST',
        body: JSON.stringify({}),
        timeoutMs: 300000,
      });
      if (r.skipped && r.reason && String(r.reason).startsWith('no_')) {
        toast(r.message || 'Акции недоступны для этого магазина — пропущено.');
        setOzonActionsStatus(r.message || 'Магазин пропущен (нет доступа к акциям).');
        return;
      }
      toast(`Автоудаление: товаров ${r.products_removed ?? 0}, акций ${r.actions_processed ?? 0}, подошло ${r.actions_matched ?? 0}`);
      setOzonActionsStatus(`Автоудаление: удалено ${r.products_removed ?? 0} позиций из ${r.actions_processed ?? 0} акций.`);
      await loadOzonActionsList(true);
    } catch (err) {
      toast(err.message, 'error');
    } finally {
      if (btn) btn.disabled = false;
      setPanelLoading('ozon-actions-loading', false);
    }
  }

  async function loadOzonActionsPanel() {
    try {
      await ensureStoresLoaded();
    } catch (e) {
      setOzonActionsStatus((e && e.message) ? e.message : 'Не удалось загрузить магазины');
      return;
    }
    fillStoreSelects();
    await loadOzonActionsSettings();
    if (ozonActionsRaw.length) {
      renderOzonActionsTable();
      return;
    }
    setOzonActionsStatus('Нажмите «Загрузить акции» для списка из Ozon API.');
  }

  function wireOzonActionsPanel() {
    if (wireOzonActionsPanel._done) return;
    wireOzonActionsPanel._done = true;
    document.getElementById('ozon-actions-store')?.addEventListener('change', () => {
      if (ozonActionsSuppressSelectChange) return;
      ozonActionsRaw = [];
      renderOzonActionsTable();
      void loadOzonActionsSettings();
      setOzonActionsStatus('Магазин сменён — нажмите «Загрузить акции».');
    });
    document.getElementById('btn-ozon-actions-load')?.addEventListener('click', () => { void loadOzonActionsList(true); });
    document.getElementById('btn-ozon-actions-remove-selected')?.addEventListener('click', () => { void ozonActionsRemoveSelected(); });
    document.getElementById('btn-ozon-actions-auto-remove')?.addEventListener('click', () => { void ozonActionsAutoRemoveNow(); });
    document.getElementById('btn-ozon-actions-save-settings')?.addEventListener('click', () => { void saveOzonActionsSettings(); });
    document.getElementById('ozon-actions-check-all')?.addEventListener('change', (e) => {
      const checked = !!e.target.checked;
      document.querySelectorAll('.ozon-action-check:not(:disabled)').forEach(el => { el.checked = checked; });
    });
  }

  // ---- Card links (WB / Ozon) ----
  const MAX_LINK_ITEMS = 30;
  const CARD_LINKS_ACTION_COOLDOWN_MS = 3000;
  const CARD_LINKS_RELOAD_DELAY_MS = 2500;
  let cardLinksData = { items: [], groups: [], candidates: [], attach_suggestions: [], review_suggestions: [], combine_suggestions: [], ai_suggestions: [], catalog_meta: {} };
  let cardLinksView = 'candidates';
  let _cardLinksActionBusy = false;
  let _cardLinksCooldownUntil = 0;
  let _cardLinksReloadTimer = null;
  const cardLinksSelectedApply = new Set();
  const cardLinksSelectedReview = new Set();

  const CARD_LINKS_MAX_PAGES_OPTS = {
    wb: [
      { v: 30, label: '3 000' },
      { v: 60, label: '6 000' },
      { v: 100, label: '10 000 (рекомендуется)' },
      { v: 150, label: '15 000 (макс.)' },
    ],
    ozon: [
      { v: 5, label: '5 000' },
      { v: 15, label: '15 000 (рекомендуется)' },
      { v: 30, label: '30 000' },
      { v: 100, label: '100 000 (макс.)' },
    ],
  };
  let _cardLinksMaxPagesByMp = { wb: 100, ozon: 15 };
  let _cardLinksCatalogSearch = '';
  let _cardLinksCatalogUnlinkedOnly = false;

  function cardLinksMaxPages() {
    const mp = cardLinksMarketplace();
    const el = document.getElementById('card-links-max-pages');
    if (el && el.value) {
      const v = Number(el.value);
      if (Number.isFinite(v) && v > 0) {
        _cardLinksMaxPagesByMp[mp] = v;
        return v;
      }
    }
    return _cardLinksMaxPagesByMp[mp] || (mp === 'ozon' ? 30 : 100);
  }

  function syncCardLinksMaxPagesSelect() {
    const mp = cardLinksMarketplace();
    const sel = document.getElementById('card-links-max-pages');
    const cap = document.getElementById('card-links-max-pages-caption');
    if (!sel) return;
    const opts = CARD_LINKS_MAX_PAGES_OPTS[mp] || CARD_LINKS_MAX_PAGES_OPTS.wb;
    const prev = _cardLinksMaxPagesByMp[mp] || opts.find((o) => /рекомендуется/.test(o.label))?.v || opts[0].v;
    sel.innerHTML = opts.map((o) => `<option value="${o.v}">${escapeHtml(o.label)}</option>`).join('');
    const has = opts.some((o) => o.v === prev);
    sel.value = String(has ? prev : opts[0].v);
    _cardLinksMaxPagesByMp[mp] = Number(sel.value);
    if (cap) {
      cap.textContent = mp === 'ozon'
        ? 'Страниц каталога Ozon (×1000 карточек)'
        : 'Страниц каталога WB (×100 карточек)';
    }
  }

  function cardLinksSortCatalogRows(rows) {
    const mp = cardLinksMarketplace();
    const bundles = new Map();
    const unlinked = [];
    for (const r of rows || []) {
      if (r?.linked) {
        const gid = String(r.link_group_id || r.imt_id || r.model_name || '');
        if (!gid) {
          unlinked.push(r);
          continue;
        }
        if (!bundles.has(gid)) bundles.set(gid, []);
        bundles.get(gid).push(r);
      } else {
        unlinked.push(r);
      }
    }
    const bundleOrder = [...bundles.entries()].sort((a, b) => {
      const itemsA = a[1];
      const itemsB = b[1];
      const ca = cardLinkItemCategory(itemsA[0], mp).toLowerCase();
      const cb = cardLinkItemCategory(itemsB[0], mp).toLowerCase();
      if (ca !== cb) return ca.localeCompare(cb, 'ru');
      if (itemsB.length !== itemsA.length) return itemsB.length - itemsA.length;
      const la = String(itemsA[0].link_group_label || a[0]).toLowerCase();
      const lb = String(itemsB[0].link_group_label || b[0]).toLowerCase();
      return la.localeCompare(lb, 'ru');
    });
    const out = [];
    for (const [, items] of bundleOrder) {
      items.sort((a, b) => String(a.title || '').localeCompare(String(b.title || ''), 'ru'));
      out.push(...items);
    }
    unlinked.sort((a, b) => {
      const ca = cardLinkItemCategory(a, mp).toLowerCase();
      const cb = cardLinkItemCategory(b, mp).toLowerCase();
      if (ca !== cb) return ca.localeCompare(cb, 'ru');
      return String(a.title || '').localeCompare(String(b.title || ''), 'ru');
    });
    out.push(...unlinked);
    return out;
  }

  function cardLinksCatalogFilterRows(rows) {
    let out = rows || [];
    if (_cardLinksCatalogUnlinkedOnly) {
      out = out.filter((r) => !r.linked);
    }
    const q = (_cardLinksCatalogSearch || '').trim().toLowerCase();
    if (q) {
      const mp = cardLinksMarketplace();
      out = out.filter((r) => {
        const title = String(r.title || '').toLowerCase();
        const article = mp === 'wb'
          ? String(r.vendor_code || '').toLowerCase()
          : String(r.offer_id || '').toLowerCase();
        const mpId = mp === 'wb'
          ? String(r.nm_id || '')
          : String(r.sku || '');
        return title.includes(q) || article.includes(q) || mpId.includes(q);
      });
    }
    return cardLinksSortCatalogRows(out);
  }

  function syncCardLinksCatalogFilterBar() {
    const bar = document.getElementById('card-links-catalog-filter');
    if (!bar) return;
    bar.hidden = cardLinksView !== 'catalog';
    const searchEl = document.getElementById('card-links-catalog-search');
    const unlinkedEl = document.getElementById('card-links-catalog-unlinked-only');
    if (searchEl && searchEl !== document.activeElement && searchEl.value !== _cardLinksCatalogSearch) {
      searchEl.value = _cardLinksCatalogSearch;
    }
    if (unlinkedEl) unlinkedEl.checked = _cardLinksCatalogUnlinkedOnly;
  }

  function cardLinksCooldownLeftMs() {
    return Math.max(0, _cardLinksCooldownUntil - Date.now());
  }

  function cardLinksEnsureCooldown() {
    const left = cardLinksCooldownLeftMs();
    if (left > 0) {
      toast(`Подождите ${Math.ceil(left / 1000)} с — лимит запросов WB`, 'error');
      return false;
    }
    return true;
  }

  function cardLinksStartCooldown() {
    _cardLinksCooldownUntil = Date.now() + CARD_LINKS_ACTION_COOLDOWN_MS;
  }

  function cardLinksScheduleCatalogReload() {
    if (_cardLinksReloadTimer) clearTimeout(_cardLinksReloadTimer);
    _cardLinksReloadTimer = setTimeout(() => {
      _cardLinksReloadTimer = null;
      void loadCardLinksCatalog({ quiet: true });
    }, CARD_LINKS_RELOAD_DELAY_MS);
  }

  function cardLinksCatalogStatusSuffix(meta) {
    const m = meta || cardLinksData.catalog_meta || {};
    const parts = [];
    if (m.truncated) {
      parts.push(`загружено не всё (лимит ${m.max_pages || '?'} стр.) — увеличьте глубину`);
    } else if (m.pages_fetched) {
      const pageSize = m.page_size || (cardLinksMarketplace() === 'ozon' ? 1000 : 100);
      parts.push(`${m.pages_fetched} стр. API ×${pageSize}`);
    }
    if (m.listed_count && m.count != null && Number(m.listed_count) > Number(m.count)) {
      parts.push(`в каталоге ${m.count} из ${m.listed_count} по списку Ozon`);
    }
    return parts.length ? ` · ${parts.join(' · ')}` : '';
  }

  function cardLinksMarketplace() {
    return String(document.getElementById('card-links-marketplace')?.value || 'wb').trim();
  }

  function syncCardLinksMarketplaceUI() {
    const mp = cardLinksMarketplace();
    const wbOnly = document.querySelectorAll('.card-links-wb-only');
    const ozOnly = document.querySelectorAll('.card-links-ozon-only');
    wbOnly.forEach(el => { el.hidden = mp !== 'wb'; });
    ozOnly.forEach(el => { el.hidden = mp !== 'ozon'; });
    syncCardLinksMaxPagesSelect();
  }

  function syncCardLinksStoreSelect() {
    const sel = document.getElementById('card-links-store');
    if (!sel) return;
    const mp = cardLinksMarketplace();
    const list = storesForMarketplace(mp);
    const prev = String(sel.value || '').trim();
    sel.innerHTML = list.length
      ? list.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('')
      : `<option value="">Нет магазинов ${mp === 'wb' ? 'WB' : 'Ozon'} — добавьте во вкладке «Магазины»</option>`;
    if (list.length) {
      const ids = new Set(list.map(s => String(s.id)));
      if (prev && ids.has(prev)) sel.value = prev;
      else selectFirstStoreOption(sel);
    }
    syncCardLinksMarketplaceUI();
  }

  function setCardLinksStatus(msg) {
    const el = document.getElementById('card-links-status');
    if (el) el.textContent = msg || '';
  }

  function cardLinkPhotoCell(url) {
    const u = (url || '').trim();
    if (!u) return '<span class="card-link-thumb--empty">нет</span>';
    return `<img class="card-link-thumb" src="${escapeHtml(u)}" alt="" loading="lazy" referrerpolicy="no-referrer">`;
  }

  function cardLinksRefItemFromCandidate(c) {
    const items = c?.items || [];
    return items[0] ? { ...items[0] } : null;
  }

  function cardLinksGroupItemCount(g) {
    return Number(g?.count || (g?.items || []).length || 0);
  }

  function cardLinksCanBulkApplyCandidate(c) {
    const kind = c?.kind || '';
    const n = (c?.items || []).length || Number(c?.count || 0);
    if (['attach', 'attach_batch', 'new_link', 'combine_suggestions', 'merge_groups', 'relocate'].includes(kind)) {
      return n > 0;
    }
    return !!(c?.ai && n > 0);
  }

  function cardLinkItemCategory(it, mp) {
    if (!it) return '';
    if (mp === 'wb') {
      const parent = String(it.parent_name || '').trim();
      const subject = String(it.subject_name || '').trim();
      if (parent && subject) return `${parent} → ${subject}`;
      return subject || parent || (it.subject_id ? `subjectID ${it.subject_id}` : '');
    }
    return String(it.category_label || it.category_key || '').trim();
  }

  function cardLinksCandidateKindOrder(kind) {
    const order = {
      attach_batch: 0,
      attach: 1,
      combine_suggestions: 2,
      new_link: 3,
      merge_groups: 4,
      relocate: 5,
    };
    return order[kind] ?? 9;
  }

  function sortCardLinksCandidates(list) {
    return [...(list || [])].sort((a, b) => {
      const catA = String(a?.category_label || '').toLowerCase();
      const catB = String(b?.category_label || '').toLowerCase();
      if (catA !== catB) return catA.localeCompare(catB, 'ru');
      const kindA = cardLinksCandidateKindOrder(a?.kind || '');
      const kindB = cardLinksCandidateKindOrder(b?.kind || '');
      if (kindA !== kindB) return kindA - kindB;
      const tgtA = String(a?.target_group_label || a?.suggested_model_name || '').toLowerCase();
      const tgtB = String(b?.target_group_label || b?.suggested_model_name || '').toLowerCase();
      if (tgtA !== tgtB) return tgtA.localeCompare(tgtB, 'ru');
      return Number(b?.count || 0) - Number(a?.count || 0);
    });
  }

  function cardLinksCanCombineCandidate(c) {
    const kind = c?.kind || '';
    return kind === 'new_link' || kind === 'combine_suggestions';
  }

  function cardLinksCandidateAddCount(c) {
    const kind = c?.kind || '';
    if (kind === 'attach' || kind === 'relocate') return 1;
    if (kind === 'attach_batch') return (c?.items || []).length;
    if (kind === 'merge_groups' || kind === 'combine_suggestions') return (c?.items || []).length;
    return 0;
  }

  function cardLinksValidateLinkSize(addCount, targetGroupSize) {
    const total = Number(targetGroupSize || 0) + Number(addCount || 0);
    if (total > MAX_LINK_ITEMS) {
      return `В связке не более ${MAX_LINK_ITEMS} товаров (сейчас ${targetGroupSize || 0}, добавляете ${addCount} — всего ${total})`;
    }
    if (addCount > MAX_LINK_ITEMS) {
      return `Нельзя связать более ${MAX_LINK_ITEMS} товаров за раз`;
    }
    return '';
  }

  function cardLinksTargetGroupSize(targetImt, modelName) {
    const mp = cardLinksMarketplace();
    for (const g of cardLinksData.groups || []) {
      if (mp === 'wb' && String(g.group_id) === String(targetImt || '')) {
        return cardLinksGroupItemCount(g);
      }
      if (mp === 'ozon' && String(g.group_label || '') === String(modelName || '')) {
        return cardLinksGroupItemCount(g);
      }
    }
    return 0;
  }

  function cardLinksCandidatesSameCategory(cands) {
    if (!cands.length) return false;
    const mp = cardLinksMarketplace();
    if (mp === 'wb') {
      const sids = new Set();
      const pids = new Set();
      for (const c of cands) {
        const it = (c.items || [])[0] || {};
        sids.add(Number(c.subject_id || it.subject_id || 0));
        pids.add(Number(it.parent_id || 0));
      }
      sids.delete(0);
      pids.delete(0);
      return sids.size <= 1 && pids.size <= 1;
    }
    const cats = new Set();
    for (const c of cands) {
      const it = (c.items || [])[0] || {};
      cats.add(String(c.category_key || it.category_key || ''));
    }
    cats.delete('');
    return cats.size <= 1;
  }

  function cardLinksMergeCandidateItems(cands) {
    const mp = cardLinksMarketplace();
    const seen = new Set();
    const items = [];
    for (const c of cands) {
      for (const it of c.items || []) {
        const key = mp === 'wb' ? `nm:${it.nm_id || ''}` : `off:${it.offer_id || ''}`;
        if (!key || seen.has(key)) continue;
        seen.add(key);
        items.push({ ...it });
      }
    }
    return items;
  }

  function buildManualCombinedCandidate(cands) {
    const items = cardLinksMergeCandidateItems(cands);
    const mp = cardLinksMarketplace();
    const first = items[0] || {};
    return {
      candidate_id: 'manual-combine',
      kind: 'new_link',
      marketplace: mp,
      category_label: cands[0]?.category_label || '',
      subject_id: first.subject_id,
      category_key: first.category_key,
      count: items.length,
      suggested_target_imt: mp === 'wb' ? Number(first.imt_id || 0) : null,
      suggested_model_name: mp === 'ozon' ? (cands[0]?.suggested_model_name || '') : '',
      items,
    };
  }

  function cardLinksCombinableCandidates() {
    return cardLinksCandidateGroups().filter((c) => cardLinksCanCombineCandidate(c));
  }

  function cardLinksBulkApplyCandidates() {
    if (cardLinksView === 'review') {
      return cardLinksReviewGroups().filter((c) => cardLinksCanBulkApplyCandidate(c));
    }
    return cardLinksCandidateGroups().filter((c) => cardLinksCanBulkApplyCandidate(c));
  }

  function cardLinksPruneSelectionSets() {
    const applyable = new Set(
      cardLinksBulkApplyCandidates().map((c) => String(c.candidate_id || '')).filter(Boolean),
    );
    for (const id of [...cardLinksSelectedApply]) {
      if (!applyable.has(id)) cardLinksSelectedApply.delete(id);
    }
    const reviewable = new Set(
      cardLinksReviewGroups()
        .filter((c) => cardLinksCanBulkApplyCandidate(c))
        .map((c) => String(c.candidate_id || ''))
        .filter(Boolean),
    );
    for (const id of [...cardLinksSelectedReview]) {
      if (!reviewable.has(id)) cardLinksSelectedReview.delete(id);
    }
  }

  function cardLinksResolvedCombinableSelected() {
    cardLinksPruneSelectionSets();
    return [...cardLinksSelectedApply]
      .map((id) => findCardLinksCandidate(id))
      .filter((c) => c && cardLinksCanCombineCandidate(c));
  }

  function cardLinksResolvedSelectedReview() {
    cardLinksPruneSelectionSets();
    return [...cardLinksSelectedReview]
      .map((id) => findCardLinksCandidate(id))
      .filter((c) => c && cardLinksCanBulkApplyCandidate(c));
  }

  function cardLinksSelectAllCombinable() {
    cardLinksSelectedApply.clear();
    for (const c of cardLinksCombinableCandidates()) {
      const id = String(c.candidate_id || '').trim();
      if (id) cardLinksSelectedApply.add(id);
    }
    renderCardLinksTable();
  }

  function cardLinksSelectAllApply() {
    if (cardLinksView === 'review') {
      cardLinksSelectedReview.clear();
      for (const c of cardLinksBulkApplyCandidates()) {
        const id = String(c.candidate_id || '').trim();
        if (id) cardLinksSelectedReview.add(id);
      }
    } else {
      cardLinksSelectedApply.clear();
      for (const c of cardLinksBulkApplyCandidates()) {
        const id = String(c.candidate_id || '').trim();
        if (id) cardLinksSelectedApply.add(id);
      }
    }
    renderCardLinksTable();
  }

  function syncCardLinksReviewBar() {
    const bar = document.getElementById('card-links-review-bar');
    const label = document.getElementById('card-links-review-label');
    if (!bar || !label) return;
    const available = cardLinksReviewGroups().filter((c) => cardLinksCanBulkApplyCandidate(c));
    if (cardLinksView !== 'review' || !available.length) {
      bar.hidden = true;
      return;
    }
    const cands = cardLinksResolvedSelectedReview();
    const mergeN = cands.filter((c) => c.kind === 'merge_groups').length;
    const relocN = cands.filter((c) => c.kind === 'relocate').length;
    const parts = [];
    if (mergeN) parts.push(`${mergeN} объединений`);
    if (relocN) parts.push(`${relocN} перепривязок`);
    label.textContent = cands.length
      ? `Выбрано ${cands.length}: ${parts.join(', ') || 'операции'}`
      : `Доступно ${available.length} — отметьте чекбоксом или «Все»`;
    const btn = document.getElementById('btn-card-links-review-apply');
    if (btn) btn.disabled = cands.length < 1;
    bar.hidden = false;
  }

  function syncCardLinksApplyBar() {
    const bar = document.getElementById('card-links-apply-bar');
    const label = document.getElementById('card-links-apply-label');
    if (!bar || !label) return;
    const available = cardLinksBulkApplyCandidates();
    if (cardLinksView !== 'candidates' || !available.length) {
      bar.hidden = true;
      return;
    }
    const cands = [...cardLinksSelectedApply]
      .map((id) => findCardLinksCandidate(id))
      .filter((c) => c && cardLinksCanBulkApplyCandidate(c));
    const poolN = cands.filter((c) => c.kind === 'attach_batch' || c.kind === 'attach').length;
    const newN = cands.filter((c) => c.kind === 'new_link' || c.kind === 'combine_suggestions').length;
    const parts = [];
    if (poolN) parts.push(`${poolN} в связку`);
    if (newN) parts.push(`${newN} новых`);
    label.textContent = cands.length
      ? `Выбрано ${cands.length}${parts.length ? `: ${parts.join(', ')}` : ''}`
      : `Доступно ${available.length} — отметьте чекбоксом или «Все»`;
    const btn = document.getElementById('btn-card-links-apply-run');
    if (btn) btn.disabled = cands.length < 1;
    bar.hidden = false;
  }

  function cardLinksSelectAllReview() {
    cardLinksSelectAllApply();
  }

  async function runBulkApplyActions(viewLabel) {
    if (_cardLinksActionBusy) return;
    const cands = cardLinksView === 'review'
      ? cardLinksResolvedSelectedReview()
      : [...cardLinksSelectedApply]
        .map((id) => findCardLinksCandidate(id))
        .filter((c) => c && cardLinksCanBulkApplyCandidate(c));
    cands.sort((a, b) => {
      const order = { attach_batch: 0, attach: 1, combine_suggestions: 2, new_link: 3, merge_groups: 4, relocate: 5 };
      const pa = order[a.kind] ?? 9;
      const pb = order[b.kind] ?? 9;
      return pa - pb || String(a.target_group_label || '').localeCompare(String(b.target_group_label || ''));
    });
    if (!cands.length) {
      toast(
        (cardLinksView === 'review' ? cardLinksSelectedReview : cardLinksSelectedApply).size
          ? 'Выбранные пункты устарели — обновите каталог и выберите снова'
          : 'Отметьте предложения чекбоксом',
        'error',
      );
      return;
    }
    const pauseSec = Math.ceil(CARD_LINKS_ACTION_COOLDOWN_MS / 1000);
    if (!confirm(`Применить ${cands.length} операций?\nМежду запросами пауза ~${pauseSec} с (защита от лимита WB).`)) return;
    _cardLinksActionBusy = true;
    let ok = 0;
    let fail = 0;
    let stopped = false;
    for (let i = 0; i < cands.length; i++) {
      if (stopped) break;
      setPanelLoading('card-links-loading', true, `${viewLabel}: ${i + 1} из ${cands.length}…`);
      const res = await mergeSelectedCardLinks({
        candidate: cands[i],
        bulk: true,
        skipConfirm: true,
        skipReload: true,
        skipToast: true,
        returnResult: true,
      });
      if (res?.ok) {
        ok += 1;
      } else {
        fail += 1;
        if (res?.rateLimited) stopped = true;
      }
      if (i < cands.length - 1 && !stopped) {
        await new Promise((r) => setTimeout(r, CARD_LINKS_ACTION_COOLDOWN_MS));
      }
    }
    if (cardLinksView === 'review') cardLinksSelectedReview.clear();
    else cardLinksSelectedApply.clear();
    setPanelLoading('card-links-loading', false);
    _cardLinksActionBusy = false;
    if (ok > 0) {
      cardLinksStartCooldown();
      cardLinksScheduleCatalogReload();
    }
    toast(
      stopped && fail
        ? `Применено ${ok} из ${cands.length}, остановлено (лимит WB). Подождите и обновите каталог.`
        : `Применено ${ok} из ${cands.length}${fail ? `, ошибок: ${fail}` : ''}`,
      ok > 0 ? 'success' : 'error',
    );
    renderCardLinksTable();
  }

  async function runBulkReviewActions() {
    return runBulkApplyActions('Перепроверка');
  }

  function syncCardLinksCombineBar() {
    const bar = document.getElementById('card-links-combine-bar');
    const label = document.getElementById('card-links-combine-label');
    if (!bar || !label) return;
    const available = cardLinksCombinableCandidates();
    if (cardLinksView !== 'candidates' || !available.length) {
      bar.hidden = true;
      return;
    }
    const cands = cardLinksResolvedCombinableSelected();
    const items = cardLinksMergeCandidateItems(cands);
    const sameCat = cands.length >= 2 && cardLinksCandidatesSameCategory(cands);
    const sizeErr = sameCat ? cardLinksValidateLinkSize(items.length, 0) : '';
    if (!cands.length) {
      label.textContent = `Можно объединить ${available.length} предложений «Новая» — отметьте чекбоксом или «Все»`;
    } else if (cands.length < 2) {
      label.textContent = `Выбрано 1 — отметьте ещё одно предложение «Новая» той же категории`;
    } else if (!sameCat) {
      label.textContent = 'Разные категории — выберите предложения одной категории';
    } else {
      label.textContent = `Выбрано ${cands.length} предложения · ${items.length} товаров (макс. ${MAX_LINK_ITEMS})`;
    }
    const btn = document.getElementById('btn-card-links-combine-candidates');
    if (btn) btn.disabled = !sameCat || !!sizeErr;
    bar.hidden = false;
  }

  function cardLinksCompatibleGroups(refItem, addCount = 0) {
    const mp = cardLinksMarketplace();
    if (!refItem) return [];
    return (cardLinksData.groups || []).filter((g) => {
      if (!g.linked || (g.count || 0) < 2) return false;
      if (String(g.group_id) === '__unlinked__') return false;
      if (mp === 'wb') {
        const sid = Number(refItem.subject_id || 0);
        const pid = Number(refItem.parent_id || 0);
        if (sid && Number(g.subject_id || 0) !== sid) return false;
        if (pid && Number(g.parent_id || 0) !== pid) return false;
        return true;
      }
      const cat = String(refItem.category_key || '');
      const gcat = String(g.category_key || (g.items && g.items[0] && g.items[0].category_key) || '');
      if (cat && gcat && cat !== gcat) return false;
      return true;
    }).filter((g) => cardLinksGroupItemCount(g) + Number(addCount || 0) <= MAX_LINK_ITEMS);
  }

  function cardLinkGroupHeadLabel(g, mp) {
    if (mp === 'wb') {
      const parts = [g.parent_name, g.subject_name].filter(Boolean);
      const cat = parts.join(' → ');
      return cat ? `${cat} · imtID ${g.group_id}` : `imtID ${g.group_id}`;
    }
    const cat = g.category_label || (g.items && g.items[0] && g.items[0].category_label) || '';
    return cat ? `${cat} · ${g.group_label}` : (g.group_label || 'модель');
  }

  function cardLinkItemLineHtml(it, mp) {
    const article = mp === 'wb' ? (it.vendor_code || '—') : (it.offer_id || '—');
    const mpId = mp === 'wb' ? (it.nm_id || '—') : (it.sku || '—');
    const cat = cardLinkItemCategory(it, mp);
    const catHtml = cat ? `<span class="card-links-picker-item-cat">${escapeHtml(cat)}</span>` : '';
    return `<div class="card-links-picker-item">
      ${cardLinkPhotoCell(it.photo_url)}
      <div class="card-links-picker-item-text">
        ${catHtml}
        <span class="card-links-picker-item-title">${escapeHtml((it.title || '').slice(0, 80))}</span>
        <span class="card-links-picker-item-meta">${escapeHtml(article)} · ${escapeHtml(String(mpId))}</span>
      </div>
    </div>`;
  }

  function cardLinkGroupItemsHtml(g, mp) {
    const items = g.items || [];
    if (!items.length) return '<div class="card-links-picker-empty">нет карточек</div>';
    return `<div class="card-links-picker-items">${items.map((it) => cardLinkItemLineHtml(it, mp)).join('')}</div>`;
  }

  function cardLinkPickerBtnHtml(g, mp) {
    if (!g) {
      return '<span class="card-links-picker-placeholder">Выберите связку</span>';
    }
    const items = g.items || [];
    const thumbs = items.slice(0, 4).map((it) => cardLinkPhotoCell(it.photo_url)).join('');
    const extra = items.length > 4 ? `<span class="card-links-picker-more">+${items.length - 4}</span>` : '';
    return `<div class="card-links-picker-preview">
      <div class="card-links-picker-thumbs">${thumbs}${extra}</div>
      <div class="card-links-picker-preview-text">
        <span class="card-links-picker-preview-title">${escapeHtml(cardLinkGroupHeadLabel(g, mp))}</span>
        <span class="card-links-picker-preview-count">${items.length}${items.length >= MAX_LINK_ITEMS ? '' : ''} / ${MAX_LINK_ITEMS} шт.</span>
      </div>
    </div>`;
  }

  function cardLinkGroupByValue(value, refItem) {
    const mp = cardLinksMarketplace();
    const groups = cardLinksCompatibleGroups(refItem);
    if (!value) return null;
    return groups.find((g) => (
      mp === 'wb'
        ? String(g.group_id) === String(value)
        : String(g.group_label || '') === String(value)
    )) || null;
  }

  function cardLinkTargetPickerHtml(opts) {
    const mp = cardLinksMarketplace();
    const {
      pickerId,
      groups,
      selected,
      placeholder,
      allowEmpty,
      emptyLabel,
      extraAttrs,
    } = opts;
    const selGroup = cardLinkGroupByValue(selected, opts.refItem) || (selected ? null : null);
    const resolved = selGroup || (selected ? cardLinksCompatibleGroups(opts.refItem).find((g) => (
      mp === 'wb' ? String(g.group_id) === String(selected) : String(g.group_label) === String(selected)
    )) : null);
    const btnInner = resolved
      ? cardLinkPickerBtnHtml(resolved, mp)
      : `<span class="card-links-picker-placeholder">${escapeHtml(placeholder || 'Выберите связку')}</span>`;
    const emptyOpt = allowEmpty
      ? `<button type="button" class="card-links-picker-option card-links-picker-option--empty${!selected ? ' card-links-picker-option--active' : ''}" data-value="">
          <div class="card-links-picker-option-head"><strong>${escapeHtml(emptyLabel || 'Новая связка')}</strong></div>
        </button>`
      : '';
    const options = groups.length
      ? groups.map((g) => {
        const val = mp === 'wb' ? String(g.group_id) : String(g.group_label || '');
        const active = val === String(selected || '') ? ' card-links-picker-option--active' : '';
        return `<button type="button" class="card-links-picker-option${active}" data-value="${escapeHtml(val)}">
          <div class="card-links-picker-option-head">
            <strong>${escapeHtml(cardLinkGroupHeadLabel(g, mp))}</strong>
            <span>${g.count || (g.items || []).length} / ${MAX_LINK_ITEMS} шт.</span>
          </div>
          ${cardLinkGroupItemsHtml(g, mp)}
        </button>`;
      }).join('')
      : `<div class="card-links-picker-empty">${escapeHtml(placeholder || 'Нет подходящих связок в этой категории')}</div>`;
    const attrs = extraAttrs ? ` ${extraAttrs}` : '';
    return `<div class="card-links-picker" id="${escapeHtml(pickerId)}" data-value="${escapeHtml(String(selected || ''))}"${attrs}>
      <button type="button" class="card-links-picker-btn" aria-haspopup="listbox">${btnInner}</button>
      <div class="card-links-picker-menu" hidden role="listbox">
        ${emptyOpt}${options}
      </div>
    </div>`;
  }

  function updateCardLinkPickerButton(picker, refItem) {
    if (!picker) return;
    const mp = cardLinksMarketplace();
    const val = picker.getAttribute('data-value') || '';
    const g = cardLinkGroupByValue(val, refItem);
    const btn = picker.querySelector('.card-links-picker-btn');
    if (!btn) return;
    if (!val) {
      btn.innerHTML = `<span class="card-links-picker-placeholder">${escapeHtml(picker.getAttribute('data-placeholder') || 'Выберите связку')}</span>`;
      return;
    }
    btn.innerHTML = g ? cardLinkPickerBtnHtml(g, mp) : `<span class="card-links-picker-placeholder">${escapeHtml(val)}</span>`;
  }

  function getCardLinkPickerValue(pickerOrId) {
    const el = typeof pickerOrId === 'string' ? document.getElementById(pickerOrId) : pickerOrId;
    return el?.getAttribute('data-value') || '';
  }

  function renderCardLinksMergePickers() {
    const mp = cardLinksMarketplace();
    const checked = getSelectedCardLinkRows();
    const refItem = checked.length ? cardLinkRowPayloadFromCheckbox(checked[0]) : null;
    const groups = cardLinksCompatibleGroups(refItem);
    const wbWrap = document.getElementById('card-links-merge-picker-wrap');
    const ozWrap = document.getElementById('card-links-ozon-picker-wrap');
    const modelEl = document.getElementById('card-links-model-name');
    if (mp === 'wb' && wbWrap) {
      const prev = wbWrap.querySelector('.card-links-picker')?.getAttribute('data-value') || '';
      const stillValid = !prev || groups.some((g) => String(g.group_id) === String(prev));
      wbWrap.innerHTML = cardLinkTargetPickerHtml({
        pickerId: 'card-links-merge-picker',
        groups,
        selected: stillValid ? prev : '',
        refItem,
        placeholder: refItem ? (groups.length ? 'Куда добавить' : 'Нет связок в этой категории') : 'Сначала отметьте товар',
        allowEmpty: true,
        emptyLabel: 'Новая связка',
      });
      const picker = wbWrap.querySelector('.card-links-picker');
      if (picker) picker.setAttribute('data-placeholder', refItem ? 'Куда добавить' : 'Сначала отметьте товар');
    }
    if (mp === 'ozon' && ozWrap) {
      const prev = ozWrap.querySelector('.card-links-picker')?.getAttribute('data-value') || '';
      const stillValid = !prev || groups.some((g) => String(g.group_label) === String(prev));
      ozWrap.innerHTML = cardLinkTargetPickerHtml({
        pickerId: 'card-links-ozon-merge-picker',
        groups,
        selected: stillValid ? prev : '',
        refItem,
        placeholder: refItem ? (groups.length ? 'Выберите модель' : 'Нет моделей в этой категории') : 'Сначала отметьте товар',
        allowEmpty: false,
      });
      if (modelEl) modelEl.hidden = false;
    } else if (modelEl) {
      modelEl.hidden = mp !== 'ozon';
    }
  }

  function cardLinkCandidateSourceHtml(c, mp) {
    const items = c.items || [];
    if (!items.length) return '';
    if (c.kind === 'merge_groups') {
      const samples = c.sample_items || [];
      return `<div class="card-links-cand-source">
        <div class="card-links-cand-source-label">Перенести из «${escapeHtml(c.source_group_label || '—')}» (${c.source_group_count || '—'} шт.)</div>
        ${items.map((it) => cardLinkItemLineHtml(it, mp)).join('')}
      </div>
      <div class="card-links-cand-source card-links-cand-source--target">
        <div class="card-links-cand-source-label">В связку «${escapeHtml(c.target_group_label || '—')}» (${c.target_group_count || '—'} шт.)</div>
        ${samples.map((it) => cardLinkItemLineHtml(it, mp)).join('')}
      </div>`;
    }
    if (c.kind === 'attach_batch') {
      const samples = c.sample_items || [];
      const n = c.count || items.length;
      const catSuffix = c.category_label ? ` · ${escapeHtml(c.category_label)}` : '';
      return `<div class="card-links-cand-source">
        <div class="card-links-cand-source-label">Добавить ${n} товаров${catSuffix}</div>
        ${items.map((it) => cardLinkItemLineHtml(it, mp)).join('')}
      </div>
      <div class="card-links-cand-source card-links-cand-source--target">
        <div class="card-links-cand-source-label">В связку «${escapeHtml(c.target_group_label || '—')}» (${c.target_group_count || '—'} шт.)</div>
        ${samples.map((it) => cardLinkItemLineHtml(it, mp)).join('')}
      </div>`;
    }
    if (c.kind === 'relocate') {
      const samples = c.sample_items || [];
      return `<div class="card-links-cand-source">
        <div class="card-links-cand-source-label">Сейчас в «${escapeHtml(c.source_group_label || '—')}» (${c.source_group_count || '—'} шт.)</div>
        ${items.map((it) => cardLinkItemLineHtml(it, mp)).join('')}
      </div>
      <div class="card-links-cand-source card-links-cand-source--target">
        <div class="card-links-cand-source-label">Лучше в «${escapeHtml(c.target_group_label || '—')}» (${c.target_group_count || '—'} шт.)</div>
        ${samples.map((it) => cardLinkItemLineHtml(it, mp)).join('')}
      </div>`;
    }
    const label = (c.kind === 'attach') ? 'Добавить' : 'Связать';
    return `<div class="card-links-cand-source">
      <div class="card-links-cand-source-label">${label}</div>
      ${items.map((it) => cardLinkItemLineHtml(it, mp)).join('')}
    </div>`;
  }

  function cardLinkCandidateMeta(c, mp) {
    const kind = c.kind || 'new_link';
    if (kind === 'attach') {
      if (mp === 'wb') {
        const imt = c.suggested_target_imt || c.target_group_id || '—';
        const label = c.target_group_label || `imtID ${imt}`;
        const tgtN = c.target_group_count;
        return tgtN ? `→ ${label} (${tgtN} шт.)` : `→ ${label}`;
      }
      const m = c.suggested_model_name || c.target_group_label || '—';
      return `→ модель «${m}»`;
    }
    if (kind === 'attach_batch') {
      const n = c.count || (c.items || []).length;
      const label = c.target_group_label || (mp === 'wb' ? `imtID ${c.suggested_target_imt || c.target_group_id || '—'}` : (c.suggested_model_name || '—'));
      const tgtN = c.target_group_count;
      return tgtN ? `пул ${n} товаров → «${label}» (${tgtN} шт.)` : `пул ${n} товаров → «${label}»`;
    }
    if (kind === 'relocate') {
      const srcN = c.source_group_count || '—';
      const tgtN = c.target_group_count || '—';
      const label = c.target_group_label || (mp === 'wb' ? `imtID ${c.target_group_id || '—'}` : '—');
      return `из ${srcN} шт. → в «${label}» (${tgtN} шт.)`;
    }
    if (kind === 'merge_groups') {
      const srcN = c.source_group_count || '—';
      const tgtN = c.target_group_count || '—';
      const tgt = mp === 'wb'
        ? (c.target_group_label || `imtID ${c.target_group_id || '—'}`)
        : (c.target_group_label || c.suggested_model_name || '—');
      return `объединить ${c.count || 0}: ${srcN} → ${tgtN} шт.`;
    }
    if (kind === 'combine_suggestions') {
      const n = c.source_count || (c.source_candidate_ids || []).length || 2;
      return `соединить ${n} предложения · ${c.count || 0} карточек`;
    }
    if (mp === 'wb') {
      return `Новая связка · ${c.count || 0} карточек`;
    }
    const m = c.suggested_model_name || '—';
    return `Новая модель «${m}» · ${c.count || 0} карточек`;
  }

  function findCardLinksCandidate(candidateId) {
    const id = String(candidateId || '');
    if (!id) return null;
    for (const list of [
      cardLinksData.combine_suggestions,
      cardLinksData.candidates,
      cardLinksData.attach_suggestions,
      cardLinksData.review_suggestions,
      cardLinksData.ai_suggestions,
    ]) {
      const hit = (list || []).find((c) => String(c.candidate_id) === id);
      if (hit) return hit;
    }
    return null;
  }

  function findCardLinksCatalogRow(nmId, article) {
    const nid = Number(nmId || 0);
    const art = String(article || '').trim();
    const items = cardLinksData.items || [];
    if (nid) {
      const hit = items.find((r) => Number(r.nm_id) === nid);
      if (hit) return hit;
    }
    if (art) {
      const hit = items.find((r) => String(r.vendor_code || '') === art);
      if (hit) return hit;
    }
    return null;
  }

  function cardLinkRowPayloadFromCheckbox(el) {
    const nmId = Number(el.getAttribute('data-nm-id') || 0);
    const article = el.getAttribute('data-article') || '';
    const full = findCardLinksCatalogRow(nmId, article);
    if (full) return { ...full };
    return {
      nm_id: nmId,
      imt_id: Number(el.getAttribute('data-imt-id') || 0),
      subject_id: Number(el.getAttribute('data-subject-id') || 0),
      parent_id: Number(el.getAttribute('data-parent-id') || 0),
      vendor_code: article,
    };
  }

  function cardLinkRowsForTargetGroup(targetImt, modelName) {
    const mp = cardLinksMarketplace();
    const out = [];
    for (const g of cardLinksData.groups || []) {
      if (mp === 'wb') {
        if (String(g.group_id) !== String(targetImt || '')) continue;
      } else if (String(g.group_label || '') !== String(modelName || '')) {
        continue;
      }
      for (const it of g.items || []) {
        out.push({ ...it });
        if (out.length >= 3) return out;
      }
    }
    return out;
  }

  function buildMergeCatalogRows(checked, opts = {}) {
    const rows = [];
    const seen = new Set();
    const push = (row) => {
      if (!row) return;
      const key = cardLinksMarketplace() === 'wb'
        ? `nm:${row.nm_id || ''}`
        : `off:${row.offer_id || ''}`;
      if (!key || seen.has(key)) return;
      seen.add(key);
      rows.push(row);
    };
    for (const el of checked) push(cardLinkRowPayloadFromCheckbox(el));
    if (opts.candidate) {
      for (const it of opts.candidate.items || []) push({ ...it });
    }
    if (opts.targetImt || opts.modelName) {
      for (const it of cardLinkRowsForTargetGroup(opts.targetImt, opts.modelName)) push(it);
    }
    return rows;
  }

  function syncCardLinksMergeBarVisibility() {
    const mergeBar = document.getElementById('card-links-merge-bar');
    if (!mergeBar) return;
    const hasData = (cardLinksData.items || []).length > 0;
    const selected = getSelectedCardLinkRows().length > 0;
    mergeBar.hidden = cardLinksView !== 'catalog' || !hasData || !selected;
  }

  function syncCardLinksCheckAllState() {
    const checkAll = document.getElementById('card-links-check-all');
    if (!checkAll) return;
    if (cardLinksView === 'candidates') {
      const available = cardLinksBulkApplyCandidates();
      const selected = available.filter((c) => cardLinksSelectedApply.has(String(c.candidate_id || '')));
      checkAll.disabled = !available.length;
      checkAll.checked = available.length > 0 && selected.length === available.length;
      checkAll.indeterminate = selected.length > 0 && selected.length < available.length;
      return;
    }
    if (cardLinksView === 'review') {
      const available = cardLinksBulkApplyCandidates();
      const selected = available.filter((c) => cardLinksSelectedReview.has(String(c.candidate_id || '')));
      checkAll.disabled = !available.length;
      checkAll.checked = available.length > 0 && selected.length === available.length;
      checkAll.indeterminate = selected.length > 0 && selected.length < available.length;
      return;
    }
    const all = document.querySelectorAll('.card-links-check');
    const checked = document.querySelectorAll('.card-links-check:checked');
    checkAll.disabled = !all.length;
    checkAll.checked = all.length > 0 && checked.length === all.length;
    checkAll.indeterminate = checked.length > 0 && checked.length < all.length;
  }

  function syncCardLinksTableMode() {
    const table = document.getElementById('card-links-table');
    const panel = document.getElementById('panel-card-links');
    if (panel) panel.setAttribute('data-cl-view', cardLinksView);
    document.querySelectorAll('.card-links-tab').forEach(btn => {
      btn.classList.toggle('active', btn.getAttribute('data-cl-view') === cardLinksView);
    });
    if (!table) return;
    table.classList.toggle('card-links--candidates', cardLinksView === 'candidates' || cardLinksView === 'review');
    table.classList.toggle('card-links--catalog', cardLinksView === 'catalog');
    const thead = table.querySelector('thead');
    if (thead) thead.hidden = false;
    syncCardLinksMergeBarVisibility();
    syncCardLinksCheckAllState();
    syncCardLinksCatalogFilterBar();
  }

  function cardLinkCandidateBadge(c) {
    if (c.ai) return { cls: 'ai', text: 'ИИ' };
    if (c.kind === 'combine_suggestions') return { cls: 'combine', text: 'Соединить' };
    if (c.kind === 'attach_batch') return { cls: 'attach', text: 'Пул' };
    if (c.kind === 'relocate') return { cls: 'relocate', text: 'Перепривязать' };
    if (c.kind === 'merge_groups') return { cls: 'merge', text: 'Объединить' };
    if (c.kind === 'attach') return { cls: 'attach', text: 'В связку' };
    return { cls: 'new', text: 'Новая' };
  }

  function cardLinkCandidateTitle(c, mp) {
    return cardLinkCandidateMeta(c, mp);
  }

  function cardLinkCandidateCategoryBadge(c) {
    const cat = String(c?.category_label || '').trim();
    return cat ? `<span class="card-links-cand-cat">${escapeHtml(cat)}</span>` : '';
  }

  function cardLinksReviewGroups() {
    return cardLinksData.review_suggestions || [];
  }

  function syncCardLinksReviewBadge() {
    const badge = document.getElementById('card-links-review-badge');
    if (!badge) return;
    const n = cardLinksReviewGroups().length;
    if (n > 0) {
      badge.textContent = String(n);
      badge.hidden = false;
    } else {
      badge.hidden = true;
    }
  }

  function cardLinksCandidateGroups() {
    const combine = cardLinksData.combine_suggestions || [];
    const consumed = new Set();
    for (const c of combine) {
      for (const id of c.source_candidate_ids || []) consumed.add(String(id));
    }
    const attach = cardLinksData.attach_suggestions || [];
    const fresh = (cardLinksData.candidates || []).filter((c) => !consumed.has(String(c.candidate_id || '')));
    const ai = (cardLinksData.ai_suggestions || []).filter((c) => !consumed.has(String(c.candidate_id || '')));
    return sortCardLinksCandidates([...combine, ...attach, ...fresh, ...ai]);
  }

  function cardLinksTableCandidateGroups() {
    if (cardLinksView === 'review') return sortCardLinksCandidates(cardLinksReviewGroups());
    return cardLinksCandidateGroups();
  }

  function cardLinkRowHtml(r, mp, idx, meta) {
    const article = mp === 'wb' ? (r.vendor_code || '—') : (r.offer_id || '—');
    const mpId = mp === 'wb' ? (r.nm_id || '—') : (r.sku || '—');
    const category = cardLinkItemCategory(r, mp) || '—';
    const group = r.link_group_label || (r._group && r._group.group_label) || meta.groupLabel || '—';
    const linkedCls = r.linked ? ' linked' : '';
    const rowKey = mp === 'wb' ? `nm:${r.nm_id || idx}` : `off:${r.offer_id || idx}`;
    const cand = meta.candidate || {};
    const sugImt = cand.suggested_target_imt != null ? cand.suggested_target_imt : (r.imt_id || '');
    const sugModel = mp === 'ozon' ? (cand.suggested_model_name || '') : '';
    return `<tr>
      <td class="col-check"><input type="checkbox" class="card-links-check"
        data-row-key="${escapeHtml(rowKey)}"
        data-article="${escapeHtml(article)}"
        data-nm-id="${escapeHtml(String(r.nm_id || ''))}"
        data-imt-id="${escapeHtml(String(r.imt_id || ''))}"
        data-subject-id="${escapeHtml(String(r.subject_id || ''))}"
        data-parent-id="${escapeHtml(String(r.parent_id || ''))}"
        data-category-key="${escapeHtml(String(r.category_key || ''))}"
        data-candidate-id="${escapeHtml(String(cand.candidate_id || ''))}"
        data-candidate-kind="${escapeHtml(String(cand.kind || ''))}"
        data-suggested-imt="${escapeHtml(String(sugImt || ''))}"
        data-suggested-model="${escapeHtml(sugModel)}"></td>
      <td>${cardLinkPhotoCell(r.photo_url)}</td>
      <td>${escapeHtml(article)}</td>
      <td>${escapeHtml(String(mpId))}</td>
      <td class="card-links-col-cat">${escapeHtml(category)}</td>
      <td>${escapeHtml((r.title || '').slice(0, 120))}</td>
      <td><span class="card-links-group-badge${linkedCls}">${escapeHtml(group)}</span></td>
    </tr>`;
  }

  function renderCardLinksTable() {
    const tbody = document.getElementById('card-links-tbody');
    if (!tbody) return;
    cardLinksPruneSelectionSets();
    const mp = cardLinksMarketplace();
    syncCardLinksTableMode();
    let html = '';

    if (cardLinksView === 'catalog') {
      const allRows = (cardLinksData.items || []).map(it => ({ item: it, meta: {} }));
      const rows = cardLinksCatalogFilterRows(allRows.map((r) => r.item)).map((it) => ({ item: it, meta: {} }));
      if (!allRows.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty-cell">Нет данных — нажмите «Загрузить».</td></tr>';
      } else if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty-cell">Ничего не найдено — измените поиск или снимите фильтр «Только без связки».</td></tr>';
      } else {
        let lastBundle = '';
        let lastUnlinkedCat = '';
        for (const row of rows) {
          const it = row.item;
          if (it.linked) {
            const bundleKey = String(it.link_group_id || it.imt_id || it.model_name || '');
            if (bundleKey && bundleKey !== lastBundle) {
              lastBundle = bundleKey;
              lastUnlinkedCat = '';
              const label = String(it.link_group_label || bundleKey);
              const cat = cardLinkItemCategory(it, mp);
              const grp = (cardLinksData.groups || []).find((g) => String(g.group_id) === bundleKey);
              const n = grp?.count || grp?.items?.length || 0;
              html += `<tr class="card-links-bundle-divider"><td colspan="7"><span class="card-links-bundle-divider-label">Связка</span> ${escapeHtml(label)}${n ? ` · ${n} шт.` : ''}${cat ? ` · <span class="card-links-bundle-divider-cat">${escapeHtml(cat)}</span>` : ''}</td></tr>`;
            }
          } else {
            const cat = cardLinkItemCategory(it, mp);
            if (cat && cat !== lastUnlinkedCat) {
              lastUnlinkedCat = cat;
              lastBundle = '';
              html += `<tr class="card-links-cat-divider card-links-cat-divider--solo"><td colspan="7">Без связки · ${escapeHtml(cat)}</td></tr>`;
            }
            lastBundle = '';
          }
          html += cardLinkRowHtml(it, mp, 0, row.meta);
        }
        tbody.innerHTML = html;
      }
    } else {
      const groups = cardLinksTableCandidateGroups();
      if (!groups.length) {
        const emptyMsg = cardLinksView === 'review'
          ? 'Нет перепривязок — все товары уже в оптимальных связках или загрузите полный каталог.'
          : 'Нет предложений. Загрузите каталог или нажмите «ИИ».';
        tbody.innerHTML = `<tr><td colspan="7" class="empty-cell">${emptyMsg}</td></tr>`;
      } else {
        let lastCat = '';
        for (const c of groups) {
          const catLabel = String(c.category_label || '').trim();
          if (catLabel && catLabel !== lastCat) {
            lastCat = catLabel;
            html += `<tr class="card-links-cat-divider"><td colspan="7">${escapeHtml(catLabel)}</td></tr>`;
          }
          const kind = c.kind || 'new_link';
          const badge = cardLinkCandidateBadge(c);
          const cid = escapeHtml(String(c.candidate_id || ''));
          const title = cardLinkCandidateTitle(c, mp);
          const refItem = cardLinksRefItemFromCandidate(c);
          const addCount = cardLinksCandidateAddCount(c);
          const compatGroups = cardLinksCompatibleGroups(refItem, addCount);
          const canSelect = cardLinksCanBulkApplyCandidate(c);
          const rowChecked = cardLinksView === 'review'
            ? cardLinksSelectedReview.has(String(c.candidate_id || ''))
            : cardLinksSelectedApply.has(String(c.candidate_id || ''));
          const needsPicker = kind === 'attach' || kind === 'attach_batch' || kind === 'relocate' || kind === 'merge_groups';
          const defaultTarget = needsPicker
            ? String(c.suggested_target_imt || c.target_group_id || c.suggested_model_name || c.target_group_label || '')
            : '';
          const pickerHtml = needsPicker
            ? cardLinkTargetPickerHtml({
              pickerId: `picker-${c.candidate_id}`,
              groups: compatGroups,
              selected: compatGroups.some((g) => {
                const gid = mp === 'wb' ? String(g.group_id) : String(g.group_label);
                return gid === defaultTarget || String(g.group_label) === defaultTarget;
              })
                ? defaultTarget
                : (compatGroups[0] ? String(mp === 'wb' ? compatGroups[0].group_id : compatGroups[0].group_label) : ''),
              refItem,
              placeholder: compatGroups.length ? 'Куда добавить' : 'Нет связок в этой категории',
              allowEmpty: false,
              extraAttrs: `data-picker-candidate="${cid}"`,
            })
            : '';
          const canAct = !needsPicker || compatGroups.length > 0;
          let actionBtn = '';
          if (kind === 'attach') {
            actionBtn = `<button type="button" class="btn btn-primary btn-sm card-links-attach-btn" data-candidate-id="${cid}"${canAct ? '' : ' disabled'}>Связать</button>`;
          } else if (kind === 'attach_batch') {
            const n = c.count || (c.items || []).length;
            actionBtn = `<button type="button" class="btn btn-primary btn-sm card-links-attach-btn" data-candidate-id="${cid}"${canAct ? '' : ' disabled'}>Связать все (${n})</button>`;
          } else if (kind === 'relocate') {
            actionBtn = `<button type="button" class="btn btn-primary btn-sm card-links-attach-btn" data-candidate-id="${cid}"${canAct ? '' : ' disabled'}>Переместить</button>`;
          } else if (kind === 'merge_groups') {
            actionBtn = `<button type="button" class="btn btn-primary btn-sm card-links-merge-group" data-candidate-id="${cid}"${canAct ? '' : ' disabled'}>Объединить</button>`;
          } else {
            const itemN = c.count || (c.items || []).length;
            const overLimit = itemN > MAX_LINK_ITEMS;
            actionBtn = `<button type="button" class="btn btn-primary btn-sm card-links-merge-group" data-candidate-id="${cid}"${overLimit ? ' disabled title="Более 30 товаров"' : ''}>Связать</button>`;
          }
          const rowCheckCell = canSelect
            ? `<td class="col-check card-links-cand-check-cell">
                <label class="card-links-cand-check-wrap" title="Выбрать для пакетного применения">
                  <input type="checkbox" class="card-links-row-check" data-candidate-id="${cid}"${rowChecked ? ' checked' : ''}>
                </label>
              </td>`
            : '<td class="col-check card-links-cand-check-cell"></td>';
          html += `<tr class="card-links-cand-header">${rowCheckCell}<td colspan="6">
            <div class="card-links-cand-block">
              <div class="card-links-cand-row">
                <div class="card-links-cand-head">
                  <span class="card-links-cand-badge card-links-cand-badge--${badge.cls}">${escapeHtml(badge.text)}</span>
                  ${cardLinkCandidateCategoryBadge(c)}
                  <span class="card-links-cand-title">${escapeHtml(title)}</span>
                </div>
                <div class="card-links-cand-actions">
                  ${needsPicker ? pickerHtml : ''}
                  ${actionBtn}
                </div>
              </div>
              ${cardLinkCandidateSourceHtml(c, mp)}
            </div>
          </td></tr>`;
        }
        tbody.innerHTML = html;
      }
    }

    renderCardLinksMergePickers();
    syncCardLinksTableMode();
    syncCardLinksCombineBar();
    syncCardLinksApplyBar();
    syncCardLinksReviewBadge();
    syncCardLinksReviewBar();
  }

  function validateCardLinkSelection(checked) {
    if (!checked.length) return 'Ничего не выбрано';
    const candIds = new Set(checked.map(el => el.getAttribute('data-candidate-id')).filter(Boolean));
    if (candIds.size > 1) return 'Выберите товары только из одной группы кандидатов';
    const kinds = new Set(checked.map(el => el.getAttribute('data-candidate-kind')).filter(Boolean));
    if (kinds.size > 1) return 'Нельзя смешивать разные типы предложений';
    const kind = kinds.size ? [...kinds][0] : '';
    if (kind === 'attach' && checked.length !== 1) return '«В связку» — выберите ровно один товар';
    if (kind === 'relocate' && checked.length !== 1) return '«Перепривязать» — выберите ровно один товар';
    if (kind === 'merge_groups' && checked.length < 1) return 'Выберите товары для объединения';
    if (kind === 'new_link' && checked.length < 2) return 'Новая связка — минимум 2 товара';
    if (!kind && checked.length < 2) return 'Выберите минимум 2 карточки';
    const mp = cardLinksMarketplace();
    if (mp === 'wb') {
      const sids = new Set(checked.map(el => el.getAttribute('data-subject-id')).filter(x => x && x !== '0'));
      if (sids.size > 1) return 'Разные предметы WB (subjectID) — связывайте только одну группу кандидатов';
      const parents = new Set(checked.map(el => el.getAttribute('data-parent-id')).filter(x => x && x !== '0'));
      if (parents.size > 1) return 'Разные родительские категории WB — выберите «Выбрать группу» у одного предложения';
    } else {
      const cats = new Set(checked.map(el => el.getAttribute('data-category-key')).filter(Boolean));
      if (cats.size > 1) return 'Разные категории Ozon — связывайте товары одной категории';
    }
    return '';
  }

  function applySuggestedLinkFields(checked) {
    const first = checked[0];
    if (!first) return;
    const sugImt = Number(first.getAttribute('data-suggested-imt') || 0);
    const sugModel = (first.getAttribute('data-suggested-model') || '').trim();
    const mp = cardLinksMarketplace();
    if (sugImt && mp === 'wb') {
      const picker = document.getElementById('card-links-merge-picker');
      if (picker && !picker.getAttribute('data-value')) picker.setAttribute('data-value', String(sugImt));
    }
    if (sugModel && mp === 'ozon') {
      const picker = document.getElementById('card-links-ozon-merge-picker');
      const modelEl = document.getElementById('card-links-model-name');
      if (picker && !picker.getAttribute('data-value')) picker.setAttribute('data-value', sugModel);
      if (modelEl && !(modelEl.value || '').trim()) modelEl.value = sugModel;
    }
  }

  function getSelectedCardLinkRows() {
    return Array.from(document.querySelectorAll('.card-links-check:checked'));
  }

  async function loadCardLinksCatalog(opts = {}) {
    const mp = cardLinksMarketplace();
    const storeId = Number(document.getElementById('card-links-store')?.value || 0);
    if (!storeId) {
      setCardLinksStatus('Выберите магазин.');
      return;
    }
    const articles = (document.getElementById('card-links-articles')?.value || '').trim();
    const qs = new URLSearchParams();
    if (articles) qs.set('articles', articles);
    qs.set('max_pages', String(cardLinksMaxPages()));
    const loadingText = mp === 'wb'
      ? `Запрос к WB Content API… (до ${cardLinksMaxPages()} стр., может занять несколько минут)`
      : `Запрос к Ozon… (до ${cardLinksMaxPages()} стр.)`;
    if (!opts.quiet) setPanelLoading('card-links-loading', true, loadingText);
    if (!opts.quiet) setCardLinksStatus('');
    try {
      const data = await api(`/card-links/${mp}/${storeId}/catalog?${qs.toString()}`);
      cardLinksSelectedApply.clear();
      cardLinksSelectedReview.clear();
      cardLinksData = {
        items: data.items || [],
        groups: data.groups || [],
        candidates: data.candidates || [],
        attach_suggestions: data.attach_suggestions || [],
        review_suggestions: data.review_suggestions || [],
        combine_suggestions: data.combine_suggestions || [],
        ai_suggestions: cardLinksData.ai_suggestions || [],
        catalog_meta: data.catalog_meta || {},
      };
      const linked = data.linked_groups != null ? data.linked_groups : 0;
      const nc = (data.candidates || []).length;
      const na = (data.attach_suggestions || []).length;
      const nr = (data.review_suggestions || []).length;
      const ncomb = (data.combine_suggestions || []).length;
      const nai = (cardLinksData.ai_suggestions || []).length;
      const truncHint = cardLinksCatalogStatusSuffix(data.catalog_meta);
      const unlinked = data.unlinked_count != null ? data.unlinked_count : (data.items || []).filter((r) => !r.linked).length;
      setCardLinksStatus(`Загружено ${data.count || 0} карточек · без связки: ${unlinked} · ${linked} связок · ${nc + na + ncomb + nai} предложений · ${nr} перепроверок · макс. ${MAX_LINK_ITEMS} в связке${truncHint}`);
      syncCardLinksReviewBadge();
      if (nr > 0) {
        cardLinksView = 'review';
        toast(`Найдено ${nr} перепривязок — вкладка «Перепроверка»`, 'info');
      } else if (!opts.quiet) {
        cardLinksView = 'candidates';
      }
      document.querySelectorAll('.card-links-tab').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-cl-view') === cardLinksView);
      });
      if (data.catalog_meta?.truncated) {
        toast('Загружены не все карточки — увеличьте «Страниц каталога» в настройках загрузки', 'error');
      }
      renderCardLinksTable();
    } catch (e) {
      const msg = (e && e.message) ? e.message : 'Ошибка загрузки';
      setCardLinksStatus(msg);
      if (/429|too many requests|слишком много запросов/i.test(msg)) {
        toast('Лимит WB API — подождите 1–2 минуты перед повтором', 'error');
        cardLinksStartCooldown();
        _cardLinksCooldownUntil = Date.now() + 60000;
      }
      cardLinksData = { items: [], groups: [], candidates: [], attach_suggestions: [], review_suggestions: [], combine_suggestions: [], ai_suggestions: [], catalog_meta: {} };
      renderCardLinksTable();
    } finally {
      setPanelLoading('card-links-loading', false);
    }
  }

  async function disconnectSelectedCardLinks() {
    if (_cardLinksActionBusy) return;
    if (!cardLinksEnsureCooldown()) return;
    const mp = cardLinksMarketplace();
    const storeId = Number(document.getElementById('card-links-store')?.value || 0);
    const checked = getSelectedCardLinkRows();
    if (!storeId || !checked.length) {
      toast('Выберите хотя бы одну карточку', 'error');
      return;
    }
    if (mp === 'wb') {
      const nmIds = checked.map(el => Number(el.getAttribute('data-nm-id') || 0)).filter(x => x > 0);
      if (!nmIds.length) {
        toast('nmID не найдены', 'error');
        return;
      }
      if (!confirm(`Разъединить ${nmIds.length} карточек WB?`)) return;
      _cardLinksActionBusy = true;
      setPanelLoading('card-links-loading', true, 'Разъединение в WB…');
      try {
        await api(`/card-links/wb/${storeId}/disconnect`, {
          method: 'POST',
          body: JSON.stringify({ nm_ids: nmIds }),
        });
        toast('Запрос на разъединение отправлен');
        cardLinksStartCooldown();
        cardLinksScheduleCatalogReload();
      } catch (e) {
        const msg = (e && e.message) ? e.message : 'Ошибка';
        toast(msg, 'error');
        if (/429|too many requests|слишком много запросов/i.test(msg)) {
          _cardLinksCooldownUntil = Date.now() + 60000;
        }
      } finally {
        setPanelLoading('card-links-loading', false);
        _cardLinksActionBusy = false;
      }
      return;
    }
    const articles = checked.map(el => el.getAttribute('data-article')).filter(Boolean);
    if (!confirm(`Разъединить ${articles.length} товаров Ozon (уникальное название модели у каждого)?`)) return;
    _cardLinksActionBusy = true;
    setPanelLoading('card-links-loading', true, 'Разъединение на Ozon…');
    try {
      await api(`/card-links/ozon/${storeId}/unlink`, {
        method: 'POST',
        body: JSON.stringify({ offer_ids: articles }),
      });
      toast('Уникальные названия модели заданы');
      cardLinksStartCooldown();
      cardLinksScheduleCatalogReload();
    } catch (e) {
      const msg = (e && e.message) ? e.message : 'Ошибка';
      toast(msg, 'error');
      if (/429|too many requests|слишком много запросов/i.test(msg)) {
        _cardLinksCooldownUntil = Date.now() + 60000;
      }
    } finally {
      setPanelLoading('card-links-loading', false);
      _cardLinksActionBusy = false;
    }
  }

  async function mergeSelectedCardLinks(opts = {}) {
    const bulk = !!opts.bulk;
    const returnResult = !!opts.returnResult;
    const finish = (result) => (returnResult ? result : undefined);

    if (!bulk) {
      if (_cardLinksActionBusy) return finish({ ok: false, skipped: true });
      if (!cardLinksEnsureCooldown()) return finish({ ok: false, skipped: true });
    }

    const fail = (msg, extra = {}) => {
      if (!opts.skipToast) toast(msg, 'error');
      return finish({ ok: false, error: msg, ...extra });
    };

    const mp = cardLinksMarketplace();
    const storeId = Number(document.getElementById('card-links-store')?.value || 0);
    const candidate = opts.candidate || null;
    let checked = opts.checked || getSelectedCardLinkRows();
    if (!checked.length && candidate) {
      document.querySelectorAll('.card-links-check').forEach((el) => {
        el.checked = el.getAttribute('data-candidate-id') === String(candidate.candidate_id || '');
      });
      checked = getSelectedCardLinkRows();
    }
    const kind = candidate?.kind || checked[0]?.getAttribute('data-candidate-kind') || '';
    if (!checked.length && candidate && (candidate.items || []).length) {
      if (kind === 'relocate' && candidate.items.length !== 1) {
        return fail('«Перепривязать» — ровно один товар');
      }
      if (kind === 'attach' && candidate.items.length !== 1) {
        return fail('«В связку» — ровно один товар');
      }
      if (kind === 'attach_batch' && candidate.items.length < 1) {
        return fail('Пул пуст');
      }
      if ((kind === 'new_link' || kind === 'combine_suggestions') && candidate.items.length < 2) {
        return fail('Новая связка — минимум 2 товара');
      }
      if (kind === 'merge_groups' && candidate.items.length < 1) {
        return fail('Нет товаров для объединения');
      }
    } else {
      const valErr = validateCardLinkSelection(checked);
      if (valErr) return fail(valErr);
    }
    if (checked.length) applySuggestedLinkFields(checked);
    const linkKind = kind === 'combine_suggestions' ? 'new_link' : (kind === 'attach_batch' ? 'attach' : kind);
    const minCount = (linkKind === 'attach' || linkKind === 'relocate') ? 1 : (linkKind === 'merge_groups' ? 1 : 2);
    const itemCount = checked.length || (candidate?.items || []).length;
    if (!storeId || itemCount < minCount) {
      const msg = (kind === 'attach' || kind === 'attach_batch' || kind === 'relocate')
        ? 'Выберите один товар'
        : (kind === 'merge_groups' ? 'Нет товаров для объединения' : 'Выберите минимум 2 карточки');
      return fail(msg);
    }
    const articles = checked.length
      ? checked.map(el => el.getAttribute('data-article')).filter(Boolean)
      : (candidate.items || []).map((it) => (mp === 'wb' ? it.vendor_code : it.offer_id)).filter(Boolean);
    if (mp === 'wb') {
      let targetImt = Number(opts.targetImt || 0);
      if (!targetImt && opts.candidate) {
        const picker = document.querySelector(`.card-links-picker[data-picker-candidate="${opts.candidate.candidate_id}"]`);
        targetImt = Number(getCardLinkPickerValue(picker) || opts.candidate.suggested_target_imt || opts.candidate.target_group_id || 0);
      }
      if (!targetImt) {
        targetImt = Number(getCardLinkPickerValue('card-links-merge-picker') || 0);
      }
      if (!targetImt) {
        targetImt = Number(
          candidate?.suggested_target_imt
          || candidate?.target_group_id
          || checked[0].getAttribute('data-suggested-imt')
          || 0,
        );
      }
      const nmIds = checked.length
        ? checked.map(el => Number(el.getAttribute('data-nm-id') || 0)).filter(x => x > 0)
        : (candidate?.items || []).map((it) => Number(it.nm_id || 0)).filter(x => x > 0);
      if ((linkKind === 'new_link' || kind === 'combine_suggestions') && !targetImt) {
        const first = (candidate?.items || [])[0];
        if (first) targetImt = Number(first.imt_id || 0);
        else if (checked[0]) targetImt = Number(checked[0].getAttribute('data-imt-id') || 0);
      }
      if ((kind === 'attach' || kind === 'attach_batch') && !targetImt) return fail('Выберите целевую связку');
      if (kind === 'relocate' && !targetImt) return fail('Выберите связку для перепривязки');
      if (kind === 'merge_groups' && !targetImt) return fail('Выберите целевую связку');
      if (kind !== 'attach' && kind !== 'attach_batch' && kind !== 'relocate' && kind !== 'merge_groups' && nmIds.length < 2) {
        return fail('Новая связка — минимум 2 карточки');
      }
      if ((kind === 'attach' || kind === 'attach_batch' || kind === 'relocate') && nmIds.length < 1) return fail('Нет товара для связки');
      if (kind === 'merge_groups' && nmIds.length < 1) return fail('Нет товаров для объединения');
      const targetSize = cardLinksTargetGroupSize(targetImt, null);
      let sizeErr = '';
      if (linkKind === 'attach' || linkKind === 'relocate' || linkKind === 'merge_groups') {
        sizeErr = cardLinksValidateLinkSize(nmIds.length, targetSize);
      } else {
        sizeErr = targetImt
          ? cardLinksValidateLinkSize(nmIds.length, targetSize)
          : cardLinksValidateLinkSize(nmIds.length, 0);
      }
      if (sizeErr) return fail(sizeErr);
      const confirmMsg = kind === 'relocate'
        ? `Переместить ${nmIds.length} карточку в imtID ${targetImt}?`
        : (kind === 'merge_groups'
          ? `Объединить ${nmIds.length} карточек в imtID ${targetImt}?`
          : (kind === 'attach_batch'
            ? `Добавить ${nmIds.length} карточек в imtID ${targetImt}?`
            : `Объединить ${nmIds.length} карточек WB в imtID ${targetImt}?`));
      const catalogRows = checked.length
        ? buildMergeCatalogRows(checked, { candidate, targetImt })
        : buildMergeCatalogRows([], { candidate, targetImt });
      if (!opts.skipConfirm && !confirm(confirmMsg)) return finish({ ok: false, cancelled: true });
      if (!bulk) {
        _cardLinksActionBusy = true;
        setPanelLoading('card-links-loading', true, 'Отправка в WB…');
      }
      try {
        await api(`/card-links/wb/${storeId}/merge`, {
          method: 'POST',
          body: JSON.stringify({ target_imt: targetImt, nm_ids: nmIds, catalog_rows: catalogRows }),
        });
        if (!opts.skipToast) toast('Запрос на объединение отправлен');
        if (!bulk && !opts.skipReload) {
          cardLinksStartCooldown();
          cardLinksScheduleCatalogReload();
        }
        return finish({ ok: true });
      } catch (e) {
        const msg = (e && e.message) ? e.message : 'Ошибка';
        const rateLimited = /429|too many requests|слишком много запросов/i.test(msg);
        if (!opts.skipToast) toast(msg, 'error');
        if (rateLimited) _cardLinksCooldownUntil = Date.now() + 60000;
        return finish({ ok: false, error: msg, rateLimited });
      } finally {
        if (!bulk) {
          setPanelLoading('card-links-loading', false);
          _cardLinksActionBusy = false;
        }
      }
      return;
    }
    let modelName = (opts.modelName || document.getElementById('card-links-model-name')?.value || '').trim();
    if (!modelName && opts.candidate) {
      const picker = document.querySelector(`.card-links-picker[data-picker-candidate="${opts.candidate.candidate_id}"]`);
      modelName = (getCardLinkPickerValue(picker) || '').trim();
    }
    if (!modelName) {
      modelName = getCardLinkPickerValue('card-links-ozon-merge-picker');
    }
    if (!modelName) {
      modelName = (
        candidate?.suggested_model_name
        || candidate?.target_group_label
        || (checked[0] && checked[0].getAttribute('data-suggested-model'))
        || ''
      ).trim();
    }
    if (!modelName) {
      return fail(kind === 'relocate' ? 'Выберите целевую модель' : 'Введите название модели');
    }
    const ozTargetSize = cardLinksTargetGroupSize(null, modelName);
    const ozSizeErr = cardLinksValidateLinkSize(articles.length, ozTargetSize);
    if (ozSizeErr) return fail(ozSizeErr);
    const ozCatalogRows = buildMergeCatalogRows(checked, { candidate, modelName });
    const ozConfirm = kind === 'relocate'
      ? `Переместить ${articles.length} товар в модель «${modelName}»?`
      : (kind === 'merge_groups'
        ? `Объединить ${articles.length} товаров с моделью «${modelName}»?`
        : `Связать ${articles.length} товаров Ozon с моделью «${modelName}»?`);
    if (!opts.skipConfirm && !confirm(ozConfirm)) return finish({ ok: false, cancelled: true });
    if (!bulk) {
      _cardLinksActionBusy = true;
      setPanelLoading('card-links-loading', true, 'Обновление «Названия модели» на Ozon…');
    }
    try {
      await api(`/card-links/ozon/${storeId}/link`, {
        method: 'POST',
        body: JSON.stringify({ offer_ids: articles, model_name: modelName, catalog_rows: ozCatalogRows }),
      });
      if (!opts.skipToast) toast('Название модели обновлено');
      if (!bulk && !opts.skipReload) {
        cardLinksStartCooldown();
        cardLinksScheduleCatalogReload();
      }
      return finish({ ok: true });
    } catch (e) {
      const msg = (e && e.message) ? e.message : 'Ошибка';
      const rateLimited = /429|too many requests|слишком много запросов/i.test(msg);
      if (!opts.skipToast) toast(msg, 'error');
      if (rateLimited) _cardLinksCooldownUntil = Date.now() + 60000;
      return finish({ ok: false, error: msg, rateLimited });
    } finally {
      if (!bulk) {
        setPanelLoading('card-links-loading', false);
        _cardLinksActionBusy = false;
      }
    }
  }

  async function runOzonQtyTableLink(opts = {}) {
    const dryRun = !!opts.dryRun;
    const mp = cardLinksMarketplace();
    if (mp !== 'ozon') {
      toast('Таблица TMS — только для Ozon', 'error');
      return;
    }
    const storeId = Number(document.getElementById('card-links-store')?.value || 0);
    if (!storeId) {
      toast('Выберите магазин Ozon', 'error');
      return;
    }
    const table = (document.getElementById('card-links-qty-table')?.value || '').trim();
    if (!table) {
      toast('Вставьте таблицу TMS', 'error');
      return;
    }
    if (_cardLinksActionBusy) return;
    if (!dryRun && !cardLinksEnsureCooldown()) return;
    if (!dryRun && !confirm('Связать все строки? Будет одно «Название модели» и кол-во в упаковке 1 / 2 / 3 по колонкам.')) return;

    _cardLinksActionBusy = true;
    const loadMsg = dryRun ? 'Проверяю таблицу TMS…' : 'Связка по таблице TMS…';
    setPanelLoading('card-links-loading', true, loadMsg);
    setCardLinksStatus('');
    try {
      const data = await api(`/card-links/ozon/${storeId}/link-qty-table`, {
        method: 'POST',
        body: JSON.stringify({ table, dry_run: dryRun }),
        timeoutMs: dryRun ? 120000 : 600000,
      });
      const fails = (data.results || data.preview || []).filter((r) => r && r.ok === false);
      const msg = data.message || (dryRun ? 'Проверка завершена' : 'Готово');
      setCardLinksStatus(msg);
      if (fails.length) {
        const first = fails[0];
        toast(`${msg} · строка ${first.row}: ${first.error || 'ошибка'}`, fails.length === 1 ? 'error' : 'info');
      } else {
        toast(msg, dryRun ? 'info' : 'success');
      }
      if (!dryRun && (data.ok_count || 0) > 0) {
        cardLinksStartCooldown();
        cardLinksScheduleCatalogReload();
      }
    } catch (e) {
      const errMsg = (e && e.message) ? e.message : 'Ошибка';
      setCardLinksStatus(errMsg);
      toast(errMsg, 'error');
    } finally {
      setPanelLoading('card-links-loading', false);
      _cardLinksActionBusy = false;
    }
  }

  async function loadCardLinksAiSuggest() {
    const mp = cardLinksMarketplace();
    const storeId = Number(document.getElementById('card-links-store')?.value || 0);
    if (!storeId) {
      setCardLinksStatus('Выберите магазин.');
      return;
    }
    const articles = (document.getElementById('card-links-articles')?.value || '').trim();
    const qs = new URLSearchParams();
    if (articles) qs.set('articles', articles);
    setPanelLoading('card-links-loading', true, 'ИИ анализирует каталог…');
    try {
      const data = await api(`/card-links/${mp}/${storeId}/ai-suggest?${qs.toString()}`, { method: 'POST', body: '{}' });
      cardLinksData.ai_suggestions = data.ai_suggestions || [];
      cardLinksView = 'candidates';
      document.querySelectorAll('.card-links-tab').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-cl-view') === 'candidates');
      });
      setCardLinksStatus(`ИИ: ${cardLinksData.ai_suggestions.length} предложений`);
      renderCardLinksTable();
    } catch (e) {
      toast((e && e.message) ? e.message : 'Ошибка ИИ', 'error');
    } finally {
      setPanelLoading('card-links-loading', false);
    }
  }

  function loadCardLinksPanel() {
    ensureStoresLoaded().then(() => {
      fillStoreSelects();
      syncCardLinksStoreSelect();
      if (!cardLinksData.items.length) {
        setCardLinksStatus('Выберите магазин и нажмите «Загрузить каталог».');
      }
      renderCardLinksTable();
    }).catch(err => setCardLinksStatus(err.message || 'Ошибка'));
  }

  function wireCardLinksPanel() {
    if (wireCardLinksPanel._done) return;
    wireCardLinksPanel._done = true;
    document.getElementById('panel-card-links')?.addEventListener('click', (e) => {
      const pickerBtn = e.target.closest('.card-links-picker-btn');
      if (pickerBtn) {
        const picker = pickerBtn.closest('.card-links-picker');
        const menu = picker?.querySelector('.card-links-picker-menu');
        if (!menu) return;
        const willOpen = menu.hidden;
        document.querySelectorAll('.card-links-picker-menu').forEach((m) => { m.hidden = true; });
        menu.hidden = !willOpen;
        e.stopPropagation();
        return;
      }
      const opt = e.target.closest('.card-links-picker-option');
      if (opt) {
        const picker = opt.closest('.card-links-picker');
        if (!picker) return;
        picker.setAttribute('data-value', opt.getAttribute('data-value') || '');
        const candId = picker.getAttribute('data-picker-candidate');
        const refItem = candId
          ? cardLinksRefItemFromCandidate(findCardLinksCandidate(candId))
          : (getSelectedCardLinkRows()[0] ? cardLinkRowPayloadFromCheckbox(getSelectedCardLinkRows()[0]) : null);
        updateCardLinkPickerButton(picker, refItem);
        const menu = picker.querySelector('.card-links-picker-menu');
        if (menu) menu.hidden = true;
        e.stopPropagation();
      }
    });
    document.addEventListener('click', () => {
      document.querySelectorAll('.card-links-picker-menu').forEach((m) => { m.hidden = true; });
    });
    document.getElementById('card-links-marketplace')?.addEventListener('change', () => {
      cardLinksData = { items: [], groups: [], candidates: [], attach_suggestions: [], review_suggestions: [], combine_suggestions: [], ai_suggestions: [], catalog_meta: {} };
      void ensureStoresLoaded().then(() => {
        syncCardLinksStoreSelect();
        renderCardLinksTable();
        setCardLinksStatus('Маркетплейс сменён — нажмите «Загрузить».');
      });
    });
    document.getElementById('card-links-store')?.addEventListener('change', () => {
      cardLinksData = { items: [], groups: [], candidates: [], attach_suggestions: [], review_suggestions: [], combine_suggestions: [], ai_suggestions: [], catalog_meta: {} };
      renderCardLinksTable();
      setCardLinksStatus('Магазин сменён — нажмите «Загрузить».');
    });
    document.getElementById('card-links-catalog-search')?.addEventListener('input', (e) => {
      _cardLinksCatalogSearch = String(e.target.value || '');
      renderCardLinksTable();
    });
    document.getElementById('card-links-catalog-unlinked-only')?.addEventListener('change', (e) => {
      _cardLinksCatalogUnlinkedOnly = !!e.target.checked;
      renderCardLinksTable();
    });
    document.getElementById('btn-card-links-load')?.addEventListener('click', () => { void loadCardLinksCatalog(); });
    document.getElementById('btn-card-links-qty-check')?.addEventListener('click', () => { void runOzonQtyTableLink({ dryRun: true }); });
    document.getElementById('btn-card-links-qty-link')?.addEventListener('click', () => { void runOzonQtyTableLink({ dryRun: false }); });
    document.getElementById('btn-card-links-ai')?.addEventListener('click', () => { void loadCardLinksAiSuggest(); });
    document.getElementById('btn-card-links-merge')?.addEventListener('click', () => { void mergeSelectedCardLinks(); });
    document.getElementById('btn-card-links-disconnect')?.addEventListener('click', () => { void disconnectSelectedCardLinks(); });
    document.getElementById('btn-card-links-combine-candidates')?.addEventListener('click', () => {
      const cands = cardLinksResolvedCombinableSelected();
      if (cands.length < 2) {
        toast('Выберите минимум 2 предложения одной категории', 'error');
        return;
      }
      if (!cardLinksCandidatesSameCategory(cands)) {
        toast('Объединяйте предложения только одной категории', 'error');
        return;
      }
      const combined = buildManualCombinedCandidate(cands);
      const sizeErr = cardLinksValidateLinkSize(combined.items.length, 0);
      if (sizeErr) {
        toast(sizeErr, 'error');
        return;
      }
      if (combined.items.length < 2) {
        toast('Новая связка — минимум 2 товара', 'error');
        return;
      }
      void mergeSelectedCardLinks({ candidate: combined });
    });
    document.getElementById('btn-card-links-combine-select-all')?.addEventListener('click', () => { cardLinksSelectAllCombinable(); });
    document.getElementById('btn-card-links-combine-clear')?.addEventListener('click', () => {
      for (const c of cardLinksCombinableCandidates()) {
        cardLinksSelectedApply.delete(String(c.candidate_id || ''));
      }
      renderCardLinksTable();
    });
    document.getElementById('btn-card-links-apply-run')?.addEventListener('click', () => { void runBulkApplyActions('Предложения'); });
    document.getElementById('btn-card-links-apply-select-all')?.addEventListener('click', () => { cardLinksSelectAllApply(); });
    document.getElementById('btn-card-links-apply-clear')?.addEventListener('click', () => {
      cardLinksSelectedApply.clear();
      renderCardLinksTable();
    });
    document.getElementById('btn-card-links-review-apply')?.addEventListener('click', () => { void runBulkReviewActions(); });
    document.getElementById('btn-card-links-review-select-all')?.addEventListener('click', () => { cardLinksSelectAllReview(); });
    document.getElementById('btn-card-links-review-clear')?.addEventListener('click', () => {
      cardLinksSelectedReview.clear();
      renderCardLinksTable();
    });
    document.getElementById('card-links-tbody')?.addEventListener('change', (e) => {
      if (e.target.matches('.card-links-row-check')) {
        const id = e.target.getAttribute('data-candidate-id');
        if (!id) return;
        const targetSet = cardLinksView === 'review' ? cardLinksSelectedReview : cardLinksSelectedApply;
        if (e.target.checked) targetSet.add(id);
        else targetSet.delete(id);
        syncCardLinksApplyBar();
        syncCardLinksCombineBar();
        syncCardLinksReviewBar();
        syncCardLinksCheckAllState();
        return;
      }
      if (e.target.matches('.card-links-check')) {
        renderCardLinksMergePickers();
        syncCardLinksMergeBarVisibility();
      }
    });
    document.getElementById('card-links-tbody')?.addEventListener('click', (e) => {
      const attachBtn = e.target.closest('.card-links-attach-btn');
      if (attachBtn) {
        const c = findCardLinksCandidate(attachBtn.getAttribute('data-candidate-id'));
        if (!c) return;
        void mergeSelectedCardLinks({ candidate: c });
        return;
      }
      const mergeGroupBtn = e.target.closest('.card-links-merge-group');
      if (mergeGroupBtn) {
        const c = findCardLinksCandidate(mergeGroupBtn.getAttribute('data-candidate-id'));
        if (!c) return;
        void mergeSelectedCardLinks({ candidate: c });
        return;
      }
    });
    document.getElementById('card-links-check-all')?.addEventListener('change', (e) => {
      const on = !!e.target.checked;
      if (cardLinksView === 'candidates') {
        if (on) cardLinksSelectAllApply();
        else {
          cardLinksSelectedApply.clear();
          renderCardLinksTable();
        }
        return;
      }
      if (cardLinksView === 'review') {
        if (on) cardLinksSelectAllReview();
        else {
          cardLinksSelectedReview.clear();
          renderCardLinksTable();
        }
        return;
      }
      document.querySelectorAll('.card-links-check').forEach(el => { el.checked = on; });
      renderCardLinksMergePickers();
      syncCardLinksMergeBarVisibility();
      syncCardLinksCheckAllState();
    });
    document.querySelectorAll('.card-links-tab').forEach(btn => {
      btn.addEventListener('click', () => {
        cardLinksView = btn.getAttribute('data-cl-view') || 'catalog';
        document.querySelectorAll('.card-links-tab').forEach(b => b.classList.toggle('active', b === btn));
        renderCardLinksTable();
      });
    });
  }

  // ---- Items (reviews / questions) ----
  let reviews = [];
  let questions = [];
  let reviewsOffset = 0;
  let questionsOffset = 0;
  const PAGE_SIZE = 200;

  function escapeHtml(s) {
    if (s == null) return '';
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  function formatDate(iso) {
    if (!iso) return '—';
    try {
      const d = new Date(iso.replace('Z', '+00:00'));
      return d.toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
    } catch (_) {
      return iso.slice(0, 16);
    }
  }

  function pollTask(taskId, progressWrapId, fillId, textId, onDone, panelPrefix = '') {
    const wrap = document.getElementById(progressWrapId);
    const fill = document.getElementById(fillId);
    const textEl = document.getElementById(textId);
    const stopBtn = wrap.querySelector('.btn-stop');
    // Не даём нескольким таймерам "драться" за один прогресс-бар
    if (wrap._interval) {
      clearInterval(wrap._interval);
      wrap._interval = null;
    }
    wrap.classList.add('visible');
    if (stopBtn) {
      stopBtn.disabled = false;
      stopBtn.onclick = async () => {
        try {
          await api('/tasks/' + taskId + '/cancel', { method: 'POST', body: JSON.stringify({}) });
          toast('Остановлено');
        } catch (err) {
          toast(err.message, 'error');
        }
      };
    }
    if (panelPrefix) setActiveTask(panelPrefix, taskId);
    wrap._interval = setInterval(async () => {
      try {
        const state = await api('/tasks/' + taskId);
        const [cur, total] = state.progress || [0, 1];
        const pctRaw = total ? Math.round((cur / total) * 100) : 0;
        const pct = Math.max(0, Math.min(100, pctRaw));
        fill.style.width = pct + '%';
        const detail = (state.detail || '').trim();
        const base = state.status === 'running' ? 'Выполняется' : state.status === 'done' ? 'Готово' : state.status;
        const line = `${base} — ${pct}% (${cur}/${total})${detail ? ' · ' + detail : ''}`;
        textEl.textContent = line;
        if (state.status === 'done') {
          clearInterval(wrap._interval);
          wrap._interval = null;
          wrap.classList.remove('visible');
          if (stopBtn) stopBtn.disabled = true;
          if (panelPrefix) setActiveTask(panelPrefix, '');
          if (onDone) onDone(state.result);
        } else if (state.status === 'error' || state.status === 'cancelled') {
          clearInterval(wrap._interval);
          wrap._interval = null;
          wrap.classList.remove('visible');
          if (stopBtn) stopBtn.disabled = true;
          if (panelPrefix) {
            setActiveTask(panelPrefix, '');
            setPanelOpsBusy(panelPrefix, false);
          }
          toast(state.error || (state.status === 'cancelled' ? 'Остановлено' : 'Ошибка'), state.status === 'cancelled' ? 'success' : 'error');
        }
      } catch (_) {}
    }, 500);
  }

  async function loadReviews(reset = true) {
    const storeId = document.getElementById('reviews-store').value || null;
    const status = (document.getElementById('reviews-status')?.value || 'new,generated');
    if (reset) { reviewsOffset = 0; reviews = []; }
    const q = (storeId ? ('?item_type=review&store_id=' + storeId) : '?item_type=review')
      + (status ? '&status=' + encodeURIComponent(status) : '')
      + ('&limit=' + PAGE_SIZE + '&offset=' + reviewsOffset);
    try {
      const page = await api('/items' + q);
      reviews = reviews.concat(page || []);
      renderItems('reviews', reviews, true);
      document.getElementById('reviews-empty').style.display = reviews.length ? 'none' : 'block';
      document.querySelector('#panel-reviews .items-table-wrap').style.display = reviews.length ? 'block' : 'none';
      const moreBtn = document.getElementById('btn-more-reviews');
      if (moreBtn) moreBtn.style.display = (page && page.length === PAGE_SIZE) ? 'inline-flex' : 'none';
      reviewsOffset += (page ? page.length : 0);
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  async function loadQuestions(reset = true) {
    const storeId = document.getElementById('questions-store').value || null;
    const status = (document.getElementById('questions-status')?.value || 'new,generated');
    if (reset) { questionsOffset = 0; questions = []; }
    const q = (storeId ? ('?item_type=question&store_id=' + storeId) : '?item_type=question')
      + (status ? '&status=' + encodeURIComponent(status) : '')
      + ('&limit=' + PAGE_SIZE + '&offset=' + questionsOffset);
    try {
      const page = await api('/items' + q);
      questions = questions.concat(page || []);
      renderItems('questions', questions, false);
      document.getElementById('questions-empty').style.display = questions.length ? 'none' : 'block';
      document.querySelector('#panel-questions .items-table-wrap').style.display = questions.length ? 'block' : 'none';
      const moreBtn = document.getElementById('btn-more-questions');
      if (moreBtn) moreBtn.style.display = (page && page.length === PAGE_SIZE) ? 'inline-flex' : 'none';
      questionsOffset += (page ? page.length : 0);
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function renderItems(prefix, items, showRating) {
    const tbody = document.getElementById(prefix + '-tbody');
    const statusRu = {
      new: 'Новый',
      generated: 'Сгенерирован',
      sending: 'Отправляется',
      sent: 'Отправлен',
      ignored: 'Игнор',
    };
    tbody.innerHTML = items.map(item => {
      const title = (item.product_title || '').slice(0, 50);
      const text = (item.text || '').slice(0, 80);
      const statusClass = ['new', 'generated', 'sending', 'sent'].includes(item.status) ? item.status : 'sent';
      const ratingCell = showRating ? `<td>${item.rating != null ? item.rating + ' ★' : '—'}</td>` : '';
      return `
        <tr data-id="${item.id}" data-prefix="${prefix}">
          <td class="col-check"><input type="checkbox" class="item-check" data-id="${item.id}"></td>
          <td class="col-date">${formatDate(item.date)}</td>
          ${ratingCell}
          <td><div class="text-preview" title="${escapeHtml(title + ' ' + text)}">${escapeHtml(title || text)}</div></td>
          <td class="col-status"><span class="status-badge ${statusClass}">${statusRu[item.status] || item.status}</span></td>
        </tr>`;
    }).join('');
    tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', (e) => {
        if (e.target.closest('.item-check')) return;
        const id = Number(tr.getAttribute('data-id'));
        const prefix = tr.getAttribute('data-prefix');
        const item = (prefix === 'reviews' ? reviews : questions).find(i => i.id === id);
        if (!item) return;
        const store = stores.find(s => s.id === item.store_id);
        showItemModal(item, store ? store.name : '—', prefix === 'reviews', prefix);
      });
    });
  }

  function showItemModal(item, storeName, isReview, prefix) {
    const modal = document.getElementById('modal-item-detail');
    const titleEl = document.getElementById('modal-item-detail-title');
    const storeEl = document.getElementById('modal-item-store');
    const productEl = document.getElementById('modal-item-product');
    const textLabel = document.getElementById('modal-item-text-label');
    const textEl = document.getElementById('modal-item-text');
    const answerInput = document.getElementById('modal-item-answer-input');
    const statusEl = document.getElementById('modal-item-status');
    const errEl = document.getElementById('modal-item-send-error');
    const btnSave = document.getElementById('btn-modal-item-save');
    if (!modal) return;
    _modalItemCtx = { id: item.id, prefix: prefix || (isReview ? 'reviews' : 'questions') };
    titleEl.textContent = isReview ? 'Отзыв' : 'Вопрос';
    textLabel.textContent = isReview ? 'Текст отзыва' : 'Текст вопроса';
    storeEl.textContent = storeName;
    productEl.textContent = (item.product_title || '').trim() || '—';
    textEl.textContent = (item.text || '').trim() || '—';
    if (answerInput) {
      answerInput.value = (item.generated_text || '').trim();
      const locked = item.status === 'sent' || item.status === 'sending';
      answerInput.readOnly = locked;
      answerInput.disabled = locked;
    }
    if (btnSave) btnSave.style.display = (item.status === 'sent' || item.status === 'sending') ? 'none' : '';
    const statusRu = { new: 'Новый', generated: 'Сгенерирован', sending: 'Отправляется', sent: 'Отправлен', ignored: 'Игнор' };
    if (statusEl) statusEl.textContent = 'Статус: ' + (statusRu[item.status] || item.status);
    if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
    modal.classList.add('visible');
  }

  function getSelectedIds(prefix) {
    return Array.from(document.querySelectorAll('#' + prefix + '-tbody .item-check:checked')).map(cb => Number(cb.getAttribute('data-id')));
  }

  function getVisibleIds(prefix) {
    const list = prefix === 'reviews' ? reviews : questions;
    return list.map(i => i.id);
  }

  function getItemsList(prefix) {
    return prefix === 'reviews' ? reviews : questions;
  }

  function filterIdsForGenerate(prefix, ids, mode) {
    const list = getItemsList(prefix);
    const idSet = new Set(ids);
    const selected = list.filter(it => idSet.has(it.id));
    if (mode === 'all') {
      return { ids: selected.map(it => it.id), willOverwrite: selected.filter(it => (it.generated_text || '').trim()).length };
    }
    // default: only new without answer
    const filtered = selected
      .filter(it => String(it.status || '') === 'new')
      .filter(it => !String(it.generated_text || '').trim());
    return { ids: filtered.map(it => it.id), willOverwrite: 0 };
  }

  document.getElementById('check-all-reviews').addEventListener('change', function () {
    document.querySelectorAll('#reviews-tbody .item-check').forEach(cb => { cb.checked = this.checked; });
  });
  document.getElementById('check-all-questions').addEventListener('change', function () {
    document.querySelectorAll('#questions-tbody .item-check').forEach(cb => { cb.checked = this.checked; });
  });

  async function runLoadNew(panelPrefix) {
    const storeSelId = panelPrefix === 'reviews' ? 'reviews-store' : 'questions-store';
    const storeIdRaw = (document.getElementById(storeSelId)?.value || '').trim();
    if (!storeIdRaw) {
      toast('Выберите магазин в фильтре (не «Все магазины») — загрузка только для выбранного магазина.', 'error');
      return;
    }
    const sid = Number(storeIdRaw);
    if (!Number.isFinite(sid) || sid <= 0) {
      toast('Некорректный магазин в фильтре', 'error');
      return;
    }
    setPanelOpsBusy(panelPrefix, true);
    try {
      const res = await api('/load-new', { method: 'POST', body: JSON.stringify({ store_ids: [sid] }) });
      const storeMeta = stores.find(s => Number(s.id) === sid);
      pollTask(res.task_id, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', (result) => {
        setPanelOpsBusy(panelPrefix, false);
        const n = Number(result ?? 0);
        if (n > 0) {
          toast(`Загружено записей: ${n}`);
        } else if (String(storeMeta?.marketplace || '').toLowerCase() === 'ozon') {
          toast(
            'Загружено 0. Отзывы: подписка «Управление отзывами» или Premium Pro + Review read only. '
            + 'Вопросы: Premium Plus + question/list. API-ключ должен быть от выбранного магазина.',
            'info',
          );
        } else {
          toast('Загружено записей: 0');
        }
        loadReviews();
        loadQuestions();
      }, panelPrefix);
    } catch (err) {
      setPanelOpsBusy(panelPrefix, false);
      toast(err.message, 'error');
    }
  }

  document.getElementById('btn-load-reviews').addEventListener('click', () => runLoadNew('reviews'));
  document.getElementById('btn-load-reviews-2').addEventListener('click', () => runLoadNew('reviews'));
  document.getElementById('btn-load-questions').addEventListener('click', () => runLoadNew('questions'));
  document.getElementById('btn-load-questions-2').addEventListener('click', () => runLoadNew('questions'));

  async function runGenerate(panelPrefix) {
    const ids = getSelectedIds(panelPrefix).length ? getSelectedIds(panelPrefix) : getVisibleIds(panelPrefix);
    if (!ids.length) {
      toast('Выберите строки или загрузите список', 'error');
      return;
    }
    const modeEl = document.getElementById(panelPrefix + '-generate-mode');
    const mode = (modeEl && modeEl.value) ? String(modeEl.value) : 'new_only';
    const filtered = filterIdsForGenerate(panelPrefix, ids, mode);
    if (!filtered.ids.length) {
      toast('Нет подходящих для генерации (нужны «Новые» и без ответа)', 'error');
      return;
    }
    if (mode === 'all' && filtered.willOverwrite) {
      if (!confirm(`Перегенерировать и перезаписать ответы для ${filtered.willOverwrite} шт.?`)) return;
    }
    setPanelOpsBusy(panelPrefix, true);
    try {
      const res = await api('/generate', { method: 'POST', body: JSON.stringify({ item_ids: filtered.ids }) });
      pollTask(res.task_id, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', (result) => {
        setPanelOpsBusy(panelPrefix, false);
        const r = result || {};
        toast('Сгенерировано: ' + (r.ok ?? 0) + ', ошибок: ' + (r.failed ?? 0));
        if (panelPrefix === 'reviews') loadReviews();
        else loadQuestions();
      }, panelPrefix);
    } catch (err) {
      setPanelOpsBusy(panelPrefix, false);
      toast(err.message, 'error');
    }
  }

  document.getElementById('btn-generate-reviews').addEventListener('click', () => runGenerate('reviews'));
  document.getElementById('btn-generate-questions').addEventListener('click', () => runGenerate('questions'));

  function ratingGroupFromValue(v, rating) {
    if (v === 'all') return true;
    if (v === 'none') return rating == null;
    if (v === '1') return rating === 1;
    if (v === '2') return rating === 2;
    if (v === '3') return rating === 3;
    if (v === '4-5') return rating != null && rating >= 4;
    return true;
  }

  async function applyTemplateToReviews() {
    const template = (document.getElementById('reviews-template-text').value || '').trim();
    if (!template) {
      toast('Введите текст шаблона', 'error');
      return;
    }
    const ratingFilter = document.getElementById('reviews-template-rating').value || 'all';
    const ids = getSelectedIds('reviews').length ? getSelectedIds('reviews') : getVisibleIds('reviews');
    if (!ids.length) {
      toast('Выберите строки или загрузите список', 'error');
      return;
    }
    const idSet = new Set(ids);
    const filtered = reviews
      .filter(r => idSet.has(r.id))
      .filter(r => r.status === 'new')
      .filter(r => !(r.generated_text || '').trim())
      .filter(r => ratingGroupFromValue(ratingFilter, r.rating))
      .map(r => r.id);
    if (!filtered.length) {
      toast('Нет подходящих отзывов (статус Новый и без ответа)', 'error');
      return;
    }
    try {
      const res = await api('/apply-template', { method: 'POST', body: JSON.stringify({ item_ids: filtered, template_text: template }) });
      toast('Шаблон применён: ' + (res.applied ?? 0) + ', пропущено: ' + (res.skipped ?? 0));
      loadReviews();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  const btnApplyTemplate = document.getElementById('btn-apply-template');
  if (btnApplyTemplate) btnApplyTemplate.addEventListener('click', applyTemplateToReviews);

  async function runSend(panelPrefix) {
    const ids = getSelectedIds(panelPrefix).length ? getSelectedIds(panelPrefix) : getVisibleIds(panelPrefix);
    if (!ids.length) {
      toast('Выберите строки или загрузите список', 'error');
      return;
    }
    const list = getItemsList(panelPrefix);
    const idSet = new Set(ids);
    const selected = list.filter((it) => idSet.has(it.id));
    const withAnswer = selected.filter((it) => (it.generated_text || '').trim() && it.status !== 'sent' && it.status !== 'sending');
    if (!withAnswer.length) {
      toast('Нет ответов для отправки (нужен сгенерированный ответ, статус не «Отправлен»)', 'error');
      return;
    }
    const byStore = {};
    withAnswer.forEach((it) => {
      const st = stores.find((s) => s.id === it.store_id);
      const key = st ? `${st.name} (${(st.marketplace || '').toUpperCase()})` : `Магазин #${it.store_id}`;
      byStore[key] = (byStore[key] || 0) + 1;
    });
    const summary = Object.entries(byStore)
      .map(([name, n]) => `<div><strong>${escapeHtml(name)}</strong>: ${n} отв.</div>`)
      .join('');
    const sendIds = withAnswer.map((it) => it.id);
    openSendConfirmModal(
      `<p>Всего к отправке: <strong>${sendIds.length}</strong></p>${summary}`,
      async () => {
        setPanelOpsBusy(panelPrefix, true);
        try {
          const res = await api('/send', { method: 'POST', body: JSON.stringify({ item_ids: sendIds }) });
          pollTask(res.task_id, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', (result) => {
            setPanelOpsBusy(panelPrefix, false);
            const r = result || {};
            toast('Отправлено: ' + (r.sent_ok ?? 0) + ', пропущено: ' + (r.skipped ?? 0) + ', ошибок: ' + (r.failed ?? 0));
            if (panelPrefix === 'reviews') loadReviews();
            else loadQuestions();
            loadStats();
          }, panelPrefix);
        } catch (err) {
          setPanelOpsBusy(panelPrefix, false);
          toast(err.message, 'error');
        }
      },
    );
  }

  document.getElementById('btn-send-reviews').addEventListener('click', () => runSend('reviews'));
  document.getElementById('btn-send-questions').addEventListener('click', () => runSend('questions'));

  document.getElementById('reviews-store').addEventListener('change', () => loadReviews(true));
  document.getElementById('questions-store').addEventListener('change', () => loadQuestions(true));
  document.getElementById('reviews-status').addEventListener('change', () => loadReviews(true));
  document.getElementById('questions-status').addEventListener('change', () => loadQuestions(true));
  document.getElementById('btn-more-reviews').addEventListener('click', () => loadReviews(false));
  document.getElementById('btn-more-questions').addEventListener('click', () => loadQuestions(false));

  // ---- Settings ----
  const DEFAULT_OZON_ALERT_TELEGRAM_TEMPLATE = (
    '⚠️ <b>{telegram_title}</b>\n\n'
    + '🏪 <b>Магазин:</b> {store_name}\n'
    + '{optional_threat_type}'
    + '📅 <b>Срок:</b> {deadline}\n'
    + '⚡ <b>Последствия:</b> {consequence}\n'
    + '{optional_amount}{optional_product}'
    + '\n<blockquote>{summary}</blockquote>\n\n'
    + '✅ <b>Действия:</b> {action_needed}\n'
    + '🕐 {message_at} · {chat_type}'
  );

  const DEFAULT_CARD_CHECK_TELEGRAM_TEMPLATE = (
    '⚠️ <b>Ошибка в карточке</b> <i>(вероятно)</i>\n\n'
    + '🏪 <b>Магазин:</b> {store_name}\n'
    + '📦 <b>Товар:</b> {product_title}\n'
    + '📋 <b>Источник:</b> {source_label}\n\n'
    + '<b>Текст покупателя:</b>\n<blockquote>{customer_text}</blockquote>\n\n'
    + '⚡ <b>Возможная ошибка:</b> {error_kind}\n'
    + '<i>{explanation}</i>'
  );

  const PROMPT_GROUP_ORDER = ['review', 'question', 'buyer_chat', 'card_check', 'ozon_important_alert'];
  const PROMPT_GROUP_TITLES = {
    review: 'Отзывы',
    question: 'Вопросы',
    buyer_chat: 'Чаты с покупателями (WB и Ozon)',
    card_check: 'Проверка карточки товара',
    ozon_important_alert: 'Уведомления Ozon (поддержка)',
  };

  function promptRatingLabel(itemType, ratingGroup) {
    if (itemType === 'review') return ratingGroup;
    return ratingGroup === 'general' ? 'общий' : ratingGroup;
  }

  function renderPromptsList(prompts) {
    const wrap = document.getElementById('prompts-list');
    if (!wrap) return;
    const byType = {};
    (prompts || []).forEach(p => {
      if (!byType[p.item_type]) byType[p.item_type] = [];
      byType[p.item_type].push(p);
    });
    const parts = [];
    PROMPT_GROUP_ORDER.forEach(type => {
      const rows = byType[type];
      if (!rows || !rows.length) return;
      parts.push(`<h3 class="prompts-group-title" style="margin:18px 0 10px; font-size:1rem;">${escapeHtml(PROMPT_GROUP_TITLES[type] || type)}</h3>`);
      rows.sort((a, b) => String(a.rating_group).localeCompare(String(b.rating_group)));
      rows.forEach(p => {
        parts.push(`
          <div class="form-group" style="margin-top: 10px;">
            <label>${escapeHtml(promptRatingLabel(p.item_type, p.rating_group))}</label>
            <textarea data-prompt-id="${p.id}" class="prompt-text">${escapeHtml(p.prompt_text)}</textarea>
          </div>`);
      });
    });
    wrap.innerHTML = parts.join('') || '<div class="form-hint">Нет промптов</div>';
    wrap.querySelectorAll('.prompt-text').forEach(ta => {
      ta.addEventListener('blur', async () => {
        const id = Number(ta.getAttribute('data-prompt-id'));
        await api('/prompts/' + id, { method: 'PATCH', body: JSON.stringify({ prompt_text: ta.value }) });
        toast('Промпт сохранён');
      });
    });
  }

  async function loadSettings() {
    syncSettingsSectionUI();
    const apiBaseEl = document.getElementById('setting-api_base');
    if (apiBaseEl) apiBaseEl.value = localStorage.getItem(STORAGE_API_BASE) || '';
    // UI prefs
    const uiCompact = document.getElementById('ui-compact');
    const uiDim = document.getElementById('ui-dim-bg');
    const uiReduce = document.getElementById('ui-reduce-motion');
    const uiBgMotion = document.getElementById('ui-bg-motion');
    const uiThemeDark = document.getElementById('theme-dark');
    const uiToast = document.getElementById('ui-toast-duration');
    const uiConfirm = document.getElementById('ui-confirm-danger');
    if (uiCompact) uiCompact.checked = localStorage.getItem(STORAGE_UI_COMPACT) === '1';
    if (uiDim) uiDim.checked = localStorage.getItem(STORAGE_UI_DIM_BG) === '1';
    if (uiReduce) uiReduce.checked = localStorage.getItem(STORAGE_UI_REDUCE_MOTION) === '1';
    if (uiBgMotion) uiBgMotion.checked = localStorage.getItem(STORAGE_UI_BG_MOTION) === '1';
    if (uiThemeDark) uiThemeDark.checked = localStorage.getItem(STORAGE_UI_THEME) === 'dark';
    if (uiToast) uiToast.value = String(getUiToastMs());
    if (uiConfirm) uiConfirm.checked = (localStorage.getItem(STORAGE_UI_CONFIRM_DANGER) || '1') === '1';
    try {
      await loadMe(true);
      const data = await api('/settings');
      const secretSettingKeys = ['openai_key', 'telegram_bot_token'];
      [
        'telegram_chat_id',
        'telegram_report_chat_id',
        'telegram_card_error_chat_id',
        'telegram_agent_chat_id',
        'telegram_agent_user_id',
        'buyer_chat_reply_from_date',
      ].forEach(k => {
        const el = document.getElementById('setting-' + k);
        if (el) el.value = data[k] || '';
      });
      secretSettingKeys.forEach(k => {
        const el = document.getElementById('setting-' + k);
        if (!el) return;
        el.value = '';
        const isSet = String(data[k + '_set'] || '0') === '1';
        el.placeholder = isSet
          ? 'Ключ сохранён — введите новый, чтобы заменить'
          : (k === 'openai_key' ? 'sk-...' : '123456789:AAH...');
      });
      const autoAgeEl = document.getElementById('setting-buyer_chat_auto_max_age_days');
      if (autoAgeEl) autoAgeEl.value = String(data.buyer_chat_auto_max_age_days || '3');
      const tgEnabled = document.getElementById('setting-telegram_enabled');
      if (tgEnabled) tgEnabled.checked = String(data.telegram_enabled || '1') !== '0';
      const tgReport = document.getElementById('setting-telegram_report_enabled');
      if (tgReport) tgReport.checked = String(data.telegram_report_enabled || '0') === '1';
      const tgInterval = document.getElementById('setting-telegram_report_interval');
      if (tgInterval) tgInterval.value = (data.telegram_report_interval || 'hour') === 'day' ? 'day' : 'hour';
      const tgAgent = document.getElementById('setting-telegram_agent_enabled');
      if (tgAgent) tgAgent.checked = String(data.telegram_agent_enabled || '0') === '1';
      const cardEnabled = document.getElementById('setting-card_check_enabled');
      if (cardEnabled) cardEnabled.checked = String(data.card_check_enabled || '1') !== '0';
      const cardTg = document.getElementById('setting-card_check_telegram_enabled');
      if (cardTg) cardTg.checked = String(data.card_check_telegram_enabled || '1') !== '0';
      const cardInReport = document.getElementById('setting-card_check_include_in_periodic_report');
      if (cardInReport) cardInReport.checked = String(data.card_check_include_in_periodic_report || '1') !== '0';
      const cardTpl = document.getElementById('setting-card_check_telegram_template');
      if (cardTpl) {
        cardTpl.value = data.card_check_telegram_template || DEFAULT_CARD_CHECK_TELEGRAM_TEMPLATE;
      }
      const ozAlertEnabled = document.getElementById('setting-ozon_alerts_enabled');
      if (ozAlertEnabled) ozAlertEnabled.checked = String(data.ozon_alerts_enabled || '0') === '1';
      const ozAlertTg = document.getElementById('setting-ozon_alerts_telegram_enabled');
      if (ozAlertTg) ozAlertTg.checked = String(data.ozon_alerts_telegram_enabled || '1') !== '0';
      const ozAlertFrom = document.getElementById('setting-ozon_alerts_check_from_date');
      if (ozAlertFrom) ozAlertFrom.value = (data.ozon_alerts_check_from_date || '').slice(0, 10);
      const ozAlertTpl = document.getElementById('setting-ozon_alerts_telegram_template');
      if (ozAlertTpl) {
        ozAlertTpl.value = data.ozon_alerts_telegram_template || DEFAULT_OZON_ALERT_TELEGRAM_TEMPLATE;
      }
      const ozAlertTgChat = document.getElementById('setting-ozon_alerts_telegram_chat_id');
      if (ozAlertTgChat) ozAlertTgChat.value = data.ozon_alerts_telegram_chat_id || '';
      fillOzonAlertsStoreSelects();
      const prompts = await api('/prompts');
      renderPromptsList(prompts);
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function syncAutoScheduleModeUi() {
    const mode = document.getElementById('auto-schedule-mode')?.value || 'slots';
    const slotsWrap = document.getElementById('auto-slots-wrap');
    const intervalWrap = document.getElementById('auto-interval-wrap');
    if (slotsWrap) slotsWrap.style.display = mode === 'slots' ? 'block' : 'none';
    if (intervalWrap) intervalWrap.style.display = mode === 'interval' ? 'block' : 'none';
  }

  async function loadAutoSchedulePanel() {
    try {
      await ensureStoresLoaded();
      const autoCfg = await api('/auto-schedule');
      const autoEnabled = document.getElementById('auto-enabled');
      const autoSlots = document.getElementById('auto-slots');
      const autoMode = document.getElementById('auto-schedule-mode');
      const autoInt = document.getElementById('auto-interval-hours');
      const autoMpToggleMap = {
        'auto-run-reviews-wb': 'run_reviews_wb',
        'auto-run-reviews-yam': 'run_reviews_yam',
        'auto-run-reviews-ozon': 'run_reviews_ozon',
        'auto-run-questions-wb': 'run_questions_wb',
        'auto-run-questions-yam': 'run_questions_yam',
        'auto-run-questions-ozon': 'run_questions_ozon',
        'auto-run-wb-chats': 'run_wb_chats',
        'auto-run-ozon-chats': 'run_ozon_chats',
      };
      if (autoEnabled) autoEnabled.checked = !!autoCfg.enabled;
      if (autoSlots) autoSlots.value = (autoCfg.slots || []).join(', ');
      if (autoMode) autoMode.value = autoCfg.schedule_mode || 'slots';
      if (autoInt) autoInt.value = String(autoCfg.interval_hours || 1);
      Object.entries(autoMpToggleMap).forEach(([elId, cfgKey]) => {
        const el = document.getElementById(elId);
        if (el) {
          const legacy = cfgKey.startsWith('run_reviews')
            ? !!autoCfg.run_reviews
            : cfgKey.startsWith('run_questions')
              ? !!autoCfg.run_questions
              : false;
          el.checked = autoCfg[cfgKey] != null ? !!autoCfg[cfgKey] : legacy;
        }
      });
      const autoRunOzonAlerts = document.getElementById('auto-run-ozon-alerts');
      if (autoRunOzonAlerts) autoRunOzonAlerts.checked = !!autoCfg.run_ozon_alerts;
      const autoRunOzonActions = document.getElementById('auto-run-ozon-actions-remove');
      if (autoRunOzonActions) autoRunOzonActions.checked = !!autoCfg.run_ozon_actions_remove;
      syncAutoScheduleModeUi();
      renderAutoStoreList(autoCfg.store_ids || []);
      await refreshAutoStatus();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function wireUiPrefs() {
    const uiCompact = document.getElementById('ui-compact');
    const uiDim = document.getElementById('ui-dim-bg');
    const uiReduce = document.getElementById('ui-reduce-motion');
    const uiBgMotion = document.getElementById('ui-bg-motion');
    const uiThemeDark = document.getElementById('theme-dark');
    const uiToast = document.getElementById('ui-toast-duration');
    const uiConfirm = document.getElementById('ui-confirm-danger');
    const uiReset = document.getElementById('ui-reset');
    if (uiCompact) uiCompact.addEventListener('change', () => { localStorage.setItem(STORAGE_UI_COMPACT, uiCompact.checked ? '1' : '0'); applyUiPrefs(); });
    if (uiDim) uiDim.addEventListener('change', () => { localStorage.setItem(STORAGE_UI_DIM_BG, uiDim.checked ? '1' : '0'); applyUiPrefs(); });
    if (uiReduce) uiReduce.addEventListener('change', () => { localStorage.setItem(STORAGE_UI_REDUCE_MOTION, uiReduce.checked ? '1' : '0'); applyUiPrefs(); });
    if (uiBgMotion) uiBgMotion.addEventListener('change', () => { localStorage.setItem(STORAGE_UI_BG_MOTION, uiBgMotion.checked ? '1' : '0'); applyUiPrefs(); syncBgParallaxListener(); });
    if (uiThemeDark) uiThemeDark.addEventListener('change', () => {
      localStorage.setItem(STORAGE_UI_THEME, uiThemeDark.checked ? 'dark' : 'light');
      applyUiPrefs();
      document.dispatchEvent(new CustomEvent('marketai-theme-change', { detail: { dark: uiThemeDark.checked } }));
      if (window.MarketAIFx && window.MarketAIFx.syncLampVisual) {
        window.MarketAIFx.syncLampVisual(document.getElementById('fx-lamp'));
      }
    });
    if (uiToast) uiToast.addEventListener('change', () => { localStorage.setItem(STORAGE_UI_TOAST_MS, String(parseInt(uiToast.value || '4000', 10) || 4000)); });
    if (uiConfirm) uiConfirm.addEventListener('change', () => { localStorage.setItem(STORAGE_UI_CONFIRM_DANGER, uiConfirm.checked ? '1' : '0'); });
    if (uiReset) uiReset.addEventListener('click', () => {
      try {
        localStorage.removeItem(STORAGE_UI_COMPACT);
        localStorage.removeItem(STORAGE_UI_DIM_BG);
        localStorage.removeItem(STORAGE_UI_REDUCE_MOTION);
        localStorage.removeItem(STORAGE_UI_BG_MOTION);
        localStorage.removeItem(STORAGE_UI_THEME);
        localStorage.removeItem(STORAGE_UI_TOAST_MS);
        localStorage.removeItem(STORAGE_UI_CONFIRM_DANGER);
      } catch (_) {}
      applyUiPrefs();
      syncBgParallaxListener();
      toast('UI сброшен');
      loadSettings();
    });
  }

  const btnSaveAuto = document.getElementById('btn-save-auto');
  if (btnSaveAuto) {
    btnSaveAuto.addEventListener('click', async () => {
      const enabled = !!document.getElementById('auto-enabled')?.checked;
      const slotsRaw = (document.getElementById('auto-slots')?.value || '').trim();
      const slots = slotsRaw ? slotsRaw.split(',').map(x => x.trim()).filter(Boolean) : [];
      const schedule_mode = (document.getElementById('auto-schedule-mode')?.value || 'slots');
      const interval_hours = parseInt(document.getElementById('auto-interval-hours')?.value || '1', 10) || 1;
      const run_reviews_wb = !!document.getElementById('auto-run-reviews-wb')?.checked;
      const run_reviews_yam = !!document.getElementById('auto-run-reviews-yam')?.checked;
      const run_reviews_ozon = !!document.getElementById('auto-run-reviews-ozon')?.checked;
      const run_questions_wb = !!document.getElementById('auto-run-questions-wb')?.checked;
      const run_questions_yam = !!document.getElementById('auto-run-questions-yam')?.checked;
      const run_questions_ozon = !!document.getElementById('auto-run-questions-ozon')?.checked;
      const run_wb_chats = !!document.getElementById('auto-run-wb-chats')?.checked;
      const run_ozon_chats = !!document.getElementById('auto-run-ozon-chats')?.checked;
      const run_ozon_alerts = !!document.getElementById('auto-run-ozon-alerts')?.checked;
      const run_ozon_actions_remove = !!document.getElementById('auto-run-ozon-actions-remove')?.checked;
      const store_ids = getAutoSelectedStoreIds();
      if (!store_ids.length) {
        toast('Выбери хотя бы один магазин для автозапуска', 'error');
        return;
      }
      const anyTask = run_reviews_wb || run_reviews_yam || run_reviews_ozon
        || run_questions_wb || run_questions_yam || run_questions_ozon
        || run_wb_chats || run_ozon_chats || run_ozon_alerts || run_ozon_actions_remove;
      if (!anyTask) {
        toast('Выбери хотя бы одну задачу в блоках WB / ЯМ / Ozon', 'error');
        return;
      }
      setButtonBusy(btnSaveAuto, true, 'Сохранение…');
      try {
        await api('/auto-schedule', {
          method: 'POST',
          body: JSON.stringify({
            enabled, slots, store_ids, schedule_mode, interval_hours,
            run_reviews_wb, run_reviews_yam, run_reviews_ozon,
            run_questions_wb, run_questions_yam, run_questions_ozon,
            run_wb_chats, run_ozon_chats, run_ozon_alerts, run_ozon_actions_remove,
          }),
        });
        toast('Автозапуск сохранён');
        await loadAutoSchedulePanel();
      } catch (err) {
        toast(err.message, 'error');
      } finally {
        setButtonBusy(btnSaveAuto, false);
      }
    });
  }

  const autoModeEl = document.getElementById('auto-schedule-mode');
  if (autoModeEl) autoModeEl.addEventListener('change', syncAutoScheduleModeUi);

  const btnRunAutoNow = document.getElementById('btn-run-auto-now');
  if (btnRunAutoNow) {
    btnRunAutoNow.addEventListener('click', async () => {
      if (!confirmDanger('Запустить цикл автозапуска сейчас (без ожидания расписания)?')) return;
      btnRunAutoNow.disabled = true;
      try {
        const r = await api('/auto-schedule/run-now', { method: 'POST', body: JSON.stringify({}) });
        toast(r && r.started ? 'Автозапуск запущен' : 'Запуск не начался');
        refreshAutoStatus();
      } catch (err) {
        toast(err.message, 'error');
      } finally {
        btnRunAutoNow.disabled = false;
      }
    });
  }

  const btnStopAuto = document.getElementById('btn-stop-auto');
  if (btnStopAuto) {
    btnStopAuto.addEventListener('click', async () => {
      try {
        const r = await api('/auto-schedule/stop', { method: 'POST', body: JSON.stringify({}) });
        const autoEnabled = document.getElementById('auto-enabled');
        if (autoEnabled) autoEnabled.checked = false;
        if (r && r.stopped) toast('Текущий цикл остановлен, автозапуск выключен');
        else toast('Автозапуск выключен');
        refreshAutoStatus();
      } catch (err) {
        toast(err.message, 'error');
      }
    });
  }

  const autoEnabledToggle = document.getElementById('auto-enabled');
  if (autoEnabledToggle) {
    autoEnabledToggle.addEventListener('change', async () => {
      if (autoEnabledToggle.checked) return;
      try {
        await api('/auto-schedule/disable', { method: 'POST', body: JSON.stringify({}) });
        toast('Автозапуск выключен');
        await refreshAutoStatus();
      } catch (err) {
        autoEnabledToggle.checked = true;
        toast(err.message, 'error');
      }
    });
  }

  function wireConfigBackup() {
    if (wireConfigBackup._done) return;
    wireConfigBackup._done = true;
    const btnExport = document.getElementById('btn-config-export');
    const btnImport = document.getElementById('btn-config-import');
    const fileInput = document.getElementById('config-import-file');

    if (btnExport) {
      btnExport.addEventListener('click', async () => {
        btnExport.disabled = true;
        try {
          const base = getApiBase();
          const url = (base ? base + '/api/config/export' : API + '/config/export');
          const res = await fetch(url, { credentials: 'include' });
          if (!res.ok) {
            const text = await res.text();
            let err;
            try { err = JSON.parse(text); } catch (_) { err = { detail: text }; }
            throw new Error(err.detail || res.statusText || 'Ошибка выгрузки');
          }
          const blob = await res.blob();
          const cd = res.headers.get('Content-Disposition') || '';
          let filename = 'wb-autoreply-config.json';
          const m = /filename="([^"]+)"/.exec(cd);
          if (m) filename = m[1];
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = filename;
          document.body.appendChild(a);
          a.click();
          a.remove();
          URL.revokeObjectURL(a.href);
          toast('Настройки выгружены (без API-ключей)');
        } catch (err) {
          toast(err.message || String(err), 'error');
        } finally {
          btnExport.disabled = false;
        }
      });
    }

    if (btnImport && fileInput) {
      btnImport.addEventListener('click', () => fileInput.click());
      fileInput.addEventListener('change', async () => {
        const file = fileInput.files && fileInput.files[0];
        fileInput.value = '';
        if (!file) return;
        if (!confirmDanger('Загрузить настройки из файла? Существующие магазины с тем же именем будут обновлены.')) return;
        btnImport.disabled = true;
        try {
          const text = await file.text();
          let data;
          try {
            data = JSON.parse(text);
          } catch (_) {
            throw new Error('Файл не является корректным JSON');
          }
          const res = await api('/config/import', {
            method: 'POST',
            body: JSON.stringify({ data }),
          });
          const parts = [];
          if (res.stores_created) parts.push(`магазинов добавлено: ${res.stores_created}`);
          if (res.stores_updated) parts.push(`обновлено: ${res.stores_updated}`);
          if (res.settings_count) parts.push(`настроек: ${res.settings_count}`);
          if (res.prompts_added || res.prompts_updated) {
            parts.push(`промптов +${res.prompts_added || 0}/~${res.prompts_updated || 0}`);
          }
          if (res.store_errors && res.store_errors.length) {
            toast((parts.join(', ') || 'Загружено') + '. Ошибки: ' + res.store_errors.join('; '), 'error');
          } else {
            toast(parts.length ? 'Загружено: ' + parts.join(', ') : 'Настройки загружены');
          }
          await loadSettings();
          await reloadStoresIntoSelects();
          if (document.getElementById('panel-stores')?.classList.contains('active')) {
            await loadStores();
          }
        } catch (err) {
          toast(err.message || String(err), 'error');
        } finally {
          btnImport.disabled = false;
        }
      });
    }
  }

  function wireSettingsPanel() {
    if (wireSettingsPanel._done) return;
    wireSettingsPanel._done = true;
    document.querySelectorAll('#settings-filter [data-settings-section]').forEach(btn => {
      btn.addEventListener('click', () => {
        const sec = btn.getAttribute('data-settings-section') || 'connection';
        if (sec === settingsSection) return;
        settingsSection = sec;
        syncSettingsSectionUI();
        setNavActive('settings', { settingsSection: sec });
      });
    });
    document.querySelectorAll('.link-btn[data-settings-section]').forEach(btn => {
      btn.addEventListener('click', () => {
        const sec = btn.getAttribute('data-settings-section') || 'connection';
        settingsSection = sec;
        syncSettingsSectionUI();
        setNavActive('settings', { settingsSection: sec });
      });
    });
  }

  async function saveServerSettings() {
    const body = {
      telegram_chat_id: document.getElementById('setting-telegram_chat_id').value,
      telegram_report_chat_id: document.getElementById('setting-telegram_report_chat_id')?.value || '',
      telegram_card_error_chat_id: document.getElementById('setting-telegram_card_error_chat_id')?.value || '',
      telegram_enabled: document.getElementById('setting-telegram_enabled')?.checked ? '1' : '0',
      telegram_report_enabled: document.getElementById('setting-telegram_report_enabled')?.checked ? '1' : '0',
      telegram_report_interval: document.getElementById('setting-telegram_report_interval')?.value === 'day' ? 'day' : 'hour',
      telegram_agent_enabled: document.getElementById('setting-telegram_agent_enabled')?.checked ? '1' : '0',
      telegram_agent_chat_id: document.getElementById('setting-telegram_agent_chat_id')?.value || '',
      telegram_agent_user_id: document.getElementById('setting-telegram_agent_user_id')?.value || '',
      card_check_enabled: document.getElementById('setting-card_check_enabled')?.checked ? '1' : '0',
      card_check_telegram_enabled: document.getElementById('setting-card_check_telegram_enabled')?.checked ? '1' : '0',
      card_check_include_in_periodic_report: document.getElementById('setting-card_check_include_in_periodic_report')?.checked ? '1' : '0',
      card_check_telegram_template: document.getElementById('setting-card_check_telegram_template')?.value || '',
      ozon_alerts_enabled: document.getElementById('setting-ozon_alerts_enabled')?.checked ? '1' : '0',
      ozon_alerts_telegram_enabled: document.getElementById('setting-ozon_alerts_telegram_enabled')?.checked ? '1' : '0',
      ozon_alerts_check_from_date: document.getElementById('setting-ozon_alerts_check_from_date')?.value || '',
      ozon_alerts_telegram_template: document.getElementById('setting-ozon_alerts_telegram_template')?.value || '',
      ozon_alerts_telegram_chat_id: document.getElementById('setting-ozon_alerts_telegram_chat_id')?.value || '',
      buyer_chat_reply_from_date: document.getElementById('setting-buyer_chat_reply_from_date')?.value || '',
      buyer_chat_auto_max_age_days: String(parseInt(document.getElementById('setting-buyer_chat_auto_max_age_days')?.value || '3', 10) || 3),
    };
    const openaiKey = (document.getElementById('setting-openai_key')?.value || '').trim();
    if (openaiKey) body.openai_key = openaiKey;
    const tgToken = (document.getElementById('setting-telegram_bot_token')?.value || '').trim();
    if (tgToken) body.telegram_bot_token = tgToken;
    await api('/settings', { method: 'POST', body: JSON.stringify(body) });
    await loadSettings();
    toast('Сохранено');
  }

  document.querySelectorAll('.btn-save-server-settings').forEach(btn => {
    btn.addEventListener('click', async () => {
      setButtonBusy(btn, true, 'Сохранение…');
      try {
        await saveServerSettings();
      } catch (err) {
        toast(err.message, 'error');
      } finally {
        setButtonBusy(btn, false);
      }
    });
  });

  const btnTgTest = document.getElementById('btn-telegram-test');
  if (btnTgTest) {
    btnTgTest.addEventListener('click', async () => {
      btnTgTest.disabled = true;
      try {
        const res = await api('/telegram/test', {
          method: 'POST',
          body: JSON.stringify({
            telegram_bot_token: document.getElementById('setting-telegram_bot_token')?.value || '',
            telegram_chat_id:
              document.getElementById('setting-telegram_report_chat_id')?.value
              || document.getElementById('setting-telegram_chat_id')?.value
              || '',
          }),
        });
        toast(res.message || 'Тест Telegram: OK');
      } catch (err) {
        toast(err.message, 'error');
      } finally {
        btnTgTest.disabled = false;
      }
    });
  }

  const btnTgReportNow = document.getElementById('btn-telegram-report-now');
  if (btnTgReportNow) {
    btnTgReportNow.addEventListener('click', async () => {
      btnTgReportNow.disabled = true;
      try {
        const res = await api('/telegram/report-now', { method: 'POST' });
        toast(
          `Отчёт отправлен: отзывы ${res.reviews_sent || 0}, вопросы ${res.questions_sent || 0}, `
          + `чаты ${res.chat_replies_total || 0}, документы Ozon ${res.ozon_cert_requests_products || 0}, `
          + `скрытия ${res.ozon_hidden_products || 0}, акции −${res.ozon_products_removed || 0}`,
        );
      } catch (err) {
        toast(err.message, 'error');
      } finally {
        btnTgReportNow.disabled = false;
      }
    });
  }

  // ---- Log ----
  function safeJsonParse(s) {
    try { return JSON.parse(s); } catch (_) { return null; }
  }

  function actionRu(a) {
    const m = {
      load_new: 'Загрузка новых',
      generate: 'Генерация',
      send: 'Отправка',
      template_apply: 'Шаблон',
      auto_run: 'Автозапуск',
      auto_run_skipped: 'Автозапуск: пропуск слота',
      store_auto: 'Автозапуск: магазин',
      store_wb_chats_auto: 'Автозапуск: чаты WB',
      store_ozon_chats_auto: 'Автозапуск: чаты Ozon',
      store_ozon_alerts_auto: 'Автозапуск: уведомления Ozon',
      ozon_alert_detected: 'Ozon: важное уведомление',
      ozon_actions_auto_remove: 'Акции Ozon: автоудаление',
      ozon_actions_remove: 'Акции Ozon: удаление',
      wb_buyer_chat_generate: 'Чат WB: генерация',
      wb_buyer_chat_send: 'Чат WB: отправка',
      wb_buyer_chat_mass_send: 'Чат WB: массово ИИ+отправка',
      ozon_buyer_chat_generate: 'Чат Ozon: генерация',
      ozon_buyer_chat_send: 'Чат Ozon: отправка',
      ozon_buyer_chat_mass_send: 'Чат Ozon: массово ИИ+отправка',
      telegram_report: 'Telegram: отчёт',
      card_error_detected: 'Ошибка в карточке',
    };
    return m[a] || a || '—';
  }

  function formatStoreAutoLine(r) {
    if (!r || typeof r !== 'object') return '';
    const parts = [`маг. ${r.store_id} (${r.marketplace || '?'})`];
    if (r.added || r.candidates || r.sent_ok) {
      parts.push(`отзывы/вопросы: +${r.added || 0}, к ответу ${r.candidates || 0}, отправлено ${r.sent_ok || 0}`);
    }
    const wb = r.wb_chats;
    if (wb && (wb.wb_chat_sent || wb.wb_chat_candidates)) {
      parts.push(`чаты WB: ${wb.wb_chat_sent || 0} отв.`);
    }
    const oz = r.ozon_chats;
    if (oz && (oz.ozon_chat_sent || oz.ozon_chat_candidates)) {
      parts.push(`чаты Ozon: ${oz.ozon_chat_sent || 0} отв.`);
    }
    const ozAl = r.ozon_alerts;
    if (ozAl && (Number(ozAl.ozon_alert_new) || Number(ozAl.ozon_alert_chats_scanned))) {
      parts.push(`уведомл. Ozon: ${ozAl.ozon_alert_new || 0} важн., чатов ${ozAl.ozon_alert_chats_scanned || 0}`);
    }
    const oa = r.ozon_actions;
    if (oa) {
      if (oa.skipped) {
        parts.push(`акции: пропуск (${oa.reason || oa.message || '—'})`);
      } else {
        parts.push(`акции: ${oa.products_removed || 0} товаров из ${oa.actions_processed || 0} акций (подошло ${oa.actions_matched || 0})`);
      }
    }
    if (r.card_errors) parts.push(`ошибок карточек: ${r.card_errors}`);
    if (r.reviews_phase_error) parts.push(`ошибка отзывов: ${r.reviews_phase_error}`);
    return parts.join(' · ');
  }

  function formatOpsLogSummary(action, meta) {
    if (!meta || typeof meta !== 'object') return '';
    if (action === 'auto_run_skipped') {
      return `Слот ${meta.slot || '—'} не запущен: ${meta.reason === 'previous_run_still_running' ? 'предыдущий цикл ещё идёт' : (meta.reason || '—')}${meta.current_store_id ? ` (был на маг. ${meta.current_store_id})` : ''}`;
    }
    if (action === 'ozon_actions_auto_remove' || action === 'ozon_actions_remove') {
      if (meta.skipped) {
        return `Пропуск: ${meta.message || meta.reason || 'нет доступа'}`;
      }
      const errs = Array.isArray(meta.errors) && meta.errors.length ? `, ошибок ${meta.errors.length}` : '';
      return `Акций: подошло ${meta.actions_matched ?? 0}, обработано ${meta.actions_processed ?? 0}, удалено товаров ${meta.products_removed ?? 0}, отклонено ${meta.products_rejected ?? 0}${errs}`;
    }
    if (action === 'auto_run') {
      const lines = [];
      const slot = meta.slot || '—';
      lines.push(`Слот ${slot} · магазинов ${meta.stores_processed ?? (meta.store_ids || []).length}`);
      const rev = [];
      if (meta.added != null) rev.push(`загружено ${meta.added}`);
      if (meta.candidates != null) rev.push(`к ответу ${meta.candidates}`);
      if (meta.gen_ok != null) rev.push(`сгенерировано ${meta.gen_ok}`);
      if (meta.card_errors) rev.push(`ошибок карточек ${meta.card_errors}`);
      if (meta.sent_ok != null) rev.push(`отправлено ${meta.sent_ok}`);
      if (meta.sent_failed) rev.push(`ошибок отправки ${meta.sent_failed}`);
      if (rev.length) lines.push('Отзывы/вопросы: ' + rev.join(', '));
      if (meta.run_wb_chats) lines.push(`Чаты WB: отправлено ${meta.wb_chat_sent ?? 0}`);
      if (meta.run_ozon_chats) lines.push(`Чаты Ozon: отправлено ${meta.ozon_chat_sent ?? 0}`);
      if (meta.run_ozon_alerts) lines.push(`Уведомления Ozon: важных ${meta.ozon_alert_new ?? 0}`);
      if (Array.isArray(meta.reviews_phase_errors) && meta.reviews_phase_errors.length) {
        lines.push('Ошибки отзывов/вопросов: ' + meta.reviews_phase_errors.join('; '));
      }
      if (meta.run_ozon_actions_remove) {
        const oa = meta.ozon_actions_totals || {};
        lines.push(
          `Акции Ozon: удалено ${oa.products_removed ?? 0} товаров из ${oa.actions_processed ?? 0} акций`
          + ` (магазинов с удалением: ${oa.stores_with_removals ?? 0}, пропущено: ${oa.stores_skipped ?? 0})`,
        );
      }
      const perStore = (meta.stores_results || []).map(formatStoreAutoLine).filter(Boolean);
      if (perStore.length) lines.push('По магазинам: ' + perStore.join(' | '));
      return lines.join('\n');
    }
    if (action === 'store_auto' && meta.summary) return String(meta.summary);
    if (action === 'store_ozon_alerts_auto') {
      const heur = [];
      if (meta.ozon_alert_heuristic_ignored) heur.push(`без ИИ пропущено ${meta.ozon_alert_heuristic_ignored}`);
      if (meta.ozon_alert_heuristic_important) heur.push(`без ИИ важных ${meta.ozon_alert_heuristic_important}`);
      if (meta.ozon_alert_ai_calls) heur.push(`ИИ ${meta.ozon_alert_ai_calls}`);
      const heurPart = heur.length ? ` (${heur.join(', ')})` : '';
      return `Ozon уведомления: новых ${meta.ozon_alert_new ?? 0}, чатов ${meta.ozon_alert_chats_scanned ?? 0}${heurPart}`;
    }
    if (action === 'store_wb_chats_auto' || action === 'store_ozon_chats_auto') {
      const sent = meta.wb_chat_sent ?? meta.ozon_chat_sent ?? 0;
      const cand = meta.wb_chat_candidates ?? meta.ozon_chat_candidates ?? 0;
      if (meta.ozon_chat_skip_reason || meta.message) {
        return `Пропуск: ${meta.ozon_chat_skip_reason || meta.message}`;
      }
      if (meta.reason) return `Пропуск: ${meta.reason}`;
      if (meta.wb_chat_events_failed) return 'Ошибка загрузки ленты событий WB (429 или API)';
      const parts = [`Отправлено ${sent}, кандидатов ${cand}`];
      const old = meta.wb_chat_skipped_too_old ?? 0;
      const ncl = meta.wb_chat_skipped_last_not_client ?? 0;
      if (old) parts.push(`старше лимита: ${old}`);
      if (ncl) parts.push(`не от покупателя: ${ncl}`);
      return parts.join('; ');
    }
    if (action === 'ozon_alert_detected') {
      const m = meta || {};
      return `Ozon: ${m.threat_type || 'важное'} — ${m.summary || m.amount || ''}`.trim();
    }
    if (action === 'card_error_detected') {
      const parts = [];
      if (meta.error_kind) parts.push(meta.error_kind);
      if (meta.product_title) parts.push(meta.product_title);
      if (meta.customer_text_preview) parts.push('«' + String(meta.customer_text_preview).slice(0, 120) + '…»');
      return parts.join(' · ') || '—';
    }
    if (action === 'telegram_report') {
      const intervalRu = meta.interval === 'day' ? 'за сутки' : 'за час';
      let line = `${intervalRu}: отзывы ${meta.reviews_sent ?? 0}, вопросы ${meta.questions_sent ?? 0}, `
        + `чаты ${meta.chat_replies_total ?? 0} (WB ${meta.wb_chat_replies ?? 0}, Ozon ${meta.ozon_chat_replies ?? 0}), `
        + `уведомл. Ozon ${meta.ozon_alerts ?? 0}, удалено с акций ${meta.ozon_products_removed ?? 0}`;
      if (meta.interval === 'day' && meta.reviews_by_rating && typeof meta.reviews_by_rating === 'object') {
        const stars = Object.keys(meta.reviews_by_rating)
          .map((k) => parseInt(k, 10))
          .filter((n) => n >= 1 && parseInt(meta.reviews_by_rating[n] || meta.reviews_by_rating[String(n)], 10) > 0)
          .sort((a, b) => b - a);
        if (stars.length) {
          line += '\nПо оценкам: ' + stars.map((s) => `⭐${s}=${meta.reviews_by_rating[s] ?? meta.reviews_by_rating[String(s)]}`).join(', ');
        }
      }
      if (meta.card_errors != null) line += `, ошибки карточек ${meta.card_errors}`;
      return line;
    }
    if (action === 'wb_buyer_chat_send' || action === 'ozon_buyer_chat_send') {
      const parts = [];
      if (meta.source) parts.push(meta.source === 'auto' ? 'авто' : 'вручную');
      if (meta.chat_id) parts.push(`чат ${meta.chat_id}`);
      if (meta.product_title) parts.push(meta.product_title);
      if (meta.message_preview) {
        const p = String(meta.message_preview);
        parts.push('«' + (p.length > 160 ? p.slice(0, 160) + '…' : p) + '»');
      }
      return parts.join(' · ') || '—';
    }
    if (meta.applied != null) return `Шаблон: применено ${meta.applied}, пропущено ${meta.skipped ?? 0}`;
    if (meta.sent_ok != null) {
      return `Отправка: ok ${meta.sent_ok}, без текста ${meta.sent_skipped ?? meta.skipped ?? 0}, ошибок ${meta.sent_failed ?? meta.failed ?? 0}`;
    }
    if (meta.ok != null) return `Генерация: ok ${meta.ok}, ошибок ${meta.failed ?? 0}`;
    if (meta.added != null) return `Загрузка: добавлено ${meta.added}`;
    if (meta.error) return String(meta.error).slice(0, 500);
    return '';
  }

  async function loadLog() {
    const mode = (document.getElementById('log-mode')?.value || 'ops');
    const action = (document.getElementById('log-action')?.value || '').trim();
    const storeId = (document.getElementById('log-store')?.value || '').trim();
    const level = (document.getElementById('log-level')?.value || '').trim();
    const q = (document.getElementById('log-q')?.value || '').trim();
    const devPre = document.getElementById('log-content');
    const opsWrap = document.getElementById('ops-log-wrap');
    if (devPre) devPre.style.display = (mode === 'dev') ? 'block' : 'none';
    if (opsWrap) opsWrap.style.display = (mode === 'ops') ? 'block' : 'none';
    try {
      if (mode === 'dev') {
        const data = await api('/log/dev?limit=600' + (level ? '&level=' + encodeURIComponent(level) : '') + (action ? '&action=' + encodeURIComponent(action) : '') + (q ? '&q=' + encodeURIComponent(q) : ''));
        devPre.textContent = (data.lines || []).join('\n');
        return;
      }

      try { await ensureStoresLoaded(); } catch (_) { /* журнал без списка магазинов */ }
      fillStoreSelects();
      let opsUrl = '/log/ops?limit=300';
      if (action) opsUrl += '&action=' + encodeURIComponent(action);
      if (storeId) opsUrl += '&store_id=' + encodeURIComponent(storeId);
      if (q) opsUrl += '&q=' + encodeURIComponent(q);
      const data = await api(opsUrl);
      const items = data.items || [];
      if (!items.length) {
        opsWrap.innerHTML = '<div class="empty-state" style="padding:24px;">Нет событий</div>';
        return;
      }
      const rowsHtml = items.map(ev => {
        const meta = safeJsonParse(ev.meta_json || '') || {};
        const storeName = meta.store_name || getStoreNameById(ev.store_id) || (ev.store_id ? `ID ${ev.store_id}` : '—');
        const summary = formatOpsLogSummary(ev.action, meta);
        const result = ev.result || 'ok';
        const resultCls = result === 'ok' ? 'ops-log-result-ok' : (result === 'error' ? 'ops-log-result-err' : 'ops-log-result-other');
        const detail = escapeHtml(summary || '—').replace(/\n/g, '<br>');
        const itemIds = Array.isArray(meta.item_ids) ? meta.item_ids : [];
        const extraBtn = itemIds.length
          ? `<button type="button" class="btn btn-secondary btn-sm" data-ops-show="${ev.id}">Ответы (${Math.min(itemIds.length, 20)})</button>`
          : '';
        return `<tr>
          <td class="ops-log-ts">${escapeHtml(ev.ts || '')}</td>
          <td class="ops-log-action">${escapeHtml(actionRu(ev.action))}</td>
          <td class="ops-log-store">${escapeHtml(storeName)}</td>
          <td class="ops-log-result"><span class="ops-log-badge ${resultCls}">${escapeHtml(result)}</span></td>
          <td class="ops-log-detail"><div class="ops-log-summary">${detail}</div>${extraBtn ? `<div class="ops-log-extra">${extraBtn}</div>` : ''}<div class="ops-items" id="ops-items-${ev.id}" style="display:none; margin-top:8px;"></div></td>
        </tr>`;
      }).join('');
      opsWrap.innerHTML = `<table class="ops-log-table"><thead><tr>
        <th>Время</th><th>Действие</th><th>Магазин</th><th>Итог</th><th>Детали</th>
      </tr></thead><tbody>${rowsHtml}</tbody></table>`;
      opsWrap.querySelectorAll('[data-ops-show]').forEach(btn => {
        btn.addEventListener('click', async () => {
          const evId = Number(btn.getAttribute('data-ops-show'));
          const wrap = document.getElementById('ops-items-' + evId);
          if (!wrap) return;
          if (wrap.style.display === 'block') {
            wrap.style.display = 'none';
            btn.textContent = 'Показать ответы (20)';
            return;
          }
          btn.disabled = true;
          btn.textContent = 'Загружаю…';
          try {
            const ev = items.find(x => x.id === evId);
            const meta = safeJsonParse(ev?.meta_json || '') || {};
            const ids = (Array.isArray(meta.item_ids) ? meta.item_ids : []).slice(0, 20);
            const rows = await api('/items/bulk', { method: 'POST', body: JSON.stringify({ item_ids: ids }) });
            wrap.innerHTML = (rows || []).map(it => {
              const store = stores.find(s => s.id === it.store_id);
              const text = (it.text || '').slice(0, 180) + ((it.text || '').length > 180 ? '…' : '');
              const ans = (it.generated_text || '').slice(0, 180) + ((it.generated_text || '').length > 180 ? '…' : '');
              return `
                <div class="store-card" style="padding:14px; margin-bottom:10px;">
                  <div class="meta">${escapeHtml(store ? store.name : '—')} · ${escapeHtml(formatDate(it.date))}</div>
                  <div class="store-name" style="margin:6px 0;">${escapeHtml((it.product_title || '').trim() || '—')}</div>
                  <div class="text-preview" style="max-width:unset;"><b>${it.item_type === 'review' ? 'Текст:' : 'Вопрос:'}</b> ${escapeHtml(text || '—')}</div>
                  <div class="text-preview" style="max-width:unset; margin-top:6px;"><b>Ответ:</b> ${escapeHtml(ans || '—')}</div>
                  <div class="actions" style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
                    <button type="button" class="btn btn-secondary btn-sm" data-ops-open="${it.id}">Открыть полностью</button>
                  </div>
                </div>
              `;
            }).join('') || '<div class="empty-state" style="padding:16px;">Нет данных</div>';
            wrap.querySelectorAll('[data-ops-open]').forEach(b => {
              b.addEventListener('click', async () => {
                const id = Number(b.getAttribute('data-ops-open'));
                try {
                  const item = await api('/items/' + id);
                  const store = stores.find(s => s.id === item.store_id);
                  showItemModal(item, store ? store.name : '—', item.item_type === 'review');
                } catch (err) { toast(err.message, 'error'); }
              });
            });
            wrap.style.display = 'block';
            btn.textContent = 'Скрыть';
          } catch (err) {
            toast(err.message, 'error');
            btn.textContent = 'Показать ответы (20)';
          } finally {
            btn.disabled = false;
          }
        });
      });
    } catch (err) {
      if (mode === 'dev') devPre.textContent = 'Ошибка: ' + err.message;
      else opsWrap.innerHTML = '<div class="empty-state" style="padding:24px;">Ошибка: ' + escapeHtml(err.message) + '</div>';
    }
  }

  const CARD_ERROR_SOURCE_LABELS = {
    review: 'Отзыв',
    question: 'Вопрос',
    wb_chat: 'Чат WB',
    ozon_chat: 'Чат Ozon',
  };

  function fillOzonAlertsStoreSelects() {
    const oz = storesForMarketplace('ozon');
    const opts = oz.length
      ? oz.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('')
      : '<option value="">Нет магазинов Ozon</option>';
    const scanSel = document.getElementById('ozon-alerts-scan-store');
    if (scanSel) {
      const prev = String(scanSel.value || '').trim();
      scanSel.innerHTML = opts;
      if (prev && oz.some(s => String(s.id) === prev)) scanSel.value = prev;
      else if (oz.length) selectFirstStoreOption(scanSel);
    }
    const panelSel = document.getElementById('ozon-alerts-store');
    if (panelSel) {
      const prevP = String(panelSel.value || '').trim();
      panelSel.innerHTML = '<option value="">Все магазины</option>'
        + oz.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
      if (prevP && (prevP === '' || oz.some(s => String(s.id) === prevP))) panelSel.value = prevP;
    }
  }

  async function loadOzonAlerts() {
    const wrap = document.getElementById('ozon-alerts-wrap');
    if (!wrap) return;
    wrap.innerHTML = '<div class="form-hint">Загрузка…</div>';
    try {
      await ensureStoresLoaded();
      fillOzonAlertsStoreSelects();
    } catch (_) {}
    const storeId = (document.getElementById('ozon-alerts-store')?.value || '').trim();
    const status = (document.getElementById('ozon-alerts-status')?.value || '').trim();
    let url = '/ozon/alerts?limit=300';
    if (storeId) url += `&store_id=${encodeURIComponent(storeId)}`;
    if (status) {
      url += `&status=${encodeURIComponent(status)}&important_only=0`;
    }
    try {
      const rows = await api(url);
      if (!rows.length) {
        wrap.innerHTML = '<div class="empty-state empty-state--compact">Важных уведомлений пока нет. Включите проверку в «Настройки → Ozon» и нажмите «Проверить чаты».</div>';
        return;
      }
      wrap.innerHTML = `
        <table class="ops-log-table ozon-alerts-table">
          <thead><tr>
            <th>Время</th><th>Магазин</th><th>Категория</th><th>Тип</th><th>Сумма</th><th>Товар</th><th>Сводка</th><th>Статус</th>
          </tr></thead>
          <tbody>
            ${rows.map(r => {
              const stCls = r.status === 'new' ? 'ops-log-result-other' : r.status === 'resolved' ? 'ops-log-result-ok' : 'ops-log-result-err';
              const stLabel = r.status === 'new' ? 'новое' : r.status === 'resolved' ? 'обработано' : escapeHtml(r.status);
              const resolveBtn = r.status === 'new'
                ? `<button type="button" class="btn btn-secondary btn-sm" data-ozon-alert-resolve="${r.id}">Обработано</button>`
                : '';
              const tg = r.telegram_sent ? ' · TG ✓' : '';
              return `
              <tr>
                <td class="ops-log-ts">${escapeHtml(r.message_at_label || r.ts || '—')}</td>
                <td class="ops-log-store">${escapeHtml(r.store_name || r.store_id)}</td>
                <td>${ozonAlertCategoryBadge(r)}</td>
                <td>${escapeHtml(r.threat_type || '—')}</td>
                <td>${escapeHtml(r.amount || '—')}</td>
                <td>${escapeHtml(r.product_ref || '—')}</td>
                <td><div class="ops-log-summary">${escapeHtml(r.summary || '—')}</div><div class="form-hint ozon-alert-action">${escapeHtml(r.action_needed || '')}</div></td>
                <td><span class="ops-log-badge ${stCls}">${stLabel}</span><span class="form-hint">${tg}</span>${resolveBtn}</td>
              </tr>
              <tr class="ozon-alert-msg-row"><td colspan="8"><div class="ozon-alert-msg">${escapeHtml((r.message_text || '').slice(0, 600))}${(r.message_text || '').length > 600 ? '…' : ''}</div></td></tr>`;
            }).join('')}
          </tbody>
        </table>`;
      wrap.querySelectorAll('[data-ozon-alert-resolve]').forEach(btn => {
        btn.addEventListener('click', async () => {
          const id = Number(btn.getAttribute('data-ozon-alert-resolve'));
          try {
            await api('/ozon/alerts/' + id, { method: 'PATCH', body: JSON.stringify({ status: 'resolved' }) });
            toast('Отмечено как обработано');
            await loadOzonAlerts();
          } catch (err) {
            toast(err.message, 'error');
          }
        });
      });
    } catch (err) {
      wrap.innerHTML = `<div class="form-hint" style="color:#b91c1c;">${escapeHtml(err.message || 'Ошибка')}</div>`;
    }
  }

  async function loadCardErrors() {
    const wrap = document.getElementById('card-errors-wrap');
    if (!wrap) return;
    try {
      await ensureStoresLoaded();
      fillStoreSelects();
    } catch (_) { /* список магазинов опционален */ }
    const storeId = (document.getElementById('card-errors-store')?.value || '').trim();
    const status = (document.getElementById('card-errors-status')?.value || '').trim();
    let url = '/card-errors?limit=300';
    if (storeId) url += '&store_id=' + encodeURIComponent(storeId);
    if (status) url += '&status=' + encodeURIComponent(status);
    wrap.innerHTML = '<div class="form-hint panel-loading-hint">Загрузка…</div>';
    try {
      const items = await api(url);
      if (!items.length) {
        wrap.innerHTML = '<div class="empty-state empty-state--compact">Нет записей</div>';
        return;
      }
      const rows = items.map(row => {
        const storeName = row.store_name || getStoreNameById(row.store_id) || `ID ${row.store_id}`;
        const src = CARD_ERROR_SOURCE_LABELS[row.source_type] || row.source_type || '—';
        const statusCls = row.status === 'resolved' ? 'ops-log-result-ok' : 'ops-log-result-other';
        const tg = row.telegram_sent ? 'да' : 'нет';
        const text = escapeHtml((row.customer_text || '').slice(0, 300)) + ((row.customer_text || '').length > 300 ? '…' : '');
        const resolveBtn = row.status === 'new'
          ? `<button type="button" class="btn btn-secondary btn-sm" data-card-resolve="${row.id}">Отметить обработанным</button>`
          : '';
        return `<tr>
          <td class="ops-log-ts">${escapeHtml(row.ts || '')}</td>
          <td class="ops-log-store">${escapeHtml(storeName)}</td>
          <td>${escapeHtml(src)}</td>
          <td>${escapeHtml((row.product_title || '').slice(0, 80))}</td>
          <td><div class="ops-log-summary">${text}</div></td>
          <td>${escapeHtml(row.error_kind || '—')}<div class="form-hint" style="margin-top:4px;">${escapeHtml(row.explanation || '')}</div></td>
          <td><span class="ops-log-badge ${statusCls}">${escapeHtml(row.status === 'resolved' ? 'обработано' : 'новое')}</span><div class="form-hint">TG: ${tg}</div>${resolveBtn}</td>
        </tr>`;
      }).join('');
      wrap.innerHTML = `<table class="ops-log-table"><thead><tr>
        <th>Время</th><th>Магазин</th><th>Источник</th><th>Товар</th><th>Текст</th><th>Ошибка</th><th>Статус</th>
      </tr></thead><tbody>${rows}</tbody></table>`;
      wrap.querySelectorAll('[data-card-resolve]').forEach(btn => {
        btn.addEventListener('click', async () => {
          const id = Number(btn.getAttribute('data-card-resolve'));
          btn.disabled = true;
          try {
            await api('/card-errors/' + id, { method: 'PATCH', body: JSON.stringify({ status: 'resolved' }) });
            toast('Отмечено как обработанное');
            await loadCardErrors();
          } catch (err) {
            toast(err.message, 'error');
            btn.disabled = false;
          }
        });
      });
    } catch (err) {
      wrap.innerHTML = '<div class="empty-state empty-state--compact">Ошибка: ' + escapeHtml(err.message) + '</div>';
    }
  }

  document.getElementById('btn-refresh-card-errors')?.addEventListener('click', () => { void loadCardErrors(); });
  document.getElementById('btn-refresh-ozon-alerts')?.addEventListener('click', () => { void loadOzonAlerts(); });
  document.getElementById('ozon-alerts-store')?.addEventListener('change', () => { void loadOzonAlerts(); });
  document.getElementById('ozon-alerts-status')?.addEventListener('change', () => { void loadOzonAlerts(); });
  async function runOzonAlertsScan(storeId, { rescan = false } = {}) {
    const sid = String(storeId || '').trim();
    if (!sid) {
      toast('Выберите магазин Ozon', 'error');
      return;
    }
    if (rescan && !window.confirm('Сбросить пометки «не важно» для этого магазина и проверить сообщения заново?')) {
      return;
    }
    try {
      await saveServerSettings();
      toast(
        rescan
          ? 'Пересканирую чаты Ozon… это может занять несколько минут'
          : 'Сканирую чаты поддержки Ozon… это может занять несколько минут',
        'info',
      );
      const r = await api(`/ozon/alerts/${sid}/scan`, {
        method: 'POST',
        body: JSON.stringify({ rescan }),
        timeoutMs: 600000,
      });
      const cleared = Number(r.ozon_alert_ignored_cleared || 0);
      const clearedPart = rescan && cleared ? `, сброшено «не важно»: ${cleared}` : '';
      const saveAi = (r.ozon_alert_heuristic_ignored || 0) + (r.ozon_alert_heuristic_important || 0);
      const aiPart = saveAi ? `, без ИИ ${saveAi} (ИИ ${r.ozon_alert_ai_calls ?? 0})` : '';
      toast(
        `Готово: новых важных ${r.ozon_alert_new ?? 0}, проверено сообщений ${r.ozon_alert_messages_checked ?? 0}${aiPart}${clearedPart}`,
      );
      await loadOzonAlerts();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  document.getElementById('btn-ozon-alerts-scan-now')?.addEventListener('click', () => {
    void runOzonAlertsScan(document.getElementById('ozon-alerts-scan-store')?.value, { rescan: false });
  });
  document.getElementById('btn-ozon-alerts-rescan-now')?.addEventListener('click', () => {
    void runOzonAlertsScan(document.getElementById('ozon-alerts-scan-store')?.value, { rescan: true });
  });
  document.getElementById('btn-ozon-alerts-rescan-panel')?.addEventListener('click', () => {
    void runOzonAlertsScan(document.getElementById('ozon-alerts-store')?.value, { rescan: true });
  });
  document.getElementById('card-errors-store')?.addEventListener('change', () => { void loadCardErrors(); });
  document.getElementById('card-errors-status')?.addEventListener('change', () => { void loadCardErrors(); });

  document.getElementById('btn-refresh-log').addEventListener('click', loadLog);
  ['log-mode', 'log-action', 'log-store', 'log-level'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('change', () => { void loadLog(); });
  });
  const logQ = document.getElementById('log-q');
  if (logQ) {
    let logQTimer = null;
    logQ.addEventListener('input', () => {
      clearTimeout(logQTimer);
      logQTimer = setTimeout(() => { void loadLog(); }, 400);
    });
  }

  // ---- API base (для доступа с телефона к ПК) ----
  const apiBaseInput = document.getElementById('setting-api_base');
  if (apiBaseInput) {
    apiBaseInput.addEventListener('change', function () {
      const v = this.value.trim().replace(/\/$/, '');
      if (v) localStorage.setItem(STORAGE_API_BASE, v); else localStorage.removeItem(STORAGE_API_BASE);
      toast('Адрес API сохранён');
    });
  }

  // ---- Auth UI ----
  let currentUser = null;

  async function loadMe(silent = false) {
    try {
      const me = await api('/auth/me');
      currentUser = me;
      const label = document.getElementById('auth-user-label');
      if (label) label.textContent = `${me.username} (${me.role})`;
      const labelHeader = document.getElementById('auth-user-label-header');
      if (labelHeader) labelHeader.textContent = `${me.username} (${me.role})`;
      const headerRight = document.querySelector('.header-right');
      if (headerRight) headerRight.hidden = false;
      applyTabVisibility();
      await refreshUsersSection();
      return me;
    } catch (err) {
      currentUser = null;
      const label = document.getElementById('auth-user-label');
      if (label) label.textContent = '—';
      const labelHeader = document.getElementById('auth-user-label-header');
      if (labelHeader) labelHeader.textContent = '—';
      const headerRight = document.querySelector('.header-right');
      if (headerRight) headerRight.hidden = true;
      if (!silent) toast(err.message, 'error');
      return null;
    }
  }

  async function doLogout() {
    try {
      await api('/auth/logout', { method: 'POST', body: JSON.stringify({}) });
    } catch (_) {}
    currentUser = null;
    window.location.href = '/login';
  }

  const btnLogoutHeader = document.getElementById('btn-logout-header');
  if (btnLogoutHeader) btnLogoutHeader.addEventListener('click', doLogout);

  async function refreshUsersSection() {
    const adminWrap = document.getElementById('users-admin-only');
    const noAccess = document.getElementById('users-no-access');
    if (!adminWrap || !noAccess) return;
    const isAdmin = currentUser && currentUser.role === 'admin';
    adminWrap.style.display = isAdmin ? 'block' : 'none';
    noAccess.style.display = isAdmin ? 'none' : 'block';
    if (!isAdmin) return;
    try {
      const users = await api('/users');
      const list = document.getElementById('users-list');
      if (list) {
        list.innerHTML = users.map(u => {
          const perms = u.permissions || [];
          const isAdminUser = u.role === 'admin';
          const settingsChecked = isAdminUser || perms.includes('view_settings');
          const logChecked = isAdminUser || perms.includes('view_log');
          const opsLogChecked = isAdminUser || perms.includes('view_ops_log');
          const disablePerms = isAdminUser ? ' disabled title="У админа все права"' : '';
          return `
          <div class="store-card user-card">
            <div class="store-card-head">
              <h3>${escapeHtml(u.username)} ${rolePillHtml(u.role)}</h3>
            </div>
            <div class="user-perms">
              <label class="user-perm"><input type="checkbox" data-user-id="${u.id}" data-perm="view_settings" ${settingsChecked ? 'checked' : ''}${disablePerms}> Настройки</label>
              <label class="user-perm"><input type="checkbox" data-user-id="${u.id}" data-perm="view_log" ${logChecked ? 'checked' : ''}${disablePerms}> Лог</label>
              <label class="user-perm"><input type="checkbox" data-user-id="${u.id}" data-perm="view_ops_log" ${opsLogChecked ? 'checked' : ''}${disablePerms}> Операции</label>
            </div>
            <div class="actions">
              <button type="button" class="btn btn-danger btn-sm" data-user-del="${u.id}">Удалить</button>
            </div>
          </div>`;
        }).join('');
        list.querySelectorAll('[data-user-del]').forEach(btn => {
          btn.addEventListener('click', async () => {
            const id = Number(btn.getAttribute('data-user-del'));
            if (!confirmDanger('Удалить пользователя?')) return;
            await api('/users/' + id, { method: 'DELETE' });
            toast('Пользователь удалён');
            await refreshUsersSection();
          });
        });
        list.querySelectorAll('input[data-perm]').forEach(cb => {
          if (cb.disabled) return;
          cb.addEventListener('change', async () => {
            const userId = Number(cb.getAttribute('data-user-id'));
            const perm = cb.getAttribute('data-perm');
            const allCheckboxes = list.querySelectorAll(`input[data-user-id="${userId}"][data-perm]`);
            const permissions = Array.from(allCheckboxes).filter(c => c.checked).map(c => c.getAttribute('data-perm'));
            try {
              await api('/users/' + userId + '/permissions', { method: 'PATCH', body: JSON.stringify({ permissions }) });
              toast('Доступы обновлены');
            } catch (err) {
              toast(err.message, 'error');
              await refreshUsersSection();
            }
          });
        });
      }
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  // ---- AI agent ----
  let agentSessionId = null;
  let agentBusy = false;

  try {
    agentSessionId = localStorage.getItem('agentSessionId') || null;
  } catch (_) {}

  function escapeAgentHtml(text) {
    return String(text || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function renderAgentMessages(messages) {
    const wrap = document.getElementById('agent-messages');
    if (!wrap) return;
    if (!messages || !messages.length) {
      wrap.innerHTML = '<div class="agent-welcome"><p>Напишите, что нужно сделать. Для опасных действий (загрузка, генерация, отправка) ассистент запросит подтверждение.</p></div>';
      return;
    }
    wrap.innerHTML = messages.map(msg => {
      const role = msg.role === 'user' ? 'user' : 'assistant';
      return `<div class="agent-msg agent-msg--${role}">${escapeAgentHtml(msg.content)}</div>`;
    }).join('');
    wrap.scrollTop = wrap.scrollHeight;
  }

  function updateAgentPending(pending, needsConfirm) {
    const box = document.getElementById('agent-pending');
    const textEl = document.getElementById('agent-pending-text');
    if (!box || !textEl) return;
    const show = !!(pending && (needsConfirm || pending.summary));
    box.hidden = !show;
    if (show) {
      textEl.textContent = pending.summary || 'Подтвердите действие';
    }
  }

  async function sendAgentChat(opts = {}) {
    if (agentBusy) return;
    const input = document.getElementById('agent-input');
    const sendBtn = document.getElementById('btn-agent-send');
    let message = opts.message;
    if (message === undefined) {
      message = (input && input.value || '').trim();
    }
    const confirm = opts.confirm;
    if (confirm === undefined && !message) return;

    agentBusy = true;
    setButtonBusy(sendBtn, true, 'Думаю…');
    const wrap = document.getElementById('agent-messages');
    let typingEl = null;
    if (wrap && confirm === undefined && message) {
      typingEl = document.createElement('div');
      typingEl.className = 'agent-msg agent-msg--assistant agent-msg--typing';
      typingEl.textContent = 'Думаю…';
      wrap.appendChild(typingEl);
      wrap.scrollTop = wrap.scrollHeight;
    }

    try {
      const body = {
        message: message || '',
        session_id: agentSessionId,
      };
      if (confirm !== undefined) body.confirm = confirm;
      const data = await api('/agent/chat', {
        method: 'POST',
        body: JSON.stringify(body),
        timeoutMs: 120000,
      });
      agentSessionId = data.session_id || agentSessionId;
      try {
        if (agentSessionId) localStorage.setItem('agentSessionId', agentSessionId);
        else localStorage.removeItem('agentSessionId');
      } catch (_) {}
      renderAgentMessages(data.messages || []);
      updateAgentPending(data.pending, data.needs_confirm);
      if (input && confirm === undefined) input.value = '';
      if (data.tool_used && (data.tool_used === 'load_new_items' || data.tool_used === 'generate_answers' || data.tool_used === 'send_answers')) {
        loadStats();
        if (data.tool_used !== 'load_new_items') {
          loadReviews();
          loadQuestions();
        }
      }
    } catch (err) {
      if (typingEl) typingEl.remove();
      toast(err.message, 'error');
    } finally {
      agentBusy = false;
      setButtonBusy(sendBtn, false);
    }
  }

  function loadAgentPanel() {
    if (!agentSessionId) return;
    api('/agent/session/' + encodeURIComponent(agentSessionId))
      .then(data => {
        renderAgentMessages(data.messages || []);
        updateAgentPending(data.pending, !!data.pending);
      })
      .catch(() => {
        agentSessionId = null;
        try { localStorage.removeItem('agentSessionId'); } catch (_) {}
      });
  }

  function wireAgentPanel() {
    const form = document.getElementById('agent-form');
    const input = document.getElementById('agent-input');
    if (form) {
      form.addEventListener('submit', (e) => {
        e.preventDefault();
        sendAgentChat();
      });
    }
    if (input) {
      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          sendAgentChat();
        }
      });
    }
    const btnConfirm = document.getElementById('btn-agent-confirm');
    if (btnConfirm) {
      btnConfirm.addEventListener('click', () => sendAgentChat({ confirm: true, message: '' }));
    }
    const btnCancel = document.getElementById('btn-agent-cancel');
    if (btnCancel) {
      btnCancel.addEventListener('click', () => sendAgentChat({ confirm: false, message: '' }));
    }
    const btnClear = document.getElementById('btn-agent-clear');
    if (btnClear) {
      btnClear.addEventListener('click', async () => {
        if (agentSessionId) {
          try {
            await api('/agent/session/' + encodeURIComponent(agentSessionId), { method: 'DELETE' });
          } catch (_) {}
        }
        agentSessionId = null;
        try { localStorage.removeItem('agentSessionId'); } catch (_) {}
        renderAgentMessages([]);
        updateAgentPending(null, false);
        if (input) input.value = '';
        toast('Новый диалог');
      });
    }
    document.querySelectorAll('[data-agent-prompt]').forEach(btn => {
      btn.addEventListener('click', () => {
        const prompt = btn.getAttribute('data-agent-prompt') || '';
        if (input) input.value = prompt;
        sendAgentChat({ message: prompt });
      });
    });
  }

  const btnCreateUser = document.getElementById('btn-create-user');
  if (btnCreateUser) {
    btnCreateUser.addEventListener('click', async () => {
      const u = (document.getElementById('new-user-username').value || '').trim();
      const p = document.getElementById('new-user-password').value || '';
      if (!u || !p) {
        toast('Введите логин и пароль', 'error');
        return;
      }
      try {
        await api('/users', { method: 'POST', body: JSON.stringify({ username: u, password: p, role: 'guest' }) });
        document.getElementById('new-user-username').value = '';
        document.getElementById('new-user-password').value = '';
        toast('Пользователь создан');
        await refreshUsersSection();
      } catch (err) {
        toast(err.message, 'error');
      }
    });
  }

  // ---- Init ----
  loadMe(true).then(me => {
    migrateUiPrefsIfNeeded();
    applyUiPrefs();
    wireUiPrefs();
    wireSettingsPanel();
    wireConfigBackup();
    syncBgParallaxListener();
    ensureAutoStatusPolling();
    if (!me) {
      window.location.href = '/login';
      return;
    }
    loadStats();
    wireWbChatsPanel();
    wireOzonChatsPanel();
    wireOzonActionsPanel();
    wireCardLinksPanel();
    wireAgentPanel();
    ensureStoresLoaded().then(() => {
      fillStoreSelects();
      loadReviews();
      loadQuestions();
    }).catch((err) => {
      console.error('stores load failed', err);
      fillStoreSelects();
    });
  });
})();
