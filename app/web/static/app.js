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
      const baseHint = base ? `\nПроверь «Адрес API (ПК)»: сейчас = ${base}` : '';
      throw new Error('Не удалось подключиться к серверу (Failed to fetch).' + baseHint + '\nЧастые причины: неверный адрес API, CORS, смешанный контент (http/https), сервер спит на Render.');
    } finally {
      if (timer) clearTimeout(timer);
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
  let _qualityPollTimer = null;

  function syncQualityPoll(wbRows) {
    const pending = (wbRows || []).some((r) => {
      const err = r.error || '';
      return err.includes('загружается') || err.includes('обновлены позже');
    });
    if (pending && !_qualityPollTimer) {
      _qualityPollTimer = setInterval(() => {
        if (_qualityLoading) return;
        void loadQualityMetrics(false);
      }, 65000);
    } else if (!pending && _qualityPollTimer) {
      clearInterval(_qualityPollTimer);
      _qualityPollTimer = null;
    }
  }

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

  const QUALITY_WB_COLUMNS = [
    { key: 'review_rating', label: 'Рейтинг', title: 'Рейтинг по отзывам WB' },
  ];
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
      const wbRows = data.wb || [];
      renderQualityTable('quality-wb-stores', wbRows, QUALITY_WB_COLUMNS, 'Нет активных магазинов WB');
      syncQualityPoll(wbRows);
      const wbKeyWarn = document.getElementById('quality-wb-key-warning');
      if (wbKeyWarn) {
        const isAdmin = currentUser && currentUser.role === 'admin';
        const groups = Array.isArray(data.wb_key_groups) ? data.wb_key_groups : [];
        if (isAdmin && groups.length) {
          const lines = groups.map((g) => {
            const n = Number(g.count) || 0;
            const ids = (g.store_ids || []).join(', ');
            return `Обнаружено ${n} магазинов с одним WB API ключом (ID: ${ids}). Рейтинг продавца будет общий.`;
          });
          wbKeyWarn.textContent = lines.join(' ');
          wbKeyWarn.hidden = false;
        } else {
          wbKeyWarn.textContent = '';
          wbKeyWarn.hidden = true;
        }
      }
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
    wrap.classList.toggle('visible', !!visible);
    wrap.style.display = visible ? 'block' : '';
    if (message) {
      const t = wrap.querySelector('.progress-text');
      if (t) t.textContent = message;
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
          <div class="meta">ID ${s.id}${s.active ? '' : ' · неактивен'}</div>
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
        textEl.textContent = `${base} — ${pct}% (${cur}/${total})${detail ? ' · ' + detail : ''}`;
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
      [
        'openai_key',
        'telegram_bot_token',
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
    if (uiThemeDark) uiThemeDark.addEventListener('change', () => { localStorage.setItem(STORAGE_UI_THEME, uiThemeDark.checked ? 'dark' : 'light'); applyUiPrefs(); });
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
          const url = base ? base + '/api/config/export' : API + '/config/export';
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
          toast('Настройки выгружены в файл');
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
          if (document.getElementById('panel-stores') && !document.getElementById('panel-stores').hidden) {
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
      openai_key: document.getElementById('setting-openai_key').value,
      telegram_bot_token: document.getElementById('setting-telegram_bot_token').value,
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
    await api('/settings', { method: 'POST', body: JSON.stringify(body) });
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
        devPre.textContent = (data.lines || []).join('\\n');
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
  ['log-mode', 'log-action', 'log-store'].forEach(id => {
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

  const btnLogout = document.getElementById('btn-logout');
  if (btnLogout) btnLogout.addEventListener('click', doLogout);
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
