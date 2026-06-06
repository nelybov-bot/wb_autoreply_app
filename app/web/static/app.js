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
      throw new Error(msg);
    }
    if (res.status === 204 || res.headers.get('content-length') === '0') return null;
    return res.json();
  }

  function toast(message, type = 'success') {
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    const icon = document.createElement('span');
    icon.className = 'toast-icon';
    icon.innerHTML = type === 'success' ? '✓' : '✕';
    el.appendChild(icon);
    el.appendChild(document.createTextNode(message));
    document.body.appendChild(el);
    setTimeout(() => el.remove(), getUiToastMs());
  }

  function applyTabVisibility() {
    const canSettings = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_settings')));
    const canLog = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_log')));
    const canOpsLog = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_ops_log')));
    document.querySelectorAll('.tab').forEach(tab => {
      const id = tab.getAttribute('data-tab');
      if (id === 'settings') tab.style.display = canSettings ? '' : 'none';
      else if (id === 'auto') tab.style.display = canSettings ? '' : 'none';
      else if (id === 'log') tab.style.display = (canLog || canOpsLog) ? '' : 'none';
    });
    const panelSettings = document.getElementById('panel-settings');
    const panelAuto = document.getElementById('panel-auto');
    const panelLog = document.getElementById('panel-log');
    if (panelSettings) panelSettings.style.display = canSettings ? '' : 'none';
    if (panelAuto) panelAuto.style.display = canSettings ? '' : 'none';
    if (panelLog) panelLog.style.display = (canLog || canOpsLog) ? '' : 'none';
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
        pollTask(taskId, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', () => {
          // onDone already clears via pollTask completion path, but keep safe
          setActiveTask(panelPrefix, '');
          if (panelPrefix === 'reviews') loadReviews();
          else if (panelPrefix === 'questions') loadQuestions();
        }, panelPrefix);
      } else {
        setActiveTask(panelPrefix, '');
      }
    } catch (_) {
      // если сеть/сервер недоступны — не трогаем ключ, попробуем при следующем заходе
    }
  }

  // ---- Tabs ----
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      tab.classList.add('active');
      const id = tab.getAttribute('data-tab');
      const panel = document.getElementById('panel-' + id);
      if (panel) panel.classList.add('active');
      if (id === 'summary') loadStats();
      if (id === 'stores') loadStores();
      if (id === 'reviews') { loadReviews(); resumePanelTask('reviews'); }
      if (id === 'questions') { loadQuestions(); resumePanelTask('questions'); }
      if (id === 'wb-chats') { loadWbChatsPanel(); }
      if (id === 'ozon-chats') { loadOzonChatsPanel(); }
      if (id === 'ozon-actions') { loadOzonActionsPanel(); }
      if (id === 'auto') loadAutoSchedulePanel();
      if (id === 'settings') loadSettings();
      if (id === 'log') loadLog();
    });
  });

  // ---- Сводка ----
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
    } catch (err) {
      toast(err.message, 'error');
    }
  }

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

  function renderStores() {
    const wrap = document.getElementById('stores-list');
    if (!stores.length) {
      wrap.innerHTML = '<p class="empty-state">Нет магазинов. Добавьте первый выше.</p>';
      return;
    }
    wrap.innerHTML = stores.map(s => {
      const mp = s.marketplace === 'wb' ? 'WB' : s.marketplace === 'yam' ? 'Яндекс' : 'Ozon';
      return `
        <div class="store-card" data-store-id="${s.id}">
          <h3>${escapeHtml(s.name)} <span class="badge">${mp}</span></h3>
          <div class="meta">ID ${s.id} ${s.active ? '' : '· неактивен'}</div>
          <div class="actions">
            <button type="button" class="btn btn-secondary btn-sm btn-edit-store" data-id="${s.id}">Изменить</button>
            <button type="button" class="btn btn-danger btn-sm btn-delete-store" data-id="${s.id}">Удалить</button>
          </div>
        </div>`;
    }).join('');

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
    const wbSel = document.getElementById('wb-chats-store');
    if (wbSel) {
      const wb = storesForMarketplace('wb');
      const prev = String(wbSel.value || '').trim();
      wbChatsSuppressSelectChange = true;
      wbSel.innerHTML = wb.length
        ? wb.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('')
        : '<option value="">Нет магазинов WB</option>';
      if (wb.length) {
        const ids = new Set(wb.map(s => String(s.id)));
        if (prev && ids.has(prev)) wbSel.value = prev;
        else selectFirstStoreOption(wbSel);
      }
      setTimeout(() => { wbChatsSuppressSelectChange = false; }, 0);
    }
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
        <span>${escapeHtml(s.name)} <span class="badge">${escapeHtml(s.marketplace)}</span></span>
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
        ozon_actions: 'акции Ozon',
        idle_items: 'без отзывов/вопросов',
        done: 'завершено',
        cancelled: 'остановлено',
        error: 'ошибка',
      };
      const phase = phaseMap[s.phase] || (s.phase || '—');
      const run = s.running ? 'Выполняется' : 'Не выполняется';
      const slot = s.slot ? `слот ${s.slot}` : '—';
      const next = s.next_slot ? `следующий ${s.next_slot}` : 'нет слотов';
      const err = s.last_error ? ` · ошибка: ${s.last_error}` : '';
      const hint = s.schedule_hint ? `\n${s.schedule_hint}` : '';
      el.textContent = `${run} · этап: ${phase} · текущий: ${slot} · ${next}${err}${hint}`;
      const stopBtn = document.getElementById('btn-stop-auto');
      if (stopBtn) stopBtn.disabled = !s.running;
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
  let wbChatThreadPages = 4;
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
    return '<div class="form-hint" style="margin-top:8px;">Показано последнее сообщение из списка WB. Нажмите «Обновить переписку» для полной истории.</div>';
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
        statusHint.textContent = 'Последнее сообщение не от покупателя — автоответ не нужен.';
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
          const lab = l.role === 'client' ? 'Покупатель' : l.role === 'seller' ? 'Вы' : escapeHtml(l.role || '');
          return `<div class="wb-chat-line"><span class="wb-chat-role">${lab}</span><span class="wb-chat-text">${escapeHtml(l.text)}</span></div>`;
        }).join('')
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
      setChatStatusBar('wb-chats-status-bar', 'ok', 'Переписка загружена ранее. «Обновить переписку» — обновить с WB.');
    } else {
      setChatStatusBar('wb-chats-status-bar', 'info', 'Нажмите «Обновить переписку» для полной истории.');
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
    if (!wbChatsRaw.length) {
      wrap.innerHTML = '<div class="form-hint">Нет чатов. Нажмите «Обновить список чатов».</div>';
      return;
    }
    const rows = [...wbChatsRaw].sort((a, b) => wbChatsSortKey(b) - wbChatsSortKey(a));
    wrap.innerHTML = rows.map(c => {
      const id = String(c.chatID || '');
      const enc = encodeURIComponent(id);
      const name = escapeHtml(c.clientName || 'Покупатель');
      const raw = c.lastMessage && c.lastMessage.text ? String(c.lastMessage.text) : '';
      const lm = escapeHtml(raw.slice(0, 140)) || '—';
      const active = wbChatSelectedId === id ? 'wb-chat-item active' : 'wb-chat-item';
      return `<button type="button" class="${active}" data-chat-id="${enc}"><div class="wb-chat-item-name">${name}</div><div class="wb-chat-item-preview">${lm}</div></button>`;
    }).join('');
    wrap.querySelectorAll('.wb-chat-item').forEach(btn => {
      btn.addEventListener('click', () => {
        const chatId = decodeURIComponent(btn.getAttribute('data-chat-id') || '');
        wbChatSelectedId = chatId;
        wbChatThreadPages = 4;
        renderWbChatsList();
        restoreWbChatDetailForSelected();
      });
    });
  }

  async function refreshWbChatsList(forceRefresh = true) {
    try {
      await ensureStoresLoaded();
      fillStoreSelects();
    } catch (err) {
      if (!getWbChatsStoreId()) {
        setChatStatusBar('wb-chats-status-bar', 'error', err.message || 'Не удалось загрузить магазины');
        setPanelLoading('wb-chats-loading', false);
        return;
      }
    }
    const sid = getWbChatsStoreId();
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
    try {
      const data = await api(`/wb/buyer-chats/${sid}${q}`, { timeoutMs: 120000 });
      if (gen !== wbChatsListFetchGen) return;
      if (Number(getWbChatsStoreId()) !== Number(sid)) return;
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
      if (!wbChatSelectedId) {
        setChatStatusBar(
          'wb-chats-status-bar',
          n ? 'ok' : 'info',
          n ? `Загружено чатов: ${n}. Выберите чат слева.` : 'Чатов пока нет (или пустой ответ WB).',
        );
        const hint = document.getElementById('wb-chats-hint');
        const body = document.getElementById('wb-chats-detail-body');
        if (hint) {
          hint.style.display = '';
          hint.textContent = 'Выберите чат слева.';
        }
        if (body) body.style.display = 'none';
      } else if (n) {
        setChatStatusBar('wb-chats-status-bar', 'ok', `Загружено чатов: ${n}.`);
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
    setChatStatusBar('wb-chats-status-bar', 'loading', 'Загружаю переписку…');
    setPanelLoading('wb-chats-loading', true, 'Загружаю переписку…');
    setChatToolbarBusy('wb-chats', true);
    try {
      const pages = Math.max(1, Math.min(8, wbChatThreadPages || 2));
      const path = `/wb/buyer-chats/${sid}/${encodeURIComponent(chatId)}/thread?pages=${pages}`;
      const t = await api(path, { timeoutMs: 90000 });
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
      const elig = r.wb_chat_eligible ?? 0;
      const cand = r.wb_chat_candidates ?? 0;
      toast(
        `Отправлено: ${sent}. В партии: ${cand} из ${elig}. Уже отвечено: ${dup}, раньше даты: ${cutoff}. Ошибки ИИ: ${genF}, отправки: ${sendF}, без reply_sign: ${skip}.`,
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
      sel.addEventListener('change', () => {
        if (wbChatsSuppressSelectChange) return;
        wbChatsRaw = [];
        wbChatsListStoreId = null;
        wbChatSelectedId = null;
        wbChatReplySign = '';
        wbChatThreadCache.clear();
        void refreshWbChatsList(true);
      });
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
      wbChatThreadPages = 4;
      void loadWbChatThread(wbChatSelectedId);
    });
    const bMore = document.getElementById('btn-wb-chats-more-history');
    if (bMore) bMore.addEventListener('click', () => {
      if (!wbChatSelectedId) {
        toast('Выберите чат', 'error');
        return;
      }
      wbChatThreadPages = Math.min(8, (wbChatThreadPages || 2) + 2);
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
      return `<button type="button" class="${active}" data-chat-id="${enc}"><div class="wb-chat-item-name">Чат ${escapeHtml(id.slice(0, 8))}…</div><div class="wb-chat-item-preview">${preview}</div></button>`;
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
    const q = forceRefresh ? '?refresh=1' : '';
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
            return `<div class="wb-chat-line"><span class="wb-chat-role">${lab}</span><span class="wb-chat-text">${escapeHtml(l.text)}</span></div>`;
          }).join('')
          : '<div class="form-hint">Нет текста сообщений.</div>';
      }
      const statusHint = document.getElementById('ozon-chats-status-hint');
      const sendBtn = document.getElementById('btn-ozon-chats-send');
      const replyBlocked = !!t.reply_window_blocked;
      if (sendBtn) sendBtn.disabled = replyBlocked;
      if (statusHint) {
        if (replyBlocked) {
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
    if (sid != null && ozonChatsListStoreId != null && Number(sid) === Number(ozonChatsListStoreId) && ozonChatsRaw.length) {
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
        ozonChatSelectedId = null;
        void refreshOzonChatsList();
      });
    }
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
          if (panelPrefix) setActiveTask(panelPrefix, '');
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
    const statusRu = { new: 'Новый', generated: 'Сгенерирован', sent: 'Отправлен', ignored: 'Игнор' };
    tbody.innerHTML = items.map(item => {
      const title = (item.product_title || '').slice(0, 50);
      const text = (item.text || '').slice(0, 80);
      const statusClass = item.status === 'new' ? 'new' : item.status === 'generated' ? 'generated' : 'sent';
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
        showItemModal(item, store ? store.name : '—', prefix === 'reviews');
      });
    });
  }

  function showItemModal(item, storeName, isReview) {
    const modal = document.getElementById('modal-item-detail');
    const titleEl = document.getElementById('modal-item-detail-title');
    const storeEl = document.getElementById('modal-item-store');
    const productEl = document.getElementById('modal-item-product');
    const textLabel = document.getElementById('modal-item-text-label');
    const textEl = document.getElementById('modal-item-text');
    const answerEl = document.getElementById('modal-item-answer');
    if (!modal) return;
    titleEl.textContent = isReview ? 'Отзыв' : 'Вопрос';
    textLabel.textContent = isReview ? 'Текст отзыва' : 'Текст вопроса';
    storeEl.textContent = storeName;
    productEl.textContent = (item.product_title || '').trim() || '—';
    textEl.textContent = (item.text || '').trim() || '—';
    answerEl.textContent = (item.generated_text || '').trim() || '—';
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
    try {
      const res = await api('/load-new', { method: 'POST', body: JSON.stringify({ store_ids: [sid] }) });
      pollTask(res.task_id, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', (result) => {
        toast('Загружено записей: ' + (result ?? 0));
        loadReviews();
        loadQuestions();
      }, panelPrefix);
    } catch (err) {
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
    try {
      const res = await api('/generate', { method: 'POST', body: JSON.stringify({ item_ids: filtered.ids }) });
      pollTask(res.task_id, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', (result) => {
        const r = result || {};
        toast('Сгенерировано: ' + (r.ok ?? 0) + ', ошибок: ' + (r.failed ?? 0));
        if (panelPrefix === 'reviews') loadReviews();
        else loadQuestions();
      }, panelPrefix);
    } catch (err) {
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
    try {
      const res = await api('/send', { method: 'POST', body: JSON.stringify({ item_ids: ids }) });
      pollTask(res.task_id, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', (result) => {
        const r = result || {};
        toast('Отправлено: ' + (r.sent_ok ?? 0) + ', пропущено: ' + (r.skipped ?? 0) + ', ошибок: ' + (r.failed ?? 0));
        if (panelPrefix === 'reviews') loadReviews();
        else loadQuestions();
        loadStats();
      }, panelPrefix);
    } catch (err) {
      toast(err.message, 'error');
    }
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
  async function loadSettings() {
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
      ['openai_key', 'telegram_bot_token', 'telegram_chat_id', 'buyer_chat_reply_from_date'].forEach(k => {
        const el = document.getElementById('setting-' + k);
        if (el) el.value = data[k] || '';
      });
      const autoAgeEl = document.getElementById('setting-buyer_chat_auto_max_age_days');
      if (autoAgeEl) autoAgeEl.value = String(data.buyer_chat_auto_max_age_days || '3');
      const tgEnabled = document.getElementById('setting-telegram_enabled');
      if (tgEnabled) tgEnabled.checked = String(data.telegram_enabled || '1') !== '0';
      const prompts = await api('/prompts');
      const wrap = document.getElementById('prompts-list');
      wrap.innerHTML = prompts.map(p => `
        <div class="form-group" style="margin-top: 12px;">
          <label>${p.item_type} / ${p.rating_group}</label>
          <textarea data-prompt-id="${p.id}" class="prompt-text">${escapeHtml(p.prompt_text)}</textarea>
        </div>`).join('');
      wrap.querySelectorAll('.prompt-text').forEach(ta => {
        ta.addEventListener('blur', async () => {
          const id = Number(ta.getAttribute('data-prompt-id'));
          await api('/prompts/' + id, { method: 'PATCH', body: JSON.stringify({ prompt_text: ta.value }) });
          toast('Промпт сохранён');
        });
      });
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
      const autoRunReviews = document.getElementById('auto-run-reviews');
      const autoRunQuestions = document.getElementById('auto-run-questions');
      const autoRunWbChats = document.getElementById('auto-run-wb-chats');
      if (autoEnabled) autoEnabled.checked = !!autoCfg.enabled;
      if (autoSlots) autoSlots.value = (autoCfg.slots || []).join(', ');
      if (autoMode) autoMode.value = autoCfg.schedule_mode || 'slots';
      if (autoInt) autoInt.value = String(autoCfg.interval_hours || 1);
      if (autoRunReviews) autoRunReviews.checked = !!autoCfg.run_reviews;
      if (autoRunQuestions) autoRunQuestions.checked = !!autoCfg.run_questions;
      if (autoRunWbChats) autoRunWbChats.checked = !!autoCfg.run_wb_chats;
      const autoRunOzonChats = document.getElementById('auto-run-ozon-chats');
      if (autoRunOzonChats) autoRunOzonChats.checked = !!autoCfg.run_ozon_chats;
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
      const run_reviews = !!document.getElementById('auto-run-reviews')?.checked;
      const run_questions = !!document.getElementById('auto-run-questions')?.checked;
      const run_wb_chats = !!document.getElementById('auto-run-wb-chats')?.checked;
      const run_ozon_chats = !!document.getElementById('auto-run-ozon-chats')?.checked;
      const run_ozon_actions_remove = !!document.getElementById('auto-run-ozon-actions-remove')?.checked;
      const store_ids = getAutoSelectedStoreIds();
      if (!store_ids.length) {
        toast('Выбери хотя бы один магазин для автозапуска', 'error');
        return;
      }
      if (!run_reviews && !run_questions && !run_wb_chats && !run_ozon_chats && !run_ozon_actions_remove) {
        toast('Выбери хотя бы один тип: отзывы, вопросы, чаты или акции Ozon', 'error');
        return;
      }
      try {
        await api('/auto-schedule', { method: 'POST', body: JSON.stringify({ enabled, slots, store_ids, schedule_mode, interval_hours, run_reviews, run_questions, run_wb_chats, run_ozon_chats, run_ozon_actions_remove }) });
        toast('Автозапуск сохранён');
        await loadAutoSchedulePanel();
      } catch (err) {
        toast(err.message, 'error');
      }
    });
  }

  const autoModeEl = document.getElementById('auto-schedule-mode');
  if (autoModeEl) autoModeEl.addEventListener('change', syncAutoScheduleModeUi);

  const btnStopAuto = document.getElementById('btn-stop-auto');
  if (btnStopAuto) {
    btnStopAuto.addEventListener('click', async () => {
      try {
        const r = await api('/auto-schedule/stop', { method: 'POST', body: JSON.stringify({}) });
        toast(r && r.stopped ? 'Автозапуск остановлен' : 'Сейчас автозапуск не выполняется');
        refreshAutoStatus();
      } catch (err) {
        toast(err.message, 'error');
      }
    });
  }

  document.getElementById('btn-save-settings').addEventListener('click', async () => {
    const body = {
      openai_key: document.getElementById('setting-openai_key').value,
      telegram_bot_token: document.getElementById('setting-telegram_bot_token').value,
      telegram_chat_id: document.getElementById('setting-telegram_chat_id').value,
      telegram_enabled: document.getElementById('setting-telegram_enabled')?.checked ? '1' : '0',
      buyer_chat_reply_from_date: document.getElementById('setting-buyer_chat_reply_from_date')?.value || '',
      buyer_chat_auto_max_age_days: String(parseInt(document.getElementById('setting-buyer_chat_auto_max_age_days')?.value || '3', 10) || 3),
    };
    try {
      await api('/settings', { method: 'POST', body: JSON.stringify(body) });
      toast('Настройки сохранены');
    } catch (err) {
      toast(err.message, 'error');
    }
  });

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
      wb_buyer_chat_generate: 'Чат WB: генерация',
      wb_buyer_chat_send: 'Чат WB: отправка',
      wb_buyer_chat_mass_send: 'Чат WB: массово ИИ+отправка',
      ozon_buyer_chat_generate: 'Чат Ozon: генерация',
      ozon_buyer_chat_send: 'Чат Ozon: отправка',
      ozon_buyer_chat_mass_send: 'Чат Ozon: массово ИИ+отправка',
    };
    return m[a] || a || '—';
  }

  async function loadLog() {
    const mode = (document.getElementById('log-mode')?.value || 'ops');
    const action = (document.getElementById('log-action')?.value || '').trim();
    const level = (document.getElementById('log-level')?.value || '').trim();
    const q = (document.getElementById('log-q')?.value || '').trim();
    const devPre = document.getElementById('log-content');
    const opsWrap = document.getElementById('ops-log-wrap');
    if (devPre) devPre.style.display = (mode === 'dev') ? 'block' : 'none';
    if (opsWrap) opsWrap.style.display = (mode === 'ops') ? 'grid' : 'none';
    try {
      if (mode === 'dev') {
        const data = await api('/log/dev?limit=600' + (level ? '&level=' + encodeURIComponent(level) : '') + (action ? '&action=' + encodeURIComponent(action) : '') + (q ? '&q=' + encodeURIComponent(q) : ''));
        devPre.textContent = (data.lines || []).join('\\n');
        return;
      }

      const data = await api('/log/ops?limit=200' + (action ? '&action=' + encodeURIComponent(action) : '') + (q ? '&q=' + encodeURIComponent(q) : ''));
      const items = data.items || [];
      if (!items.length) {
        opsWrap.innerHTML = '<div class="empty-state" style="grid-column:1/-1; padding:24px;">Нет событий</div>';
        return;
      }
      opsWrap.innerHTML = items.map(ev => {
        const meta = safeJsonParse(ev.meta_json || '') || {};
        const store = ev.store_id ? ('store_id=' + ev.store_id) : '';
        const itemIds = Array.isArray(meta.item_ids) ? meta.item_ids : [];
        const summary = meta.applied != null ? (`применено ${meta.applied}, пропущено ${meta.skipped ?? 0}`) :
          meta.sent_ok != null ? (`ok ${meta.sent_ok}, пропущено ${meta.skipped ?? 0}, ошибок ${meta.failed ?? 0}`) :
          meta.ok != null ? (`ok ${meta.ok}, ошибок ${meta.failed ?? 0}`) :
          (meta.added != null ? (`добавлено ${meta.added}`) : '');
        const result = ev.result || '';
        return `
          <div class="store-card">
            <div class="store-head">
              <div class="store-name">${escapeHtml(actionRu(ev.action))}</div>
              <span class="badge">${escapeHtml(result || 'ok')}</span>
            </div>
            <div class="meta">${escapeHtml(ev.ts)} · ${escapeHtml(ev.actor)} ${store ? '· ' + escapeHtml(store) : ''}</div>
            <div class="text-preview" style="max-width:unset;">${escapeHtml(summary || (meta.error || '') || '')}</div>
            ${itemIds.length ? `<div class="actions" style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;"><button type="button" class="btn btn-secondary btn-sm" data-ops-show="${ev.id}">Показать ответы (20)</button><span class="form-hint">items: ${itemIds.length}</span></div><div class="ops-items\" id=\"ops-items-${ev.id}\" style=\"display:none; margin-top:10px;\"></div>` : ''}
          </div>
        `;
      }).join('');
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
      else opsWrap.innerHTML = '<div class="empty-state" style="grid-column:1/-1; padding:24px;">Ошибка: ' + escapeHtml(err.message) + '</div>';
    }
  }

  document.getElementById('btn-refresh-log').addEventListener('click', loadLog);

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
          <div class="store-card">
            <div class="store-head">
              <div class="store-name">${escapeHtml(u.username)}</div>
              <span class="badge">${escapeHtml(u.role)}</span>
            </div>
            <div class="store-meta" style="margin-top:8px;font-size:0.85rem;color:var(--text-muted);">
              <label style="display:inline-flex;align-items:center;gap:6px;margin-right:12px;"><input type="checkbox" data-user-id="${u.id}" data-perm="view_settings" ${settingsChecked ? 'checked' : ''}${disablePerms}> Настройки</label>
              <label style="display:inline-flex;align-items:center;gap:6px;"><input type="checkbox" data-user-id="${u.id}" data-perm="view_log" ${logChecked ? 'checked' : ''}${disablePerms}> Лог</label>
              <label style="display:inline-flex;align-items:center;gap:6px;margin-left:12px;"><input type="checkbox" data-user-id="${u.id}" data-perm="view_ops_log" ${opsLogChecked ? 'checked' : ''}${disablePerms}> Операции</label>
            </div>
            <div class="store-actions">
              <button type="button" class="btn btn-danger" data-user-del="${u.id}">Удалить</button>
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
