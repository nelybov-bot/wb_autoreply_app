(function () {
  'use strict';

  const API = '/api';
  const STORAGE_API_BASE = 'wb_autoreply_api_base';

  function getApiBase() {
    return (localStorage.getItem(STORAGE_API_BASE) || '').trim().replace(/\/$/, '');
  }

  async function api(path, options = {}) {
    const base = getApiBase();
    const url = path.startsWith('http') ? path : (base ? base + '/api' + path : API + path);
    const res = await fetch(url, {
      ...options,
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });
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
        showLogin();
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
    setTimeout(() => el.remove(), 4000);
  }

  function applyTabVisibility() {
    const canSettings = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_settings')));
    const canLog = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_log')));
    const canOpsLog = currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('view_ops_log')));
    document.querySelectorAll('.tab').forEach(tab => {
      const id = tab.getAttribute('data-tab');
      if (id === 'settings') tab.style.display = canSettings ? '' : 'none';
      else if (id === 'log') tab.style.display = (canLog || canOpsLog) ? '' : 'none';
    });
    const panelSettings = document.getElementById('panel-settings');
    const panelLog = document.getElementById('panel-log');
    if (panelSettings) panelSettings.style.display = canSettings ? '' : 'none';
    if (panelLog) panelLog.style.display = (canLog || canOpsLog) ? '' : 'none';
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
      if (id === 'reviews') loadReviews();
      if (id === 'questions') loadQuestions();
      if (id === 'settings') loadSettings();
      if (id === 'log') loadLog();
    });
  });

  // ---- Сводка ----
  async function loadStats() {
    try {
      const s = await api('/stats');
      document.getElementById('stat-total').textContent = s.total_sent ?? 0;
      document.getElementById('stat-today').textContent = s.sent_today ?? 0;
      document.getElementById('stat-reviews').textContent = s.by_type?.review ?? 0;
      document.getElementById('stat-questions').textContent = s.by_type?.question ?? 0;

      const byStore = s.by_store || [];
      const maxCount = Math.max(1, ...byStore.map(x => x.count));
      const wrap = document.getElementById('stats-by-store');
      if (!byStore.length) {
        wrap.innerHTML = '<p class="empty-state" style="padding: 24px;">Нет данных по магазинам</p>';
      } else {
        wrap.innerHTML = byStore.map(row => {
          const pct = maxCount ? Math.round((row.count / maxCount) * 100) : 0;
          return `
            <div class="chart-row">
              <span class="name">${escapeHtml(row.name || 'Без названия')}</span>
              <div class="bar-wrap"><div class="bar" style="width: ${pct}%"></div></div>
              <span class="count">${row.count}</span>
            </div>`;
        }).join('');
      }
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  // ---- Stores ----
  let stores = [];

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
      loadStores();
      fillStoreSelects();
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
            <button type="button" class="btn btn-danger btn-secondary btn-sm btn-delete-store" data-id="${s.id}">Удалить</button>
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
      loadStores();
      fillStoreSelects();
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
    if (!confirm('Удалить магазин и все его отзывы/вопросы?')) return;
    try {
      await api(`/stores/${storeId}`, { method: 'DELETE' });
      toast('Магазин удалён');
      loadStores();
      fillStoreSelects();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  async function loadStores() {
    try {
      stores = await api('/stores');
      renderStores();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  function fillStoreSelects() {
    const opts = '<option value="">Все магазины</option>' + stores.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
    document.getElementById('reviews-store').innerHTML = opts;
    document.getElementById('questions-store').innerHTML = opts;
  }

  // ---- Items (reviews / questions) ----
  let reviews = [];
  let questions = [];

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

  function pollTask(taskId, progressWrapId, fillId, textId, onDone) {
    const wrap = document.getElementById(progressWrapId);
    const fill = document.getElementById(fillId);
    const textEl = document.getElementById(textId);
    wrap.classList.add('visible');
    let interval = setInterval(async () => {
      try {
        const state = await api('/tasks/' + taskId);
        const [cur, total] = state.progress || [0, 1];
        const pctRaw = total ? Math.round((cur / total) * 100) : 0;
        const pct = Math.max(0, Math.min(100, pctRaw));
        fill.style.width = pct + '%';
        textEl.textContent = state.status === 'running' ? `Выполняется… ${cur}/${total}` : state.status === 'done' ? 'Готово' : state.status;
        if (state.status === 'done') {
          clearInterval(interval);
          wrap.classList.remove('visible');
          if (onDone) onDone(state.result);
        } else if (state.status === 'error') {
          clearInterval(interval);
          wrap.classList.remove('visible');
          toast(state.error || 'Ошибка', 'error');
        }
      } catch (_) {}
    }, 500);
  }

  async function loadReviews() {
    const storeId = document.getElementById('reviews-store').value || null;
    const q = storeId ? '?item_type=review&store_id=' + storeId : '?item_type=review';
    try {
      reviews = await api('/items' + q);
      renderItems('reviews', reviews, true);
      document.getElementById('reviews-empty').style.display = reviews.length ? 'none' : 'block';
      document.querySelector('#panel-reviews .items-table-wrap').style.display = reviews.length ? 'block' : 'none';
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  async function loadQuestions() {
    const storeId = document.getElementById('questions-store').value || null;
    const q = storeId ? '?item_type=question&store_id=' + storeId : '?item_type=question';
    try {
      questions = await api('/items' + q);
      renderItems('questions', questions, false);
      document.getElementById('questions-empty').style.display = questions.length ? 'none' : 'block';
      document.querySelector('#panel-questions .items-table-wrap').style.display = questions.length ? 'block' : 'none';
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

  document.getElementById('check-all-reviews').addEventListener('change', function () {
    document.querySelectorAll('#reviews-tbody .item-check').forEach(cb => { cb.checked = this.checked; });
  });
  document.getElementById('check-all-questions').addEventListener('change', function () {
    document.querySelectorAll('#questions-tbody .item-check').forEach(cb => { cb.checked = this.checked; });
  });

  async function runLoadNew(panelPrefix) {
    try {
      const res = await api('/load-new', { method: 'POST', body: JSON.stringify({ store_ids: null }) });
      pollTask(res.task_id, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', (result) => {
        toast('Загружено записей: ' + (result ?? 0));
        loadReviews();
        loadQuestions();
      });
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
    try {
      const res = await api('/generate', { method: 'POST', body: JSON.stringify({ item_ids: ids }) });
      pollTask(res.task_id, 'progress-' + panelPrefix, 'progress-' + panelPrefix + '-fill', 'progress-' + panelPrefix + '-text', (result) => {
        const r = result || {};
        toast('Сгенерировано: ' + (r.ok ?? 0) + ', ошибок: ' + (r.failed ?? 0));
        if (panelPrefix === 'reviews') loadReviews();
        else loadQuestions();
      });
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
      });
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  document.getElementById('btn-send-reviews').addEventListener('click', () => runSend('reviews'));
  document.getElementById('btn-send-questions').addEventListener('click', () => runSend('questions'));

  document.getElementById('reviews-store').addEventListener('change', loadReviews);
  document.getElementById('questions-store').addEventListener('change', loadQuestions);

  // ---- Settings ----
  async function loadSettings() {
    const apiBaseEl = document.getElementById('setting-api_base');
    if (apiBaseEl) apiBaseEl.value = localStorage.getItem(STORAGE_API_BASE) || '';
    try {
      await loadMe(true);
      const data = await api('/settings');
      ['openai_key', 'telegram_bot_token', 'telegram_chat_id'].forEach(k => {
        const el = document.getElementById('setting-' + k);
        if (el) el.value = data[k] || '';
      });
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

  document.getElementById('btn-save-settings').addEventListener('click', async () => {
    const body = {
      openai_key: document.getElementById('setting-openai_key').value,
      telegram_bot_token: document.getElementById('setting-telegram_bot_token').value,
      telegram_chat_id: document.getElementById('setting-telegram_chat_id').value,
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
        const summary = meta.applied != null ? (`применено ${meta.applied}, пропущено ${meta.skipped ?? 0}`) :
          meta.sent_ok != null ? (`ok ${meta.sent_ok}, пропущено ${meta.skipped ?? 0}, ошибок ${meta.failed ?? 0}`) :
          meta.ok != null ? (`ok ${meta.ok}, ошибок ${meta.failed ?? 0}`) :
          (meta.added != null ? (`добавлено ${meta.added}`) : '');
        const result = ev.result || '';
        const badge = result === 'error' ? 'danger' : 'ok';
        return `
          <div class="store-card">
            <div class="store-head">
              <div class="store-name">${escapeHtml(actionRu(ev.action))}</div>
              <span class="badge">${escapeHtml(result || 'ok')}</span>
            </div>
            <div class="meta">${escapeHtml(ev.ts)} · ${escapeHtml(ev.actor)} ${store ? '· ' + escapeHtml(store) : ''}</div>
            <div class="text-preview" style="max-width:unset;">${escapeHtml(summary || (meta.error || '') || '')}</div>
          </div>
        `;
      }).join('');
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

  function showLogin() {
    const m = document.getElementById('modal-login');
    if (!m) return;
    m.style.display = 'flex';
    setTimeout(() => {
      const u = document.getElementById('login-username');
      if (u) u.focus();
    }, 50);
  }

  function hideLogin() {
    const m = document.getElementById('modal-login');
    if (!m) return;
    m.style.display = 'none';
  }

  async function loadMe(silent = false) {
    try {
      const me = await api('/auth/me');
      currentUser = me;
      const label = document.getElementById('auth-user-label');
      if (label) label.textContent = `${me.username} (${me.role})`;
      applyTabVisibility();
      await refreshUsersSection();
      return me;
    } catch (err) {
      currentUser = null;
      const label = document.getElementById('auth-user-label');
      if (label) label.textContent = '—';
      if (!silent) toast(err.message, 'error');
      return null;
    }
  }

  async function doLogin() {
    const u = (document.getElementById('login-username').value || '').trim();
    const p = document.getElementById('login-password').value || '';
    if (!u || !p) {
      toast('Введите логин и пароль', 'error');
      return;
    }
    try {
      await api('/auth/login', { method: 'POST', body: JSON.stringify({ username: u, password: p }) });
      hideLogin();
      toast('Вход выполнен');
      await loadMe(true);
      await loadStores();
      fillStoreSelects();
      await loadReviews();
      await loadQuestions();
      await loadStats();
    } catch (err) {
      toast(err.message, 'error');
    }
  }

  async function doLogout() {
    try {
      await api('/auth/logout', { method: 'POST', body: JSON.stringify({}) });
    } catch (_) {}
    currentUser = null;
    showLogin();
  }

  const btnLogin = document.getElementById('btn-login');
  if (btnLogin) btnLogin.addEventListener('click', doLogin);
  const btnLogout = document.getElementById('btn-logout');
  if (btnLogout) btnLogout.addEventListener('click', doLogout);
  const passEl = document.getElementById('login-password');
  if (passEl) passEl.addEventListener('keydown', (e) => { if (e.key === 'Enter') doLogin(); });

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
            if (!confirm('Удалить пользователя?')) return;
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
    if (!me) {
      showLogin();
      return;
    }
    loadStats();
    loadStores().then(() => {
      fillStoreSelects();
      loadReviews();
      loadQuestions();
    });
  });
})();
