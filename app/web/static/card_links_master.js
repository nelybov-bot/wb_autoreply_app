/**
 * Мастер связок WB — отдельная вкладка, не трогает ИИ-связки.
 */
(function () {
  const selected = new Set();
  let page = 1;
  let pageSize = 100;
  let pageCount = 1;
  let pollTimer = null;
  let categoryCounts = {};

  function storeId() {
    return Number(document.getElementById('card-links-store')?.value || 0);
  }

  function esc(s) {
    if (s == null) return '';
    const d = document.createElement('div');
    d.textContent = String(s);
    return d.innerHTML;
  }

  async function api(path, opts = {}) {
    const r = await fetch(path, {
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
      ...opts,
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) {
      const detail = data.detail;
      const msg = Array.isArray(detail)
        ? detail.map((x) => x.msg || x).join('; ')
        : (detail || data.error || data.message || r.statusText);
      throw new Error(msg);
    }
    return data;
  }

  function setStats(text) {
    const el = document.getElementById('card-links-master-stats');
    if (el) el.textContent = text;
  }

  function setLog(lines) {
    const wrap = document.getElementById('card-links-master-log');
    if (!wrap) return;
    if (!lines || !lines.length) {
      wrap.hidden = true;
      wrap.innerHTML = '';
      return;
    }
    wrap.hidden = false;
    wrap.innerHTML = lines.slice(-30).map((x) => {
      const msg = typeof x === 'string' ? x : (x.message || '');
      const lvl = typeof x === 'object' ? (x.level || 'info') : 'info';
      return `<div class="clm-log-line clm-log-${esc(lvl)}">${esc(msg)}</div>`;
    }).join('');
  }

  function fillSelect(id, options, keepAll = true) {
    const sel = document.getElementById(id);
    if (!sel) return;
    const cur = sel.value;
    sel.innerHTML = keepAll ? '<option value="">Все</option>' : '';
    (options || []).forEach((v) => {
      const o = document.createElement('option');
      o.value = typeof v === 'object' ? v.value : v;
      o.textContent = typeof v === 'object' ? v.label : v;
      sel.appendChild(o);
    });
    if (cur && [...sel.options].some((o) => o.value === cur)) sel.value = cur;
  }

  function denseMinValue() {
    return Number(document.getElementById('clm-filter-dense-min')?.value || 3);
  }

  function denseFilterOn() {
    return Boolean(document.getElementById('clm-filter-dense')?.checked);
  }

  function filterParams(includePaging = true) {
    const seg = document.getElementById('clm-filter-segment')?.value || '';
    const cat = document.getElementById('clm-filter-category')?.value || '';
    const brand = document.getElementById('clm-filter-brand')?.value || '';
    const q = new URLSearchParams();
    if (seg) q.set('segment', seg);
    if (cat) q.set('category', cat);
    if (brand) q.set('brand', brand);
    if (denseFilterOn()) {
      q.set('min_bundles_in_category', String(denseMinValue()));
    }
    if (includePaging) {
      const psRaw = Number(document.getElementById('clm-page-size')?.value ?? 100);
      pageSize = psRaw === 0 ? 100000 : psRaw;
      q.set('page', String(page));
      q.set('page_size', String(pageSize > 5000 ? 5000 : pageSize));
    }
    return q;
  }

  function updateMergeButton() {
    const btn = document.getElementById('btn-clm-merge-selected');
    if (!btn) return;
    const n = selected.size;
    btn.disabled = n < 2;
    btn.textContent = n >= 2 ? `Объединить выбранные (${n})` : 'Объединить выбранные';
  }

  async function refreshStatus() {
    const sid = storeId();
    if (!sid) {
      setStats('Выберите магазин WB.');
      return null;
    }
    const data = await api(`/api/card-links/master/${sid}/status`);
    const c = data.coverage || {};
    const f = data.filters || {};
    const dense = data.dense_categories || [];
    const catOpts = (f.categories || []).map((name) => {
      const cnt = dense.find((d) => d.category_label === name);
      return cnt
        ? { value: name, label: `${name} (${cnt.bundle_count} связок)` }
        : name;
    });
    fillSelect('clm-filter-category', catOpts);
    fillSelect('clm-filter-brand', f.brands || []);
    let stats = `Всего ${c.total || 0} · в плане ${c.planned_items || 0} · одиночек ${c.singles || 0} · связок ${c.bundles || 0}`;
    if (dense.length) {
      stats += ` · дробных категорий (≥3): ${dense.length}`;
    }
    setStats(stats);
    setLog((data.state && data.state.log) || []);
    return data;
  }

  function renderBundles(bundles) {
    const list = document.getElementById('card-links-master-list');
    if (!list) return;
    if (!bundles.length) {
      const hint = denseFilterOn()
        ? 'Нет связок по фильтру «категории с несколькими связками». Снимите фильтр или уменьшите порог.'
        : 'Нет связок. Выполните шаги 1–5.';
      list.innerHTML = `<p class="empty-cell empty-cell--muted">${esc(hint)}</p>`;
      return;
    }
    list.innerHTML = bundles.map((b) => {
      const bid = String(b.bundle_id || '');
      const checked = selected.has(bid) ? ' checked' : '';
      const st = String(b.apply_status || '');
      const stBadge = st && st !== 'pending'
        ? `<span class="clm-apply-badge clm-apply-${esc(st)}">${esc(st === 'applied' ? 'применено' : st === 'skipped' ? 'пропуск' : 'ошибка')}</span>`
        : '';
      const cat = String(b.category_label || '');
      const catN = Number(categoryCounts[cat] || 0);
      const catBadge = catN >= 2
        ? `<span class="clm-cat-badge" title="Связок в этой категории WB">${catN} в кат.</span>`
        : '';
      const items = (b.items || []).slice(0, 12);
      const cards = items.map((it) => `
        <div class="clm-item-card">
          <span class="clm-item-title">${esc((it.title || '').slice(0, 70))}</span>
          <span class="clm-item-meta">${esc(it.vendor_code || it.nm_id || '')}</span>
        </div>`).join('');
      const more = (b.item_count || 0) > items.length ? `<span class="form-hint">+${(b.item_count || 0) - items.length} ещё</span>` : '';
      return `<div class="clm-bundle card-links-ai-block">
        <label class="clm-bundle-head">
          <input type="checkbox" class="clm-bundle-check" data-bundle-id="${esc(bid)}"${checked}>
          <strong>${esc(cat || 'Категория')}</strong>
          ${catBadge}
          <span class="clm-bundle-meta">${esc(b.brand || '')} · ${esc(b.subtype_label || '')} · ${b.item_count || 0} шт</span>
          ${b.target_imt ? `<span class="clm-bundle-imt">imtID ${b.target_imt}</span>` : ''}
          ${stBadge}
        </label>
        <div class="clm-bundle-items">${cards}${more}</div>
      </div>`;
    }).join('');
    list.querySelectorAll('.clm-bundle-check').forEach((el) => {
      el.addEventListener('change', () => {
        const id = el.getAttribute('data-bundle-id');
        if (!id) return;
        if (el.checked) selected.add(id);
        else selected.delete(id);
        updateMergeButton();
      });
    });
    updateMergeButton();
  }

  async function loadBundles() {
    const sid = storeId();
    if (!sid) return;
    const data = await api(`/api/card-links/master/${sid}/bundles?${filterParams(true).toString()}`);
    categoryCounts = data.category_counts || {};
    pageCount = data.page_count || 1;
    page = data.page || 1;
    const label = document.getElementById('clm-page-label');
    if (label) {
      label.textContent = pageSize >= 5000
        ? `Все · ${data.total || 0} связок`
        : `Стр. ${page}/${pageCount} · ${data.total || 0} связок`;
    }
    renderBundles(data.bundles || []);
  }

  async function mergeSelected() {
    const sid = storeId();
    if (!sid) {
      alert('Выберите магазин WB');
      return;
    }
    const ids = [...selected];
    if (ids.length < 2) {
      alert('Выберите минимум 2 связки');
      return;
    }
    if (!confirm(`Объединить ${ids.length} связок в одну (макс. 29 SKU)?\n\nОдин subject WB, совместимый сегмент/бренд.`)) {
      return;
    }
    try {
      setStats('Объединение…');
      const res = await api(`/api/card-links/master/${sid}/merge-bundles`, {
        method: 'POST',
        body: JSON.stringify({ bundle_ids: ids }),
      });
      selected.clear();
      selected.add(String(res.new_bundle_id || ''));
      await refreshStatus();
      await loadBundles();
      alert(`Готово: ${res.merged_from?.length || ids.length} связок → ${res.new_bundle_id} (${res.item_count} шт)`);
    } catch (e) {
      alert(e.message || String(e));
      await loadBundles();
    }
  }

  function applyResultMessage(result) {
    if (!result || result.step !== 'apply') return '';
    const ok = Number(result.ok || 0);
    const fail = Number(result.fail || 0);
    const skipped = Number(result.skipped || 0);
    let msg = `Применено: ${ok}`;
    if (skipped) msg += ` · пропущено (уже в связке): ${skipped}`;
    if (fail) msg += ` · ошибок: ${fail}`;
    if (result.errors && result.errors.length) {
      const first = result.errors.slice(0, 3).map((e) => `${e.bundle_id}: ${e.error}`).join('\n');
      msg += `\n\n${first}`;
    }
    return msg;
  }

  async function pollTask(taskId, onDone) {
    clearInterval(pollTimer);
    pollTimer = setInterval(async () => {
      try {
        const t = await api(`/api/tasks/${taskId}`);
        if (t.status === 'running') {
          const p = t.progress || [0, 1];
          setStats(t.detail || `Выполняется… ${p[0]}/${p[1]}`);
          return;
        }
        clearInterval(pollTimer);
        if (t.status === 'done') {
          await refreshStatus();
          await loadBundles();
          onDone(null, t.result || {});
        } else {
          onDone(t.error || 'Ошибка задачи');
        }
      } catch (e) {
        clearInterval(pollTimer);
        onDone(e.message || String(e));
      }
    }, 1200);
  }

  async function runStep(step, extraBody = {}) {
    const sid = storeId();
    if (!sid) {
      alert('Выберите магазин WB');
      return;
    }
    if (step === 'apply') {
      const n = selected.size;
      const msg = n
        ? `Применить ${n} выбранных связок? Между запросами пауза ~2 с.`
        : 'Применить все связки из плана?';
      if (!confirm(msg)) return;
    } else if (step === 'load') {
      if (!confirm('Загрузить каталог WB? Текущий кэш мастера будет заменён.')) return;
    }
    try {
      setStats('Запуск…');
      const body = { max_pages: 100, ...extraBody };
      if (step === 'apply') {
        body.bundle_ids = selected.size ? [...selected] : [];
      }
      const res = await api(`/api/card-links/master/${sid}/step/${step}`, {
        method: 'POST',
        body: JSON.stringify(body),
      });
      pollTask(res.task_id, (err, result) => {
        if (err) {
          alert(err);
          return;
        }
        if (step === 'apply') {
          selected.clear();
          updateMergeButton();
          const summary = applyResultMessage(result);
          if (summary) alert(summary);
        }
      });
    } catch (e) {
      alert(e.message || String(e));
    }
  }

  function bindUi() {
    document.querySelectorAll('.clm-step').forEach((btn) => {
      btn.addEventListener('click', () => {
        const step = btn.getAttribute('data-clm-step');
        if (step) void runStep(step);
      });
    });
    document.getElementById('btn-clm-refresh')?.addEventListener('click', () => {
      void refreshStatus().then(() => loadBundles());
    });
    document.getElementById('btn-clm-select-all')?.addEventListener('click', async () => {
      const sid = storeId();
      if (!sid) return;
      try {
        const data = await api(`/api/card-links/master/${sid}/bundle-ids?${filterParams(false).toString()}`);
        (data.bundle_ids || []).forEach((id) => selected.add(String(id)));
        await loadBundles();
        setStats(`Выбрано связок: ${selected.size}`);
      } catch (e) {
        alert(e.message || String(e));
      }
    });
    document.getElementById('btn-clm-clear')?.addEventListener('click', () => {
      selected.clear();
      updateMergeButton();
      void loadBundles();
    });
    document.getElementById('btn-clm-merge-selected')?.addEventListener('click', () => {
      void mergeSelected();
    });
    document.getElementById('btn-clm-apply-selected')?.addEventListener('click', () => {
      void runStep('apply');
    });
    ['clm-filter-segment', 'clm-filter-category', 'clm-filter-brand', 'clm-page-size', 'clm-filter-dense', 'clm-filter-dense-min'].forEach((id) => {
      document.getElementById(id)?.addEventListener('change', () => {
        page = 1;
        void loadBundles();
      });
    });
    document.getElementById('btn-clm-page-prev')?.addEventListener('click', () => {
      if (page > 1) {
        page -= 1;
        void loadBundles();
      }
    });
    document.getElementById('btn-clm-page-next')?.addEventListener('click', () => {
      if (page < pageCount) {
        page += 1;
        void loadBundles();
      }
    });
    updateMergeButton();
  }

  window.cardLinksMasterOnShow = function () {
    void refreshStatus().then(() => loadBundles()).catch((e) => setStats(e.message || String(e)));
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bindUi);
  } else {
    bindUi();
  }
})();
