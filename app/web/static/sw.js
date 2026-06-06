const CACHE = 'marketai-v4';

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then(() => self.skipWaiting()));
});

self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then((keys) => Promise.all(
    keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))
  )).then(() => self.clients.claim()));
});

self.addEventListener('fetch', (e) => {
  const u = new URL(e.request.url);
  if (u.origin !== location.origin || e.request.method !== 'GET') return;
  // Never cache HTML entry pages to avoid stale UI/auth state.
  if (u.pathname === '/' || u.pathname === '/app' || u.pathname === '/login' || u.pathname === '/landing') {
    return;
  }
  if (!u.pathname.startsWith('/static/')) return;
  // Network-first for static assets: always try fresh version,
  // fallback to cache only when network is unavailable.
  e.respondWith(
    fetch(e.request)
      .then((r) => {
        if (r.ok) {
          return caches.open(CACHE).then((cache) => {
            cache.put(e.request, r.clone());
            return r;
          });
        }
        return caches.match(e.request).then((cached) => cached || r);
      })
      .catch(() => caches.match(e.request))
  );
});
