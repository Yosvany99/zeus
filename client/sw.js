const CACHE = 'zeus-v8';
const SHELL = ['/', '/manifest.json', '/icon.svg', '/icon-maskable.svg'];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE)
      .then(c => c.addAll(SHELL))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// API paths that must always go to network
const API_PATHS = ['/voz', '/estado', '/activar', '/desactivar', '/siguiente/', '/tarea-actual'];

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  const isApi = API_PATHS.some(p => url.pathname.startsWith(p));

  if (isApi) return; // let browser handle API calls normally

  // App shell: network-first, fallback to cache
  e.respondWith(
    fetch(e.request).then(res => {
      if (res.ok && res.type === 'basic') {
        const clone = res.clone();
        caches.open(CACHE).then(c => c.put(e.request, clone));
      }
      return res;
    }).catch(() => caches.match(e.request))
  );
});
