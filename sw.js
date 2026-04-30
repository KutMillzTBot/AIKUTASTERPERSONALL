// SupervisorTrainer Service Worker v3
const CACHE = "supervisor-v3";
const ASSETS = [
  "./command_center.html",
  "./style.css",
  "./app.js"
];

self.addEventListener("install", e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)));
  self.skipWaiting();
});

self.addEventListener("activate", e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
  ));
  self.clients.claim();
});

self.addEventListener("fetch", e => {
  if (e.request.url.includes("127.0.0.1") || e.request.url.includes("localhost")) return;
  e.respondWith(
    fetch(e.request).then(res => {
      const copy = res.clone();
      caches.open(CACHE).then(c => c.put(e.request, copy));
      return res;
    }).catch(() => caches.match(e.request))
  );
});

// Push notification support
self.addEventListener("push", e => {
  const data = e.data ? e.data.json() : { title: "SupervisorAI", body: "New signal available" };
  e.waitUntil(self.registration.showNotification(data.title, {
    body: data.body,
    icon: "./icon-192.png",
    badge: "./icon-192.png",
    tag: "trade-signal",
    renotify: true,
    data: data
  }));
});

self.addEventListener("notificationclick", e => {
  e.notification.close();
  e.waitUntil(clients.openWindow("./command_center.html"));
});
