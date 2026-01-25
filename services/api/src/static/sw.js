// Service Worker for Push Notifications

console.log('[SW] Service worker script loaded');

// Install event - activate immediately
self.addEventListener('install', function(event) {
    console.log('[SW] Installing service worker...');
    event.waitUntil(self.skipWaiting().then(() => {
        console.log('[SW] skipWaiting completed');
    }));
});

// Activate event - claim all clients immediately
self.addEventListener('activate', function(event) {
    console.log('[SW] Activating service worker...');
    event.waitUntil(self.clients.claim().then(() => {
        console.log('[SW] clients.claim completed - service worker is now active and controlling pages');
    }));
});

self.addEventListener('push', function(event) {
    console.log('[SW] Push notification received:', event);
    
    let notificationData = {
        title: 'New Report Available',
        body: 'A new analysis report has been generated.',
        icon: '/static/logo.svg',
        badge: '/static/logo.svg',
        vibrate: [200, 100, 200],
        data: {
            url: '/executive'
        },
        actions: [
            {
                action: 'view',
                title: 'View Report'
            },
            {
                action: 'close',
                title: 'Dismiss'
            }
        ]
    };

    // Try to parse data if provided
    if (event.data) {
        try {
            const data = event.data.json();
            notificationData.title = data.title || notificationData.title;
            notificationData.body = data.body || notificationData.body;
            if (data.url) {
                notificationData.data.url = data.url;
            }
        } catch (e) {
            console.log('[SW] Could not parse notification data:', e);
            notificationData.body = event.data.text();
        }
    }

    const promiseChain = self.registration.showNotification(
        notificationData.title,
        notificationData
    );

    event.waitUntil(promiseChain);
});

self.addEventListener('notificationclick', function(event) {
    console.log('[SW] Notification clicked:', event);
    event.notification.close();

    if (event.action === 'view' || !event.action) {
        const targetPath = event.notification.data.url || '/executive';
        const urlToOpen = new URL(targetPath, self.location.origin).href;
        
        console.log('[SW] Opening URL:', urlToOpen);
        
        event.waitUntil(
            clients.matchAll({
                type: 'window',
                includeUncontrolled: true
            }).then(function(clientList) {
                console.log('[SW] Found clients:', clientList.length);
                
                // Check if there's already a window open with the same path
                for (let i = 0; i < clientList.length; i++) {
                    const client = clientList[i];
                    const clientUrl = new URL(client.url);
                    const targetUrl = new URL(urlToOpen);
                    
                    // Match by pathname instead of full URL to handle origin differences
                    if (clientUrl.pathname === targetUrl.pathname && 'focus' in client) {
                        console.log('[SW] Focusing existing client:', client.url);
                        return client.focus();
                    }
                }
                
                // If no matching window found, open a new one
                console.log('[SW] Opening new window');
                if (clients.openWindow) {
                    return clients.openWindow(urlToOpen);
                }
            }).catch(function(err) {
                console.error('[SW] Error handling notification click:', err);
            })
        );
    }
});

self.addEventListener('pushsubscriptionchange', function(event) {
    console.log('[SW] Push subscription changed');
    // Handle subscription refresh if needed
});
