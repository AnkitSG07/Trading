(function (root) {
  const HEARTBEAT_INTERVAL_MS = 30 * 1000;
  const DEFAULT_PAGE_ROOM_PREFIX = 'page:';
  const MIN_INVALIDATION_DEBOUNCE_MS = 250;
  const MAX_INVALIDATION_DEBOUNCE_MS = 500;
  const CACHE_INVALIDATION_EVENTS = new Set([
    'trade_update',
    'portfolio_ticker',
    'order_update',
    'price_alert',
    'notification',
    'market_data',
    'job_progress',
    'collaboration',
    'system_notice',
    'feature_flag',
    'audit_event',
    'chat_message',
    'latency_alert'
  ]);

  let socket = null;
  let heartbeatTimer = null;
  const subscriptions = new Set();
  let dataChangedDebounceTimer = null;
  let dataChangedMaxDeadline = null;
  let pendingDataChangedPage = null;

  function scheduleDataChanged(pageHint) {
    const now = Date.now();
    pendingDataChangedPage = pageHint;

    if (!dataChangedDebounceTimer) {
      dataChangedMaxDeadline = now + MAX_INVALIDATION_DEBOUNCE_MS;
      dataChangedDebounceTimer = root.setTimeout(flushDataChanged, MIN_INVALIDATION_DEBOUNCE_MS);
      return;
    }

    const nextRunAt = Math.min(now + MIN_INVALIDATION_DEBOUNCE_MS, dataChangedMaxDeadline);
    clearTimeout(dataChangedDebounceTimer);
    dataChangedDebounceTimer = root.setTimeout(
      flushDataChanged,
      Math.max(0, nextRunAt - now)
    );
  }

  function flushDataChanged() {
    dataChangedDebounceTimer = null;
    dataChangedMaxDeadline = null;
    const pageHint = pendingDataChangedPage;
    pendingDataChangedPage = null;
    triggerDataChanged(pageHint);
  }

  function isRealtimeEnabled() {
    const pageContainerFlag = document.getElementById('page-container')?.dataset?.realtime;
    const bodyFlag = document.body?.dataset?.realtime;
    return pageContainerFlag === 'true' || bodyFlag === 'true';
  }

  function hasSocket() {
    if (!root.io) {
      console.warn('Socket.IO client is not available on window.io');
      return false;
    }
    return true;
  }

  function ensureSocket() {
    if (!isRealtimeEnabled()) {
      return null;
    }

    if (socket || !hasSocket()) {
      return socket;
    }

    socket = root.io({
      transports: ['websocket'],
      withCredentials: true,
      autoConnect: true,
      reconnectionAttempts: 10,
      reconnectionDelayMax: 8000
    });

    bindCoreHandlers();
    return socket;
  }

  function bindCoreHandlers() {
    if (!socket) return;

    socket.on('connect', () => {
      dispatchLifecycleEvent('connected');
      resetPendingInvalidations();
      joinPlannedRooms();
      requestRecentForPlannedRooms();
      startHeartbeat();
    });

    socket.on('disconnect', () => {
      dispatchLifecycleEvent('disconnected');
      resetPendingInvalidations();
      stopHeartbeat();
    });

    socket.on('server_ready', (payload) => {
      dispatchLifecycleEvent('ready', payload);
    });

    socket.on('recent_events', ({ room, events } = {}) => {
      if (!room || !Array.isArray(events)) return;
      events.forEach((evt) => handleRealtimeEvent(evt.event || 'recent_events', evt));
    });

    socket.on('subscription_error', (payload) => {
      dispatchLifecycleEvent('subscription_error', payload);
    });

    socket.on('recent_error', (payload) => {
      dispatchLifecycleEvent('recent_error', payload);
    });

    socket.on('heartbeat_ack', (payload) => {
      dispatchLifecycleEvent('heartbeat_ack', payload);
    });

    const forwardEvents = Array.from(CACHE_INVALIDATION_EVENTS);
    forwardEvents.push('trade_update', 'server_ready');
    forwardEvents.forEach((eventName) => {
      socket.on(eventName, (payload) => handleRealtimeEvent(eventName, payload));
    });
  }

  function handleRealtimeEvent(eventName, payload) {
    try {
      root.dispatchEvent(new CustomEvent('realtime:event', {
        detail: { event: eventName, payload }
      }));
    } catch (err) {
      console.warn('Failed to dispatch realtime event', err);
    }

    if (CACHE_INVALIDATION_EVENTS.has(eventName)) {
      scheduleDataChanged(payload && payload.page ? payload.page : window.location.pathname);
    }
  }

  function triggerDataChanged(pageHint) {
    const pageKey = pageHint || window.location.pathname || 'default';

    try {
      document.dispatchEvent(new CustomEvent('data:changed', { detail: { page: pageKey } }));
    } catch (err) {
      console.warn('Failed to dispatch data:changed from realtime', err);
    }

    try {
      if (typeof root.invalidatePagePrefetchData === 'function') {
        root.invalidatePagePrefetchData();
      }
    } catch (err) {
      console.warn('Failed to invalidate prefetch cache from realtime', err);
    }
  }
  
  function joinPlannedRooms() {
    defaultRooms().forEach((room) => subscribe(room));
  }

  function requestRecentForPlannedRooms() {
    defaultRooms().forEach((room) => requestRecent(room).catch(() => {}));
  }

  function defaultRooms() {
    const rooms = [];
    const pageSlug = document.getElementById('page-container')?.dataset?.page;
    if (pageSlug) {
      rooms.push(DEFAULT_PAGE_ROOM_PREFIX + pageSlug);
    } else {
      rooms.push(DEFAULT_PAGE_ROOM_PREFIX + 'dashboard');
    }
    const userId = document.body?.dataset?.userId;
    if (userId) {
      rooms.push(`acct:${userId}`);
    }
    return rooms;
  }

  function subscribe(room) {
    if (!room) return;
    const instance = ensureSocket();
    if (!instance) return;
    subscriptions.add(room);
    instance.emit('subscribe_room', { room });
  }

  function unsubscribe(room) {
    if (!room || !socket) return;
    subscriptions.delete(room);
    socket.emit('unsubscribe_room', { room });
  }

  function requestRecent(room, limit = 20) {
    return new Promise((resolve, reject) => {
      const instance = ensureSocket();
      if (!instance) {
        reject(new Error('socket not initialized'));
        return;
      }

      const handleRecent = (payload) => {
        if (payload && payload.room === room) {
          instance.off('recent_events', handleRecent);
          resolve(payload.events || []);
        }
      };

      const handleError = (payload) => {
        if (payload && payload.error) {
          instance.off('recent_error', handleError);
          reject(new Error(payload.error));
        }
      };

      instance.on('recent_events', handleRecent);
      instance.on('recent_error', handleError);
      instance.emit('fetch_recent', { room, limit });

      setTimeout(() => {
        instance.off('recent_events', handleRecent);
        instance.off('recent_error', handleError);
        reject(new Error('recent request timed out'));
      }, 5000);
    });
  }

  function startHeartbeat() {
    stopHeartbeat();
    heartbeatTimer = root.setInterval(() => {
      if (!socket || !socket.connected) return;
      socket.emit('heartbeat', { source: 'dashboard' });
    }, HEARTBEAT_INTERVAL_MS);
  }

  function stopHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  function resetPendingInvalidations() {
    if (dataChangedDebounceTimer) {
      clearTimeout(dataChangedDebounceTimer);
      dataChangedDebounceTimer = null;
    }
    dataChangedMaxDeadline = null;
    pendingDataChangedPage = null;
  }
  
  function dispatchLifecycleEvent(name, detail) {
    try {
      root.dispatchEvent(new CustomEvent(`realtime:${name}`, { detail }));
    } catch (err) {
      console.warn('Failed to dispatch realtime lifecycle event', name, err);
    }
  }

  root.realtimeDashboard = {
    connect: ensureSocket,
    subscribe,
    unsubscribe,
    requestRecent,
    isConnected: () => Boolean(socket && socket.connected)
  };

  function onDocumentReady(callback) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', callback, { once: true });
      return;
    }
    callback();
  }

  onDocumentReady(() => {
    if (!isRealtimeEnabled()) return;
    ensureSocket();
  });
})(window);
