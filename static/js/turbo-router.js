
(function () {
  const cache = new Map();
  const CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes
  const parser = new DOMParser();

  // Expose cache invalidation API globally
  window.turboRouter = {
    clearCache: function (url) {
      if (url) {
        cache.delete(url);
      } else {
        cache.clear();
      }
    },
    clearCurrentPage: function () {
      cache.delete(window.location.href);
    },
    clearExpiredCache: function () {
      const now = Date.now();
      for (const [url, entry] of cache.entries()) {
        if (isExpired(entry, now)) {
          cache.delete(url);
        }
      }
    },
    refreshExpiredCache: async function () {
      const now = Date.now();
      const entries = Array.from(cache.entries());
      for (const [url, entry] of entries) {
        if (isExpired(entry, now)) {
          try {
            await refreshEntry(url);
          } catch (e) {
            console.warn('Failed to refresh expired cache entry:', url, e);
            cache.delete(url);
          }
        }
      }
    }
  };

  // Listen for data change events to invalidate cache
  document.addEventListener('data:changed', (e) => {
    // Clear current page cache when data changes
    cache.delete(window.location.href);
    // Also clear specific page if specified
    if (e.detail && e.detail.page) {
      const pageUrl = new URL(e.detail.page, window.location.origin).href;
      cache.delete(pageUrl);
    }
  });

  // Prefetch on hover
  document.addEventListener('mouseover', (e) => {
    const link = e.target.closest('a.sidebar-item');
    if (link && link.href && link.href.startsWith(window.location.origin) && !getCacheEntry(link.href)) {
      prefetch(link.href);
    }
  });

  // Handle click
  document.addEventListener('click', async (e) => {
    const link = e.target.closest('a.sidebar-item');
    if (!link || !link.href || !link.href.startsWith(window.location.origin) || e.ctrlKey || e.metaKey || e.shiftKey || e.altKey) return;

    e.preventDefault();
    const url = link.href;

    // Don't reload if already on the page
    if (url === window.location.href) return;

    // Show loading state if not cached
    if (!getCacheEntry(url)) {
      document.body.classList.add('loading');
    }

    try {
      const cachedEntry = getCacheEntry(url);
      const html = cachedEntry ? cachedEntry.html : await fetchPage(url);
      updatePage(html, url);
      history.pushState({}, '', url);
    } catch (err) {
      console.error('Turbo navigation failed, falling back to reload:', err);
      window.location.href = url; // Fallback
    } finally {
      document.body.classList.remove('loading');
    }
  });

  // Handle back/forward
  window.addEventListener('popstate', async () => {
    try {
      const html = await fetchPage(window.location.href);
      updatePage(html, window.location.href);
    } catch (err) {
      window.location.reload();
    }
  });

  async function prefetch(url) {
    try {
      const res = await fetch(url);
      if (res.ok) {
        const text = await res.text();
        storeInCache(url, text);
      }
    } catch (e) { console.warn('Prefetch failed:', e); }
  }

  async function fetchPage(url) {
    const cached = getCacheEntry(url);
    if (cached) return cached.html;
    return refreshEntry(url);
  }

  async function refreshEntry(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const text = await res.text();
    storeInCache(url, text);
    return text;
  }

  function storeInCache(url, html) {
    cache.set(url, { html, timestamp: Date.now() });
  }

  function getCacheEntry(url) {
    const entry = cache.get(url);
    if (!entry) return null;

    if (isExpired(entry)) {
      cache.delete(url);
      return null;
    }

    return entry;
  }

  function isExpired(entry, now = Date.now()) {
    return now - entry.timestamp > CACHE_TTL_MS;
  }

  function updatePage(html, url) {
    const doc = parser.parseFromString(html, 'text/html');

    // Swap content
    const newContent = doc.getElementById('page-container');
    const oldContent = document.getElementById('page-container');

    if (newContent && oldContent) {
      // 1. Update title
      document.title = doc.title;

      // 2. Update body attributes (e.g. data-user-id)
      if (doc.body.dataset.userId) {
        document.body.dataset.userId = doc.body.dataset.userId;
      }

      // 3. Swap main container content
      oldContent.innerHTML = newContent.innerHTML;
      oldContent.setAttribute('data-page', newContent.getAttribute('data-page'));

      // 4. Re-execute scripts in the new content
      // Scripts inserted via innerHTML are not executed by default.
      // We must manually recreate and insert them.
      const scripts = oldContent.querySelectorAll('script');
      scripts.forEach(oldScript => {
        const newScript = document.createElement('script');
        Array.from(oldScript.attributes).forEach(attr => newScript.setAttribute(attr.name, attr.value));
        newScript.textContent = oldScript.textContent;
        oldScript.parentNode.replaceChild(newScript, oldScript);
      });

      // 5. Update active sidebar item
      document.querySelectorAll('.sidebar-item').forEach(item => {
        // Simple check: does the href match the current path?
        // We use .pathname to compare relative paths
        try {
          const itemPath = new URL(item.href).pathname;
          const currentPath = new URL(url).pathname;
          item.classList.toggle('active', itemPath === currentPath);
        } catch (e) {
          item.classList.remove('active');
        }
      });

      // 6. Re-initialize global components if needed
      // (e.g. tooltips, dropdowns that might be inside the page-container)
      if (window.bootstrap && window.bootstrap.Tooltip) {
        const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
        [...tooltipTriggerList].forEach(el => new bootstrap.Tooltip(el));
      }

      // 7. Trigger a custom event for page-specific initialization
      const event = new Event('turbo:load');
      document.dispatchEvent(event);

      // Also trigger DOMContentLoaded for scripts that rely on it
      // (Note: this is a simulation, not a real browser event)
      // A better pattern is for pages to expose an init function
    } else {
      throw new Error('Page container not found');
    }
  }
})();
