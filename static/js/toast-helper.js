(function (root) {
  const typeMap = {
    success: 'success'
  };

  function ensureContainer() {
    let container = root.document && root.document.getElementById('global-toast-container');
    if (!container && root.document) {
      container = root.document.createElement('div');
      container.id = 'global-toast-container';
      container.className = 'toast-container position-fixed top-0 start-50 translate-middle-x p-3';
      container.style.zIndex = '1080';
      root.document.body.appendChild(container);
    }
    return container;
  }

  function showToast(message, type = 'error', options = {}) {
    if (!root || !root.document || !root.bootstrap || !root.bootstrap.Toast) {
      console.log(`[${type}] ${message}`);
      return;
    }

    const container = ensureContainer();
    if (!container) {
      console.log(`[${type}] ${message}`);
      return;
    }

    const variant = typeMap[type] || 'danger';
    const toastEl = root.document.createElement('div');
    toastEl.className = `toast align-items-center text-bg-${variant} border-0`;
    toastEl.setAttribute('role', 'alert');
    toastEl.setAttribute('aria-live', 'assertive');
    toastEl.setAttribute('aria-atomic', 'true');

    const bodyWrapper = root.document.createElement('div');
    bodyWrapper.className = 'd-flex';

    const body = root.document.createElement('div');
    body.className = 'toast-body';
    body.textContent = message;

    const closeBtn = root.document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'btn-close btn-close-white me-2 m-auto';
    closeBtn.setAttribute('data-bs-dismiss', 'toast');
    closeBtn.setAttribute('aria-label', 'Close');

    bodyWrapper.appendChild(body);
    bodyWrapper.appendChild(closeBtn);
    toastEl.appendChild(bodyWrapper);
    container.appendChild(toastEl);

    const delay = Number.isFinite(options.delay) ? options.delay : 4000;
    const toast = new root.bootstrap.Toast(toastEl, { delay });
    toastEl.addEventListener('hidden.bs.toast', () => toastEl.remove());
    toast.show();
    return toast;
  }

  root.toastHelper = {
    show: showToast,
    success: (message, options) => showToast(message, 'success', options),
    error: (message, options) => showToast(message, 'error', options)
  };

  root.showToast = showToast;
})(typeof window !== 'undefined' ? window : this);
