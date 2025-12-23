(function () {
  const sidebar = document.querySelector('.admin-sidebar');
  const toggle = document.getElementById('sidebar-toggle');
  if (sidebar && toggle) {
    toggle.addEventListener('click', function () {
      sidebar.classList.toggle('is-open');
    });
  }

  document.addEventListener('click', function (event) {
    if (!sidebar || !toggle) return;
    const isClickInside = sidebar.contains(event.target) || toggle.contains(event.target);
    if (!isClickInside && sidebar.classList.contains('is-open')) {
      sidebar.classList.remove('is-open');
    }
  });

  window.addEventListener('resize', function () {
    if (window.innerWidth >= 992 && sidebar) {
      sidebar.classList.remove('is-open');
    }
  });
})();
