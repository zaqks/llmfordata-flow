document.addEventListener('DOMContentLoaded', () => {
    setupThemeToggle();
});

function setupThemeToggle() {
    const themeToggleBtn = document.getElementById('theme-toggle');
    const sunIcon = document.getElementById('theme-icon-sun');
    const moonIcon = document.getElementById('theme-icon-moon');

    function updateIcons() {
        const isDark = document.documentElement.classList.contains('dark');
        if (isDark) {
            sunIcon.classList.remove('hidden');
            moonIcon.classList.add('hidden');
        } else {
            sunIcon.classList.add('hidden');
            moonIcon.classList.remove('hidden');
        }
    }

    // Initial icon state
    updateIcons();

    themeToggleBtn.addEventListener('click', () => {
        document.documentElement.classList.toggle('dark');
        const isDark = document.documentElement.classList.contains('dark');
        localStorage.plomberyTheme = isDark ? 'dark' : 'light';
        updateIcons();
    });
}
