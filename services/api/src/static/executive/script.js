// ===================== CONSTANTS & CONFIGURATION =====================
const API_BASE = window.location.origin;

// ===================== GLOBAL STATE =====================
let currentReportId = null;
let reportChartsDoc = null;
let currentPngImages = [];

// ===================== INITIALIZATION =====================
document.addEventListener('DOMContentLoaded', () => {
    // Configure marked to use GFM (GitHub Flavored Markdown) which includes tables
    marked.setOptions({
        gfm: true,
        breaks: true,
        tables: true
    });

    loadKPIs();
    loadReports();
    setupDownloadButton();
    setupThemeToggle();
    setupModals();
});

// ===================== LOADING OVERLAY HELPERS =====================
function showLoadingOverlay() {
    const overlay = document.getElementById('report-loading');
    overlay.classList.add('active');
}

function hideLoadingOverlay() {
    const overlay = document.getElementById('report-loading');
    overlay.classList.remove('active');
}

// ===================== KPI LOADER (NEW ADDITION) =====================
async function loadKPIs() {
    const kpiEndpoints = {
        'kpi-anal-length': { url: `${API_BASE}/api/anal_length`, fallback: '69' },
        'kpi-n-discos': { url: `${API_BASE}/api/n_discos`, fallback: '69' },
        'kpi-n-alerts': { url: `${API_BASE}/api/n_alerts`, fallback: '69' },
        'kpi-n-alerts-w': { url: `${API_BASE}/api/n_alerts_w`, fallback: '69' }
    };

    for (const [id, config] of Object.entries(kpiEndpoints)) {
        const element = document.getElementById(id);
        if (!element) continue;

        try {
            const response = await fetch(config.url);
            if (!response.ok) throw new Error('API error');
            const data = await response.json();

            // Handle different possible response formats
            let value = data;
            if (typeof data === 'object' && data !== null) {
                value = data.value ?? data.count ?? data.total ?? config.fallback;
            }

            // Format number with commas if it's a large number
            if (typeof value === 'number' || !isNaN(value)) {
                const numValue = typeof value === 'number' ? value : parseFloat(value);
                if (numValue >= 1000) {
                    element.textContent = numValue.toLocaleString('en-US');
                } else {
                    element.textContent = value;
                }
            } else {
                element.textContent = value;
            }
        } catch (err) {
            console.warn(`Failed to load KPI ${id}:`, err);
            // Keep the fallback value already in HTML
        }
    }
}

// ===================== ORIGINAL CODE BELOW - UNTOUCHED =====================

function setupModals() {
    const chartsModal = document.getElementById('charts-modal');
    const zoomModal = document.getElementById('zoom-modal');
    const showChartsBtn = document.getElementById('show-charts-btn');
    const closeChartsBtn = document.querySelector('.close-modal-btn');
    const closeZoomBtn = document.querySelector('.close-zoom-btn');

    showChartsBtn.addEventListener('click', () => {
        renderGallery();
        chartsModal.classList.remove('hidden');
    });

    closeChartsBtn.addEventListener('click', () => {
        chartsModal.classList.add('hidden');
    });

    closeZoomBtn.addEventListener('click', () => {
        zoomModal.classList.add('hidden');
    });

    // Close on click outside
    window.addEventListener('click', (e) => {
        if (e.target === chartsModal) {
            chartsModal.classList.add('hidden');
        }
        if (e.target === zoomModal) {
            zoomModal.classList.add('hidden');
        }
    });
}

function renderGallery() {
    const gallery = document.getElementById('charts-gallery');
    gallery.innerHTML = '';

    if (currentPngImages.length > 0) {
        currentPngImages.forEach(img => {
            const item = document.createElement('div');
            item.className = 'chart-item';
            item.onclick = () => openZoom(img);

            const label = document.createElement('p');
            label.textContent = img.name;

            const image = document.createElement('img');
            image.src = `data:image/png;base64,${img.file}`;
            image.alt = img.name;

            item.appendChild(label);
            item.appendChild(image);
            gallery.appendChild(item);
        });
    } else {
        gallery.innerHTML = '<p class="placeholder-text">No charts available</p>';
    }
}

function openZoom(imgData) {
    const zoomModal = document.getElementById('zoom-modal');
    const zoomedImg = document.getElementById('zoomed-chart-img');
    zoomedImg.src = `data:image/png;base64,${imgData.file}`;
    zoomModal.classList.remove('hidden');
}

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

// Fetch and display all reports
async function loadReports() {
    try {
        const response = await fetch('/api/reports');
        const reports = await response.json();

        const reportsList = document.getElementById('reports-list');
        reportsList.innerHTML = '';

        reports.forEach(report => {
            const reportItem = document.createElement('div');
            reportItem.className = 'report-item';
            reportItem.textContent = `Report #${report.id} - ${new Date(report.created_at).toLocaleDateString()}`;
            reportItem.onclick = () => loadReport(report.id);
            reportsList.appendChild(reportItem);
        });
    } catch (error) {
        console.error('Error loading reports:', error);
    }
}

// Load a specific report's documents
async function loadReport(reportId) {
    currentReportId = reportId;

    // Show loading overlay
    showLoadingOverlay();

    try {
        const response = await fetch(`/api/reports/${reportId}/documents`);
        const documents = await response.json();

        // Separate documents by type
        let reportMd = null;
        let reportChartsMd = null;
        const pngImages = [];

        documents.forEach(doc => {
            if (doc.name === 'report.md') {
                reportMd = doc;
            } else if (doc.name === 'report_charts.md') {
                reportChartsMd = doc;
            } else if (doc.name.endsWith('.png')) {
                pngImages.push(doc);
            }
        });

        // Store report_charts.md for download
        reportChartsDoc = reportChartsMd;
        currentPngImages = pngImages;

        // Render markdown content
        if (reportMd) {
            const markdownText = atob(reportMd.file); // Decode base64
            const htmlContent = marked.parse(markdownText);
            document.getElementById('markdown-content').innerHTML = htmlContent;
        } else {
            document.getElementById('markdown-content').innerHTML = '<p>No report content available</p>';
        }

        // Show/hide buttons
        const downloadBtn = document.getElementById('download-btn');
        const showChartsBtn = document.getElementById('show-charts-btn');

        if (reportChartsMd) {
            downloadBtn.style.display = 'block';
        } else {
            downloadBtn.style.display = 'none';
        }

        if (pngImages.length > 0) {
            showChartsBtn.style.display = 'block';
        } else {
            showChartsBtn.style.display = 'none';
        }

    } catch (error) {
        console.error('Error loading report:', error);
        document.getElementById('markdown-content').innerHTML = '<p class="error">Failed to load report. Please try again.</p>';
    } finally {
        // Hide loading overlay
        hideLoadingOverlay();
    }
}

// Setup download button
function setupDownloadButton() {
    const downloadBtn = document.getElementById('download-btn');
    downloadBtn.addEventListener('click', () => {
        if (reportChartsDoc) {
            // Decode base64 and get markdown text
            const markdownText = atob(reportChartsDoc.file);

            // Convert markdown to HTML
            const htmlContent = marked.parse(markdownText);

            // Create full HTML document with styling
            const fullHtml = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Report ${currentReportId} Charts</title>
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
            line-height: 1.6;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
            color: #24292e;
        }
        img { 
            max-width: 100%; 
            height: auto; 
            display: block; 
            margin: 20px auto; 
        }
        table { 
            border-collapse: collapse; 
            width: 100%; 
            margin: 20px 0; 
        }
        th, td { 
            border: 1px solid #dfe2e5; 
            padding: 6px 13px; 
        }
        th { 
            background-color: #f6f8fa; 
            font-weight: bold; 
            text-align: left;
        }
        tr:nth-child(2n) { 
            background-color: #f6f8fa; 
        }
        code {
            background-color: rgba(27,31,35,.05);
            border-radius: 3px;
            font-size: 85%;
            margin: 0;
            padding: .2em .4em;
        }
        pre {
            background-color: #f6f8fa;
            border-radius: 3px;
            font-size: 85%;
            line-height: 1.45;
            overflow: auto;
            padding: 16px;
        }
    </style>
</head>
<body>
    ${htmlContent}
</body>
</html>`;

            // Create a new window for printing
            const printWindow = window.open('', '_blank');
            printWindow.document.write(fullHtml);
            printWindow.document.close();

            // Wait for content to load then print
            printWindow.onload = function () {
                printWindow.focus();
                printWindow.print();
                // Optional: close window after print
                // printWindow.close();
            };
        }
    });
}