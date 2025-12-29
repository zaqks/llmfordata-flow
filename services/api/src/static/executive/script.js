// Global state
let currentReportId = null;
let reportChartsDoc = null;

// Load reports on page load
document.addEventListener('DOMContentLoaded', () => {
    loadReports();
    setupDownloadButton();
});

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
        
        // Render markdown content
        if (reportMd) {
            const markdownText = atob(reportMd.file); // Decode base64
            const htmlContent = marked.parse(markdownText);
            document.getElementById('markdown-content').innerHTML = htmlContent;
        } else {
            document.getElementById('markdown-content').innerHTML = '<p>No report content available</p>';
        }
        
        // Display charts
        const chartsGallery = document.getElementById('charts-gallery');
        chartsGallery.innerHTML = '';
        
        if (pngImages.length > 0) {
            pngImages.forEach(img => {
                const chartContainer = document.createElement('div');
                chartContainer.className = 'chart-item';
                
                const chartLabel = document.createElement('p');
                chartLabel.textContent = img.name;
                
                const chartImage = document.createElement('img');
                chartImage.src = `data:image/png;base64,${img.file}`;
                chartImage.alt = img.name;
                
                chartContainer.appendChild(chartLabel);
                chartContainer.appendChild(chartImage);
                chartsGallery.appendChild(chartContainer);
            });
        } else {
            chartsGallery.innerHTML = '<p>No charts available</p>';
        }
        
        // Show/hide download button
        const downloadBtn = document.getElementById('download-btn');
        if (reportChartsMd) {
            downloadBtn.style.display = 'block';
        } else {
            downloadBtn.style.display = 'none';
        }
        
    } catch (error) {
        console.error('Error loading report:', error);
    }
}

// Setup download button
function setupDownloadButton() {
    const downloadBtn = document.getElementById('download-btn');
    downloadBtn.addEventListener('click', () => {
        if (reportChartsDoc) {
            // Decode base64 and create blob
            const binaryString = atob(reportChartsDoc.file);
            const bytes = new Uint8Array(binaryString.length);
            for (let i = 0; i < binaryString.length; i++) {
                bytes[i] = binaryString.charCodeAt(i);
            }
            const blob = new Blob([bytes], { type: 'text/markdown' });
            
            // Create download link
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'report_charts.md';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }
    });
}
