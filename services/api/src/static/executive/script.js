// Global state
let currentReportId = null;
let reportChartsDoc = null;

// Load reports on page load
document.addEventListener('DOMContentLoaded', () => {
    // Configure marked to use GFM (GitHub Flavored Markdown) which includes tables
    marked.setOptions({
        gfm: true,
        breaks: true,
        tables: true
    });
    
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
            printWindow.onload = function() {
                printWindow.focus();
                printWindow.print();
                // Optional: close window after print
                // printWindow.close();
            };
        }
    });
}
