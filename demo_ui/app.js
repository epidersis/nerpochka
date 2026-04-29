/**
 * АЦК-Аналитика - Конструктор аналитических выборок
 * Интерфейс для хакатона БФТ и Минфин АО
 */

// ============================================
// Sample Data (имитация данных из CSV)
// ============================================
const sampleData = [
    {
        kcsr: "08.3.02.97070",
        name: "Мероприятия по проведению оздоровительной кампании детей",
        plan: 857000.00,
        kassa: 425000.00,
        obligations: 500000.00,
        contracts: 450000.00,
        payments: 425000.00
    },
    {
        kcsr: "02.2.01.97003",
        name: "Реализация мероприятий в транспортной сфере (дорожное хозяйство)",
        plan: 133752149.85,
        kassa: 89500000.00,
        obligations: 95000000.00,
        contracts: 92000000.00,
        payments: 89500000.00
    },
    {
        kcsr: "88.8.00.61050",
        name: "Реализация мер по списанию задолженности по бюджетным кредитам",
        plan: 3015023766.63,
        kassa: 2500000000.00,
        obligations: 2600000000.00,
        contracts: 2550000000.00,
        payments: 2500000000.00
    },
    {
        kcsr: "13.2.01.97003",
        name: "Реализация мероприятий в транспортной сфере",
        plan: 133752149.85,
        kassa: 75000000.00,
        obligations: 80000000.00,
        contracts: 78000000.00,
        payments: 75000000.00
    },
    {
        kcsr: "05.2.01.97002",
        name: "Коммунальная инфраструктура и благоустройство территорий",
        plan: 77437662.85,
        kassa: 45000000.00,
        obligations: 50000000.00,
        contracts: 48000000.00,
        payments: 45000000.00
    }
];

// ============================================
// State Management
// ============================================
let state = {
    selectedObjects: [],
    selectedSources: ['planning', 'finance', 'goszakaz'],
    selectedIndicators: ['plan', 'kassa', 'obligations', 'contracts', 'payments'],
    periodType: 'single',
    singleDate: '2025-01-01',
    dateRange1: { start: '2024-01-01', end: '2024-12-31' },
    dateRange2: { start: '2025-01-01', end: '2025-12-31' },
    currentView: 'table',
    charts: {}
};

// ============================================
// DOM Elements
// ============================================
const elements = {
    smartSearch: document.getElementById('smartSearch'),
    voiceBtn: document.getElementById('voiceBtn'),
    searchSubmit: document.getElementById('searchSubmit'),
    objectSearch: document.getElementById('objectSearch'),
    objectList: document.getElementById('objectList'),
    selectedObjects: document.getElementById('selectedObjects'),
    singlePeriod: document.getElementById('singlePeriod'),
    comparePeriod: document.getElementById('comparePeriod'),
    resultsSection: document.getElementById('resultsSection'),
    tableView: document.getElementById('tableView'),
    chartView: document.getElementById('chartView'),
    tableBody: document.getElementById('tableBody'),
    loadingOverlay: document.getElementById('loadingOverlay'),
    generateReport: document.getElementById('generateReport'),
    clearFilters: document.getElementById('clearFilters'),
    exportExcel: document.getElementById('exportExcel')
};

// ============================================
// Initialization
// ============================================
document.addEventListener('DOMContentLoaded', () => {
    initEventListeners();
    initObjectSelection();
    initIndicatorSelection();
    initSourceSelection();
    initPeriodTabs();
    initViewToggle();
    initVoiceSearch();
    initHintClicks();
});

// ============================================
// Event Listeners
// ============================================
function initEventListeners() {
    // Search
    elements.searchSubmit.addEventListener('click', handleSearch);
    elements.smartSearch.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleSearch();
    });
    
    // Report generation
    elements.generateReport.addEventListener('click', generateReport);
    elements.clearFilters.addEventListener('click', clearFilters);
    elements.exportExcel.addEventListener('click', exportToExcel);
}

// ============================================
// Voice Search (Web Speech API)
// ============================================
function initVoiceSearch() {
    if ('webkitSpeechRecognition' in window || 'SpeechRecognition' in window) {
        const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
        const recognition = new SpeechRecognition();
        
        recognition.lang = 'ru-RU';
        recognition.continuous = false;
        recognition.interimResults = false;
        
        elements.voiceBtn.addEventListener('click', () => {
            if (elements.voiceBtn.classList.contains('listening')) {
                recognition.stop();
            } else {
                recognition.start();
            }
        });
        
        recognition.onstart = () => {
            elements.voiceBtn.classList.add('listening');
        };
        
        recognition.onend = () => {
            elements.voiceBtn.classList.remove('listening');
        };
        
        recognition.onresult = (event) => {
            const transcript = event.results[0][0].transcript;
            elements.smartSearch.value = transcript;
            parseVoiceQuery(transcript);
        };
        
        recognition.onerror = (event) => {
            console.error('Speech recognition error:', event.error);
            elements.voiceBtn.classList.remove('listening');
        };
    } else {
        elements.voiceBtn.style.display = 'none';
    }
}

// ============================================
// Parse Voice Query (Simple NLP simulation)
// ============================================
function parseVoiceQuery(query) {
    const lowerQuery = query.toLowerCase();
    
    // Detect indicators
    if (lowerQuery.includes('план') || lowerQuery.includes('лимит')) {
        toggleIndicator('plan', true);
    }
    if (lowerQuery.includes('касс') || lowerQuery.includes('факт')) {
        toggleIndicator('kassa', true);
    }
    if (lowerQuery.includes('обязательств')) {
        toggleIndicator('obligations', true);
    }
    if (lowerQuery.includes('контракт') || lowerQuery.includes('договор')) {
        toggleIndicator('contracts', true);
    }
    if (lowerQuery.includes('платеж') || lowerQuery.includes('выплат')) {
        toggleIndicator('payments', true);
    }
    
    // Detect comparison
    if (lowerQuery.includes('сравн') || lowerQuery.includes('vs')) {
        document.querySelector('[data-period="compare"]').click();
    }
    
    // Detect objects
    if (lowerQuery.includes('дорож') || lowerQuery.includes('транспорт')) {
        selectObject('02.2.01.97003');
    }
    if (lowerQuery.includes('образован') || lowerQuery.includes('оздоров')) {
        selectObject('08.3.02.97070');
    }
    if (lowerQuery.includes('коммунальн')) {
        selectObject('05.2.01.97002');
    }
    
    // Auto-generate after parsing
    setTimeout(generateReport, 500);
}

// ============================================
// Object Selection
// ============================================
function initObjectSelection() {
    const items = elements.objectList.querySelectorAll('.object-item');
    
    items.forEach(item => {
        item.addEventListener('click', () => {
            const code = item.dataset.code;
            const name = item.querySelector('.name').textContent;
            selectObject(code, name);
        });
    });
    
    // Search filter
    elements.objectSearch.addEventListener('input', (e) => {
        const filter = e.target.value.toLowerCase();
        items.forEach(item => {
            const text = item.textContent.toLowerCase();
            item.style.display = text.includes(filter) ? 'flex' : 'none';
        });
    });
}

function selectObject(code, name) {
    if (!state.selectedObjects.find(o => o.code === code)) {
        state.selectedObjects.push({ code, name });
        renderTags();
        
        // Highlight selected item
        const item = elements.objectList.querySelector(`[data-code="${code}"]`);
        if (item) item.classList.add('selected');
    }
}

function removeObject(code) {
    state.selectedObjects = state.selectedObjects.filter(o => o.code !== code);
    renderTags();
    
    const item = elements.objectList.querySelector(`[data-code="${code}"]`);
    if (item) item.classList.remove('selected');
}

function renderTags() {
    elements.selectedObjects.innerHTML = state.selectedObjects.map(obj => `
        <span class="tag">
            ${obj.code}
            <span class="tag-remove" onclick="removeObject('${obj.code}')">×</span>
        </span>
    `).join('');
}

// ============================================
// Indicator Selection
// ============================================
function initIndicatorSelection() {
    const items = document.querySelectorAll('.indicator-item');
    
    items.forEach(item => {
        item.addEventListener('click', () => {
            const checkbox = item.querySelector('input[type="checkbox"]');
            const value = checkbox.value;
            
            item.classList.toggle('active');
            checkbox.checked = !checkbox.checked;
            
            if (checkbox.checked) {
                state.selectedIndicators.push(value);
            } else {
                state.selectedIndicators = state.selectedIndicators.filter(i => i !== value);
            }
        });
    });
}

function toggleIndicator(value, forceState) {
    const item = document.querySelector(`.indicator-item input[value="${value}"]`).closest('.indicator-item');
    const checkbox = item.querySelector('input[type="checkbox"]');
    
    if (forceState && !checkbox.checked) {
        item.classList.add('active');
        checkbox.checked = true;
        if (!state.selectedIndicators.includes(value)) {
            state.selectedIndicators.push(value);
        }
    }
}

// ============================================
// Source Selection
// ============================================
function initSourceSelection() {
    const items = document.querySelectorAll('.checkbox-item');
    
    items.forEach(item => {
        item.addEventListener('click', () => {
            const checkbox = item.querySelector('input[type="checkbox"]');
            const value = checkbox.value;
            
            item.classList.toggle('active');
            checkbox.checked = !checkbox.checked;
            
            if (checkbox.checked) {
                state.selectedSources.push(value);
            } else {
                state.selectedSources = state.selectedSources.filter(s => s !== value);
            }
        });
    });
}

// ============================================
// Period Tabs
// ============================================
function initPeriodTabs() {
    const tabs = document.querySelectorAll('.tab-btn');
    
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            
            state.periodType = tab.dataset.period;
            
            if (state.periodType === 'single') {
                elements.singlePeriod.classList.remove('hidden');
                elements.comparePeriod.classList.add('hidden');
            } else {
                elements.singlePeriod.classList.add('hidden');
                elements.comparePeriod.classList.remove('hidden');
            }
        });
    });
    
    // Date inputs
    document.getElementById('singleDate').addEventListener('change', (e) => {
        state.singleDate = e.target.value;
    });
    
    document.getElementById('date1Start').addEventListener('change', (e) => {
        state.dateRange1.start = e.target.value;
    });
    document.getElementById('date1End').addEventListener('change', (e) => {
        state.dateRange1.end = e.target.value;
    });
    document.getElementById('date2Start').addEventListener('change', (e) => {
        state.dateRange2.start = e.target.value;
    });
    document.getElementById('date2End').addEventListener('change', (e) => {
        state.dateRange2.end = e.target.value;
    });
}

// ============================================
// View Toggle
// ============================================
function initViewToggle() {
    const toggles = document.querySelectorAll('.toggle-btn');
    
    toggles.forEach(btn => {
        btn.addEventListener('click', () => {
            toggles.forEach(t => t.classList.remove('active'));
            btn.classList.add('active');
            
            state.currentView = btn.dataset.view;
            
            if (state.currentView === 'table') {
                elements.tableView.classList.remove('hidden');
                elements.chartView.classList.add('hidden');
            } else {
                elements.tableView.classList.add('hidden');
                elements.chartView.classList.remove('hidden');
                renderCharts();
            }
        });
    });
}

// ============================================
// Hint Clicks
// ============================================
function initHintClicks() {
    const hints = document.querySelectorAll('.hint');
    
    hints.forEach(hint => {
        hint.addEventListener('click', () => {
            elements.smartSearch.value = hint.textContent.replace(/^[📊📅🏗️]\s*/, '');
            handleSearch();
        });
    });
}

// ============================================
// Handle Search
// ============================================
function handleSearch() {
    const query = elements.smartSearch.value.trim();
    if (query) {
        parseVoiceQuery(query);
    }
}

// ============================================
// Generate Report
// ============================================
function generateReport() {
    // Show loading
    elements.loadingOverlay.classList.remove('hidden');
    
    // Simulate data loading
    setTimeout(() => {
        elements.loadingOverlay.classList.add('hidden');
        elements.resultsSection.classList.remove('hidden');
        
        // Filter data based on selection
        let filteredData = sampleData;
        
        if (state.selectedObjects.length > 0) {
            filteredData = sampleData.filter(d => 
                state.selectedObjects.find(o => o.code === d.kcsr)
            );
        }
        
        renderTable(filteredData);
        
        // Scroll to results
        elements.resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 1500);
}

// ============================================
// Render Table
// ============================================
function renderTable(data) {
    if (data.length === 0) {
        elements.tableBody.innerHTML = `
            <tr>
                <td colspan="8" style="text-align: center; padding: 3rem;">
                    <p style="color: var(--text-muted);">Нет данных для отображения</p>
                </td>
            </tr>
        `;
        return;
    }
    
    elements.tableBody.innerHTML = data.map(row => {
        const percent = ((row.kassa / row.plan) * 100).toFixed(1);
        let percentClass = 'good';
        if (percent < 50) percentClass = 'danger';
        else if (percent < 75) percentClass = 'warning';
        
        return `
            <tr>
                <td><code style="background: var(--bg-tertiary); padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.8125rem;">${row.kcsr}</code></td>
                <td>${row.name}</td>
                <td class="numeric">${formatNumber(row.plan)}</td>
                <td class="numeric">${formatNumber(row.kassa)}</td>
                <td class="numeric">${formatNumber(row.obligations)}</td>
                <td class="numeric">${formatNumber(row.contracts)}</td>
                <td class="numeric">${formatNumber(row.payments)}</td>
                <td class="percent ${percentClass}">${percent}%</td>
            </tr>
        `;
    }).join('');
    
    // Destroy old charts if they exist
    destroyCharts();
}

// ============================================
// Format Number
// ============================================
function formatNumber(num) {
    return new Intl.NumberFormat('ru-RU', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(num);
}

// ============================================
// Render Charts
// ============================================
function renderCharts() {
    // Wait for DOM to update
    setTimeout(() => {
        renderBarChart();
        renderPieChart();
        renderLineChart();
    }, 100);
}

function renderBarChart() {
    const ctx = document.getElementById('mainChart').getContext('2d');
    
    // Destroy existing chart
    if (state.charts.bar) state.charts.bar.destroy();
    
    const labels = sampleData.map(d => d.kcsr);
    
    state.charts.bar = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'План',
                    data: sampleData.map(d => d.plan),
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 1
                },
                {
                    label: 'Касса',
                    data: sampleData.map(d => d.kassa),
                    backgroundColor: 'rgba(72, 187, 120, 0.8)',
                    borderColor: 'rgba(72, 187, 120, 1)',
                    borderWidth: 1
                },
                {
                    label: 'Обязательства',
                    data: sampleData.map(d => d.obligations),
                    backgroundColor: 'rgba(237, 137, 54, 0.8)',
                    borderColor: 'rgba(237, 137, 54, 1)',
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        usePointStyle: true,
                        padding: 15
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: (value) => formatNumber(value)
                    }
                }
            }
        }
    });
}

function renderPieChart() {
    const ctx = document.getElementById('pieChart').getContext('2d');
    
    if (state.charts.pie) state.charts.pie.destroy();
    
    state.charts.pie = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: sampleData.map(d => d.kcsr),
            datasets: [{
                data: sampleData.map(d => d.kassa),
                backgroundColor: [
                    'rgba(102, 126, 234, 0.8)',
                    'rgba(72, 187, 120, 0.8)',
                    'rgba(237, 137, 54, 0.8)',
                    'rgba(159, 122, 234, 0.8)',
                    'rgba(66, 153, 225, 0.8)'
                ],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        usePointStyle: true,
                        padding: 10,
                        boxWidth: 12
                    }
                }
            }
        }
    });
}

function renderLineChart() {
    const ctx = document.getElementById('lineChart').getContext('2d');
    
    if (state.charts.line) state.charts.line.destroy();
    
    // Mock time series data
    const months = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];
    
    state.charts.line = new Chart(ctx, {
        type: 'line',
        data: {
            labels: months,
            datasets: [
                {
                    label: 'План',
                    data: months.map((_, i) => sampleData[2].plan * (i + 1) / 12),
                    borderColor: 'rgba(102, 126, 234, 1)',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Касса',
                    data: months.map((_, i) => sampleData[2].kassa * (i + 1) / 12 * (0.8 + Math.random() * 0.2)),
                    borderColor: 'rgba(72, 187, 120, 1)',
                    backgroundColor: 'rgba(72, 187, 120, 0.1)',
                    tension: 0.4,
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'bottom'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: (value) => formatNumber(value)
                    }
                }
            }
        }
    });
}

function destroyCharts() {
    Object.values(state.charts).forEach(chart => chart.destroy());
    state.charts = {};
}

// ============================================
// Clear Filters
// ============================================
function clearFilters() {
    state.selectedObjects = [];
    state.selectedSources = ['planning', 'finance', 'goszakaz'];
    state.selectedIndicators = ['plan', 'kassa', 'obligations', 'contracts', 'payments'];
    state.periodType = 'single';
    
    // Reset UI
    renderTags();
    elements.objectList.querySelectorAll('.object-item').forEach(item => {
        item.classList.remove('selected');
    });
    
    document.querySelectorAll('.checkbox-item').forEach(item => {
        item.classList.add('active');
        item.querySelector('input').checked = true;
    });
    
    document.querySelectorAll('.indicator-item').forEach(item => {
        item.classList.add('active');
        item.querySelector('input').checked = true;
    });
    
    document.querySelector('[data-period="single"]').click();
    document.getElementById('singleDate').value = '2025-01-01';
    
    elements.resultsSection.classList.add('hidden');
    elements.smartSearch.value = '';
}

// ============================================
// Export to Excel (CSV)
// ============================================
function exportToExcel() {
    const headers = ['КЦСР', 'Наименование объекта', 'План (руб.)', 'Касса (руб.)', 'Обязательства (руб.)', 'Контракты (руб.)', 'Платежи (руб.)', '% исполнения'];
    
    let csvContent = headers.join(';') + '\n';
    
    sampleData.forEach(row => {
        const percent = ((row.kassa / row.plan) * 100).toFixed(1);
        const rowData = [
            row.kcsr,
            `"${row.name}"`,
            row.plan.toFixed(2).replace('.', ','),
            row.kassa.toFixed(2).replace('.', ','),
            row.obligations.toFixed(2).replace('.', ','),
            row.contracts.toFixed(2).replace('.', ','),
            row.payments.toFixed(2).replace('.', ','),
            percent + '%'
        ];
        csvContent += rowData.join(';') + '\n';
    });
    
    // Create download link
    const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    
    link.setAttribute('href', url);
    link.setAttribute('download', `analitika_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// Make functions available globally for inline handlers
window.removeObject = removeObject;
