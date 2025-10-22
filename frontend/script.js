// State
let autoRefreshEnabled = false;
let autoRefreshInterval = null;
let currentPeriod = 'last_3_months';
let includeWedding = false;

// Initialize charts
let categoryChart;
let balanceTrendChart;

// API fetch functions
async function fetchCategorySpending() {
    const response = await fetch(`/api/metrics/spending-by-category?period=${currentPeriod}&include_wedding=${includeWedding}`);
    const data = await response.json();
    // Transform data: meta_category -> category, avg_monthly_spend -> amount
    return data.map(item => ({
        category: item.meta_category,
        amount: item.avg_monthly_spend,
        pct: item.pct_of_avg_monthly_spend
    }));
}

async function fetchMonthlyBudgetHistory() {
    const response = await fetch(`/api/metrics/monthly-budget-history?period=${currentPeriod}&include_wedding=${includeWedding}`);
    const data = await response.json();
    return data;
}

async function fetchAverageMonthlyBudget() {
    const response = await fetch(`/api/metrics/average-monthly-budget?period=${currentPeriod}&include_wedding=${includeWedding}`);
    const data = await response.json();
    return data;
}

// Update UI functions
function updateTimestamp() {
    const now = new Date();
    const formatted = now.toISOString().replace('T', ' ').substring(0, 19);
    document.getElementById('timestamp').textContent = `LAST UPDATE: ${formatted} UTC`;
    document.getElementById('lastSync').textContent = now.toTimeString().substring(0, 5);
}

function updateMonthlySummary(budgetData) {
    // Extract income, spending, and cash flow from the budget data
    const income = budgetData.find(item => item.description === 'SALARY');
    const mortgageContrib = budgetData.find(item => item.description === 'MORTGAGE_CONTRIBUTION');
    const cashFlow = budgetData.find(item => item.category === 'CASH_FLOW');
    const spendingItems = budgetData.filter(item => item.category === 'SPENDING');

    const totalIncome = income ? income.amount : 0;
    const totalIncomeWithMortgage = totalIncome + (mortgageContrib ? mortgageContrib.amount : 0);
    const totalExpenses = Math.abs(spendingItems.reduce((sum, item) => sum + item.amount, 0));
    const netCashFlow = cashFlow ? cashFlow.amount : 0;

    document.getElementById('monthlyIncome').textContent = `$${totalIncomeWithMortgage.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    document.getElementById('monthlyExpenses').textContent = `$${totalExpenses.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    document.getElementById('monthlyNet').textContent = `$${netCashFlow.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;

    // Color code based on positive/negative
    const netElement = document.getElementById('monthlyNet');
    netElement.style.color = netCashFlow >= 0 ? '#00ff00' : '#ff0000';
}

function updateBudgetHistoryTable(historyData) {
    const container = document.getElementById('transactionsTable');

    // Sort by year_month descending to show most recent first
    const sortedData = [...historyData].sort((a, b) => b.year_month.localeCompare(a.year_month));

    const table = document.createElement('table');
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');

    ['MONTH', 'SALARY', 'SPENDING', 'SAVINGS', 'CUMULATIVE'].forEach(header => {
        const th = document.createElement('th');
        th.textContent = header;
        headerRow.appendChild(th);
    });

    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    sortedData.forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${row.year_month}</td>
            <td style="color: #00ff00">$${(row.monthly_salary || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}</td>
            <td style="color: #ff0000">$${Math.abs(row.monthly_spending || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}</td>
            <td style="color: ${row.monthly_savings >= 0 ? '#00ff00' : '#ff0000'}">$${(row.monthly_savings || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}</td>
            <td style="color: ${row.cumulative_savings >= 0 ? '#00ff00' : '#ff0000'}">$${(row.cumulative_savings || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}</td>
        `;
        tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    container.innerHTML = '';
    container.appendChild(table);
}

function updateCategoryChart(data) {
    const ctx = document.getElementById('categoryChart');
    
    if (categoryChart) {
    categoryChart.destroy();
    }
    
    categoryChart = new Chart(ctx, {
    type: 'bar',
    data: {
        labels: data.map(d => d.category),
        datasets: [{
        label: 'Spending',
        data: data.map(d => d.amount),
        backgroundColor: 'rgba(0, 255, 0, 0.3)',
        borderColor: '#00ff00',
        borderWidth: 2
        }]
    },
    options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
        legend: {
            display: false
        }
        },
        scales: {
        y: {
            beginAtZero: true,
            grid: {
            color: 'rgba(0, 255, 0, 0.2)'
            },
            ticks: {
            color: '#00ff00',
            font: {
                family: 'Courier New'
            }
            }
        },
        x: {
            grid: {
            color: 'rgba(0, 255, 0, 0.1)'
            },
            ticks: {
            color: '#00ff00',
            font: {
                family: 'Courier New'
            }
            }
        }
        }
    }
    });
}

function updateBalanceTrendChart(historyData) {
    const ctx = document.getElementById('balanceTrendChart');

    if (balanceTrendChart) {
        balanceTrendChart.destroy();
    }

    // Sort data by year_month ascending for chronological order
    const sortedData = [...historyData].sort((a, b) => a.year_month.localeCompare(b.year_month));

    balanceTrendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: sortedData.map(d => d.year_month),
            datasets: [{
                label: 'Cumulative Savings',
                data: sortedData.map(d => d.cumulative_savings || 0),
                borderColor: '#00ff00',
                backgroundColor: 'rgba(0, 255, 0, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.1,
                pointRadius: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    grid: {
                        color: 'rgba(0, 255, 0, 0.2)'
                    },
                    ticks: {
                        color: '#00ff00',
                        font: {
                            family: 'Courier New'
                        },
                        callback: function(value) {
                            return '$' + value.toLocaleString();
                        }
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(0, 255, 0, 0.1)'
                    },
                    ticks: {
                        color: '#00ff00',
                        font: {
                            family: 'Courier New'
                        },
                        maxRotation: 45,
                        minRotation: 45
                    }
                }
            }
        }
    });
}

// Main refresh function
async function refreshData() {
    updateTimestamp();

    try {
        const [categorySpend, budgetHistory, avgBudget] = await Promise.all([
            fetchCategorySpending(),
            fetchMonthlyBudgetHistory(),
            fetchAverageMonthlyBudget()
        ]);

        updateMonthlySummary(avgBudget);
        updateCategoryChart(categorySpend);
        updateBudgetHistoryTable(budgetHistory);
        updateBalanceTrendChart(budgetHistory);
    } catch (error) {
        console.error('Error refreshing data:', error);
        // Update UI to show error state
        document.getElementById('transactionsTable').innerHTML = `<div class="loading" style="color: #ff0000;">ERROR LOADING DATA: ${error.message}</div>`;
    }
}

// Event listeners
document.getElementById('refreshBtn').addEventListener('click', refreshData);

document.getElementById('toggleAutoRefresh').addEventListener('click', function() {
    autoRefreshEnabled = !autoRefreshEnabled;
    this.textContent = `AUTO: ${autoRefreshEnabled ? 'ON' : 'OFF'}`;

    if (autoRefreshEnabled) {
        autoRefreshInterval = setInterval(refreshData, 30000); // Refresh every 30 seconds
    } else {
        if (autoRefreshInterval) {
            clearInterval(autoRefreshInterval);
        }
    }
});

// Period selector
const periodSelect = document.getElementById('periodSelect');
if (periodSelect) {
    periodSelect.addEventListener('change', function() {
        currentPeriod = this.value;
        refreshData();
    });
}

// Wedding toggle
const weddingToggle = document.getElementById('weddingToggle');
if (weddingToggle) {
    weddingToggle.addEventListener('click', function() {
        includeWedding = !includeWedding;
        this.textContent = `WEDDING: ${includeWedding ? 'ON' : 'OFF'}`;
        refreshData();
    });
}

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
    if (e.key === 'r' || e.key === 'R') {
        refreshData();
    }
});

// Initial load
refreshData();