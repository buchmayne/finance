// TypeScript-style code (will need transpilation in real project)

// Data types
const SampleTransaction = {
    id: '',
    date: '',
    description: '',
    category: '',
    amount: 0,
    account: ''
};

const SampleBalanceData = {
    date: '',
    balance: 0
};

const SampleCategorySpend = {
    category: '',
    amount: 0
};

// State
let autoRefreshEnabled = false;
let autoRefreshInterval = null;

// Initialize charts
let categoryChart;
let balanceTrendChart;

// API fetch functions (replace with your actual API endpoints)
async function fetchAccountBalances() {
    // Replace with: return fetch('/api/balances').then(r => r.json());
    return new Promise(resolve => {
    setTimeout(() => {
        resolve({
        total: 45782.50,
        checking: 12450.00,
        savings: 33332.50
        });
    }, 500);
    });
}

async function fetchMonthlySummary() {
    // Replace with: return fetch('/api/summary/monthly').then(r => r.json());
    return new Promise(resolve => {
    setTimeout(() => {
        resolve({
        income: 8500.00,
        expenses: 5234.67,
        net: 3265.33,
        budgetRemaining: 1450.00
        });
    }, 500);
    });
}

async function fetchRecentTransactions() {
    // Replace with: return fetch('/api/transactions/recent').then(r => r.json());
    return new Promise(resolve => {
    setTimeout(() => {
        resolve([
        { id: 'TX-9821', date: '2025-10-12', description: 'GROCERY STORE', category: 'FOOD', amount: -87.43, account: 'CHK' },
        { id: 'TX-9820', date: '2025-10-11', description: 'PAYCHECK DEPOSIT', category: 'INCOME', amount: 4250.00, account: 'CHK' },
        { id: 'TX-9819', date: '2025-10-10', description: 'ELECTRIC UTILITY', category: 'UTILITIES', amount: -142.56, account: 'CHK' },
        { id: 'TX-9818', date: '2025-10-09', description: 'GAS STATION', category: 'TRANSPORT', amount: -52.00, account: 'CHK' },
        { id: 'TX-9817', date: '2025-10-08', description: 'RESTAURANT', category: 'FOOD', amount: -65.20, account: 'CHK' },
        { id: 'TX-9816', date: '2025-10-07', description: 'ONLINE SHOPPING', category: 'SHOPPING', amount: -129.99, account: 'CHK' },
        { id: 'TX-9815', date: '2025-10-06', description: 'RENT PAYMENT', category: 'HOUSING', amount: -1850.00, account: 'CHK' },
        { id: 'TX-9814', date: '2025-10-05', description: 'COFFEE SHOP', category: 'FOOD', amount: -8.50, account: 'CHK' }
        ]);
    }, 600);
    });
}

async function fetchCategorySpending() {
    // Replace with: return fetch('/api/spending/categories').then(r => r.json());
    const response = await fetch('/api/metrics/spending-by-category').then(r => r.json());
    return response.json()
    // return new Promise(resolve => {
    // setTimeout(() => {
    //     resolve([
    //     { category: 'HOUSING', amount: 1850.00 },
    //     { category: 'FOOD', amount: 682.45 },
    //     { category: 'TRANSPORT', amount: 245.80 },
    //     { category: 'UTILITIES', amount: 284.50 },
    //     { category: 'SHOPPING', amount: 456.20 },
    //     { category: 'ENTERTAINMENT', amount: 189.00 },
    //     { category: 'OTHER', amount: 312.50 }
    //     ]);
    // }, 550);
    // });
}

async function fetchBalanceTrend() {
    // Replace with: return fetch('/api/balance/trend?days=30').then(r => r.json());
    return new Promise(resolve => {
    setTimeout(() => {
        const data = [];
        let balance = 42000;
        for (let i = 30; i >= 0; i--) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        balance += (Math.random() - 0.4) * 500;
        data.push({
            date: date.toISOString().split('T')[0],
            balance: Math.round(balance * 100) / 100
        });
        }
        resolve(data);
    }, 600);
    });
}

// Update UI functions
function updateTimestamp() {
    const now = new Date();
    const formatted = now.toISOString().replace('T', ' ').substring(0, 19);
    document.getElementById('timestamp').textContent = `LAST UPDATE: ${formatted} UTC`;
    document.getElementById('lastSync').textContent = now.toTimeString().substring(0, 5);
}

function updateBalances(data) {
    document.getElementById('totalBalance').textContent = `$${data.total.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    document.getElementById('checking').textContent = `$${data.checking.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    document.getElementById('savings').textContent = `$${data.savings.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
}

function updateMonthlySummary(data) {
    document.getElementById('monthlyIncome').textContent = `$${data.income.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    document.getElementById('monthlyExpenses').textContent = `$${data.expenses.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    document.getElementById('monthlyNet').textContent = `$${data.net.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    document.getElementById('budgetRemain').textContent = `$${data.budgetRemaining.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
}

function updateTransactionsTable(transactions) {
    const container = document.getElementById('transactionsTable');
    
    const table = document.createElement('table');
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    
    ['DATE', 'ID', 'DESCRIPTION', 'CATEGORY', 'AMOUNT', 'ACCT'].forEach(header => {
    const th = document.createElement('th');
    th.textContent = header;
    th.addEventListener('click', () => sortTable(transactions, header.toLowerCase()));
    headerRow.appendChild(th);
    });
    
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    const tbody = document.createElement('tbody');
    transactions.forEach(tx => {
    const row = document.createElement('tr');
    row.innerHTML = `
        <td>${tx.date}</td>
        <td>${tx.id}</td>
        <td>${tx.description}</td>
        <td>${tx.category}</td>
        <td style="color: ${tx.amount < 0 ? '#ff0000' : '#00ff00'}">${tx.amount < 0 ? '-' : '+'}$${Math.abs(tx.amount).toFixed(2)}</td>
        <td>${tx.account}</td>
    `;
    tbody.appendChild(row);
    });
    
    table.appendChild(tbody);
    container.innerHTML = '';
    container.appendChild(table);
}

function sortTable(transactions, column) {
    // Simple sort implementation - expand as needed
    console.log(`Sorting by ${column}`);
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

function updateBalanceTrendChart(data) {
    const ctx = document.getElementById('balanceTrendChart');
    
    if (balanceTrendChart) {
    balanceTrendChart.destroy();
    }
    
    balanceTrendChart = new Chart(ctx, {
    type: 'line',
    data: {
        labels: data.map(d => d.date),
        datasets: [{
        label: 'Balance',
        data: data.map(d => d.balance),
        borderColor: '#00ff00',
        backgroundColor: 'rgba(0, 255, 0, 0.1)',
        borderWidth: 2,
        fill: true,
        tension: 0.1,
        pointRadius: 0
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
            display: false
        }
        }
    }
    });
}

// Main refresh function
async function refreshData() {
    updateTimestamp();
    
    try {
    const [balances, summary, transactions, categorySpend, balanceTrend] = await Promise.all([
        fetchAccountBalances(),
        fetchMonthlySummary(),
        fetchRecentTransactions(),
        fetchCategorySpending(),
        fetchBalanceTrend()
    ]);
    
    updateBalances(balances);
    updateMonthlySummary(summary);
    updateTransactionsTable(transactions);
    updateCategoryChart(categorySpend);
    updateBalanceTrendChart(balanceTrend);
    } catch (error) {
    console.error('Error refreshing data:', error);
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

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
    if (e.key === 'r' || e.key === 'R') {
    refreshData();
    }
});

// Initial load
refreshData();