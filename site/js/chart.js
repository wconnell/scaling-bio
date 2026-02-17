/**
 * Chart manager using Chart.js
 */
class ChartManager {
    constructor(canvasId) {
        this.canvas = document.getElementById(canvasId);
        this.ctx = this.canvas.getContext('2d');
        this.chart = null;
    }

    render(source, viewType = 'cumulative') {
        if (this.chart) {
            this.chart.destroy();
        }

        const datasets = this.buildDatasets(source, viewType);
        const options = this.getChartOptions(source, viewType);

        this.chart = new Chart(this.ctx, {
            type: 'line',
            data: { datasets },
            options
        });
    }

    buildDatasets(source, viewType) {
        const datasets = [];

        source.timeseries.forEach(ts => {
            const metric = source.metrics.find(m => m.id === ts.metric_id);
            if (!metric) return;

            const data = ts.data
                .map(point => ({
                    // Parse as local time to avoid timezone shift (2025-01-01 -> Dec 31 2024)
                    x: new Date(point.date + 'T00:00:00'),
                    y: viewType === 'annual' ? point.value : point.cumulative
                }))
                .filter(d => d.y != null && d.y > 0);

            if (data.length === 0) return;

            datasets.push({
                label: metric.name,
                data: data,
                borderColor: source.source.color,
                backgroundColor: source.source.color + '20',
                fill: viewType === 'cumulative',
                tension: 0.3,
                pointRadius: 3,
                pointHoverRadius: 6,
                borderWidth: 2
            });
        });

        return datasets;
    }

    getChartOptions(source, viewType) {
        const self = this;
        const metric = source.metrics[0];
        const yAxisLabel = viewType === 'annual'
            ? `New ${metric.unit}/period`
            : `Total ${metric.unit}`;

        return {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index'
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'year',
                        displayFormats: {
                            year: 'yyyy'
                        }
                    },
                    title: {
                        display: true,
                        text: 'Year',
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#334155'
                    },
                    ticks: {
                        color: '#94a3b8'
                    }
                },
                y: {
                    type: viewType === 'log' ? 'logarithmic' : 'linear',
                    title: {
                        display: true,
                        text: yAxisLabel,
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#334155'
                    },
                    ticks: {
                        color: '#94a3b8',
                        callback: function(value) {
                            return self.formatNumber(value);
                        }
                    }
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: '#1e293b',
                    titleColor: '#f8fafc',
                    bodyColor: '#94a3b8',
                    borderColor: '#334155',
                    borderWidth: 1,
                    callbacks: {
                        title: function(context) {
                            const date = new Date(context[0].parsed.x);
                            return date.toLocaleDateString('en-US', {
                                year: 'numeric',
                                month: 'short',
                                day: 'numeric'
                            });
                        },
                        label: function(context) {
                            return `${context.dataset.label}: ${self.formatNumber(context.parsed.y)}`;
                        }
                    }
                }
            }
        };
    }

    formatNumber(n) {
        if (n === 0) return '0';
        const exp = Math.floor(Math.log10(Math.abs(n)));
        const mantissa = n / Math.pow(10, exp);
        const superscripts = '⁰¹²³⁴⁵⁶⁷⁸⁹';
        const expStr = String(exp).split('').map(d => superscripts[d]).join('');
        return `${mantissa.toFixed(1)}×10${expStr}`;
    }
}
