/**
 * Main application for Scaling Biology site.
 */
class ScalingBioApp {
    constructor() {
        this.sources = [];
        this.manifest = null;
        this.chartManager = null;
        this.currentSourceIndex = 0;
        this.currentView = 'cumulative';
    }

    async init() {
        try {
            // Load manifest to discover all data sources
            this.manifest = await this.loadJSON('data/manifest.json');

            // Load all source data in parallel
            const loadPromises = this.manifest.sources.map(sourceId =>
                this.loadJSON(`data/${sourceId}.json`)
            );
            this.sources = await Promise.all(loadPromises);

            // Hide loading, show content
            document.getElementById('loading').style.display = 'none';
            document.getElementById('charts').style.display = 'block';
            document.getElementById('sources').style.display = 'block';

            // Initialize UI components
            this.renderCounters();
            this.initChart();
            this.renderSourceCards();
            this.updateLastUpdated();
            this.setupEventListeners();

        } catch (error) {
            console.error('Failed to initialize app:', error);
            this.showError('Failed to load data. Please try again later.');
        }
    }

    async loadJSON(path) {
        // Add cache-busting for data files
        const url = path.includes('data/') ? `${path}?v=${Date.now()}` : path;
        const response = await fetch(url);
        if (!response.ok) throw new Error(`Failed to load ${path}`);
        return response.json();
    }

    renderCounters() {
        const container = document.getElementById('counters');
        container.innerHTML = '';

        this.sources.forEach((source, sourceIndex) => {
            source.metrics.forEach(metric => {
                const card = this.createCounterCard(source.source, metric, sourceIndex);
                container.appendChild(card);
            });
        });

        // Animate counters when visible
        AnimatedCounter.initAll();
    }

    createCounterCard(sourceInfo, metric, sourceIndex) {
        const card = document.createElement('div');
        card.className = 'counter-card' + (sourceIndex === this.currentSourceIndex ? ' selected' : '');
        card.dataset.sourceIndex = sourceIndex;
        card.style.setProperty('--accent-color', sourceInfo.color);

        card.innerHTML = `
            <div class="counter-icon">${this.getIcon(sourceInfo.icon)}</div>
            <div class="counter-value"
                 data-target="${metric.current_value}"
                 data-formatted="${metric.formatted_value}">
                0
            </div>
            <div class="counter-label">${metric.name}</div>
            <div class="counter-source">${sourceInfo.name}</div>
        `;

        return card;
    }

    initChart() {
        this.chartManager = new ChartManager('mainChart');
        this.updateChart();
    }

    updateChart() {
        const source = this.sources[this.currentSourceIndex];
        this.chartManager.render(source, this.currentView);
    }

    renderSourceCards() {
        const container = document.getElementById('source-cards');
        container.innerHTML = '';

        this.sources.forEach(source => {
            const card = document.createElement('div');
            card.className = 'source-card';
            card.style.setProperty('--accent-color', source.source.color);

            card.innerHTML = `
                <h3>${source.source.name}</h3>
                <p>${source.source.description}</p>
                <div class="source-meta">
                    <span>Updated ${source.metadata.update_frequency}</span>
                    <a href="${source.source.url}" target="_blank">Visit Source</a>
                </div>
            `;

            container.appendChild(card);
        });
    }

    updateLastUpdated() {
        const dates = this.sources.map(s => new Date(s.metadata.last_updated));
        const latest = new Date(Math.max(...dates));
        document.getElementById('last-updated').textContent =
            latest.toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'long',
                day: 'numeric'
            });
    }

    setupEventListeners() {
        // Counter card clicks to select source
        document.querySelectorAll('.counter-card').forEach(card => {
            card.addEventListener('click', (e) => {
                const sourceIndex = parseInt(card.dataset.sourceIndex);
                if (sourceIndex === this.currentSourceIndex) return;

                document.querySelectorAll('.counter-card').forEach(c =>
                    c.classList.remove('selected')
                );
                card.classList.add('selected');
                this.currentSourceIndex = sourceIndex;
                this.updateChart();
            });
        });

        // View controls
        document.querySelectorAll('.chart-controls .btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                document.querySelectorAll('.chart-controls .btn').forEach(b =>
                    b.classList.remove('active')
                );
                e.target.classList.add('active');
                this.currentView = e.target.dataset.view;
                this.updateChart();
            });
        });
    }

    getIcon(iconName) {
        const icons = {
            'dna': '🧬',
            'cell': '🔬',
            'crystal': '🧊',
            'chain': '🔗',
            'protein': '🔷',
            'database': '🗄️',
            'default': '📊'
        };
        return icons[iconName] || icons.default;
    }

    showError(message) {
        document.getElementById('loading').style.display = 'none';
        const errorEl = document.getElementById('error');
        errorEl.textContent = message;
        errorEl.style.display = 'block';
    }
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    const app = new ScalingBioApp();
    app.init();
});
