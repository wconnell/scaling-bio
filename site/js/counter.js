/**
 * Animated counter that counts up to a target value.
 */
class AnimatedCounter {
    constructor(element) {
        this.element = element;
        this.target = parseFloat(element.dataset.target);
        this.formatted = element.dataset.formatted;
        this.duration = 2000;
        this.startTime = null;
        this.started = false;
    }

    animate(timestamp) {
        if (!this.startTime) this.startTime = timestamp;

        const progress = Math.min((timestamp - this.startTime) / this.duration, 1);
        const eased = this.easeOutQuart(progress);
        const current = Math.floor(this.target * eased);

        this.element.textContent = this.formatNumber(current);

        if (progress < 1) {
            requestAnimationFrame((t) => this.animate(t));
        } else {
            // Use scientific notation for final value
            this.element.textContent = this.toScientific(this.target);
        }
    }

    easeOutQuart(x) {
        return 1 - Math.pow(1 - x, 4);
    }

    formatNumber(n) {
        return this.toScientific(n);
    }

    toScientific(n) {
        if (n === 0) return '0';
        const exp = Math.floor(Math.log10(Math.abs(n)));
        const mantissa = n / Math.pow(10, exp);
        const superscripts = '⁰¹²³⁴⁵⁶⁷⁸⁹';
        const expStr = String(exp).split('').map(d => superscripts[d]).join('');
        return `${mantissa.toFixed(1)}×10${expStr}`;
    }

    start() {
        if (this.started) return;
        this.started = true;
        requestAnimationFrame((t) => this.animate(t));
    }

    static initAll() {
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const counter = new AnimatedCounter(entry.target);
                    counter.start();
                    observer.unobserve(entry.target);
                }
            });
        }, { threshold: 0.5 });

        document.querySelectorAll('.counter-value').forEach(el => {
            observer.observe(el);
        });
    }
}
