const puppeteer = require('puppeteer');
(async () => {
    try {
        const browser = await puppeteer.launch();
        const page = await browser.newPage();
        page.on('console', msg => console.log('LOG:', msg.text()));
        page.on('pageerror', err => console.log('ERROR:', err.toString()));
        await page.goto('http://localhost:8000/eda.html', { waitUntil: 'networkidle0' });
        await browser.close();
    } catch(e) {
        console.error(e);
    }
})();
