// Configuration for API URL
// Support Universal Deployment (Vercel Serverless / Render / Local Node)

const isLocalhost = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';

// We use relative path for API calls ('/api/...').
// - On Local/Render, Express serves Frontend and /api from the same port.
// - On Vercel, the vercel.json rewrites /api to the serverless function and / Frontend to static.
window.API_BASE_URL = ''; // Empty string means it will append '/api/...' to current origin automatically.

if (isLocalhost && window.location.port === "3000") {
    // Special case: If user runs 2 separate processes (React dev pattern: Frontend 3000, Backend 8000)
    window.API_BASE_URL = 'http://localhost:8000';
}

console.log(`🔧 API Base URL Origin: ${window.API_BASE_URL || window.location.origin}`);
