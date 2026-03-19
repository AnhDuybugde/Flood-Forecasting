// Date Manager Component (Merged Calendar & Manual Input)
// Handles unified date state, calendar UI, and manual text input

let currentCalendarMonth = new Date().getMonth();
let currentCalendarYear = new Date().getFullYear();
let selectedCalendarDate = null; // Global state for current date YYYY-MM-DD

const monthNames = [
    'Tháng 1', 'Tháng 2', 'Tháng 3', 'Tháng 4', 'Tháng 5', 'Tháng 6',
    'Tháng 7', 'Tháng 8', 'Tháng 9', 'Tháng 10', 'Tháng 11', 'Tháng 12'
];

// ─────────────────────────────────────────────────────────
// 1. INITIALIZATION
// ─────────────────────────────────────────────────────────
function initDateManager() {
    console.log('📅 Initializing Date Manager (Calendar + Input)...');

    // Init state from the hidden date picker (may be set by map.js or empty)
    const datePicker = document.getElementById('date-picker');
    if (datePicker && datePicker.value) {
        const parts = datePicker.value.split('-');
        currentCalendarYear = parseInt(parts[0]);
        currentCalendarMonth = parseInt(parts[1]) - 1;
        selectedCalendarDate = datePicker.value;
    }

    attachCalendarEvents();
    initManualInputEvents();

    // Fetch latest available date from API and sync UI if not already set
    fetchAndSetLatestDate();

    console.log('✅ Date Manager initialized');
}

// ─────────────────────────────────────────────────────────
// 2. AUTO-DETECT LATEST AVAILABLE DATE
// ─────────────────────────────────────────────────────────
async function fetchAndSetLatestDate() {
    // Skip if a date is already selected (e.g. from URL params or prior navigation)
    if (selectedCalendarDate) {
        syncDateUI(selectedCalendarDate);
        return;
    }

    try {
        const region = window.currentRegion || 'DaNang';
        let latestDate = null;

        // Try DataLoader first (may already be cached)
        if (window.dataLoader) {
            const datesInfo = await window.dataLoader.loadAvailableDates(region);
            if (datesInfo && datesInfo.availableDates) {
                latestDate = getLatestDateFromAvailable(datesInfo.availableDates);
            }
        }

        // Fallback: direct API call
        if (!latestDate) {
            const r = await fetch(`${window.API_BASE_URL || ''}/api/dates/${region}`);
            const env = await r.json();
            if (env.success && env.data?.availableDates) {
                latestDate = getLatestDateFromAvailable(env.data.availableDates);
            }
        }

        if (latestDate) {
            console.log('📅 DateManager: Auto-set to latest date:', latestDate);
            selectedCalendarDate = latestDate;
            const parts = latestDate.split('-');
            currentCalendarYear = parseInt(parts[0]);
            currentCalendarMonth = parseInt(parts[1]) - 1;

            // Sync hidden picker
            const datePicker = document.getElementById('date-picker');
            if (datePicker) datePicker.value = latestDate;

            // Sync display button
            syncDateUI(latestDate);

            // Sync global state for map.js and data-panels
            window.currentDate = latestDate;
            window.currentRegion = window.currentRegion || 'DaNang';
        }
    } catch (e) {
        console.warn('⚠️ DateManager: Could not auto-detect latest date:', e);
    }
}

/**
 * Extract the latest date string (YYYY-MM-DD) from the nested availableDates structure.
 */
function getLatestDateFromAvailable(avail) {
    const years = Object.keys(avail).sort();
    if (!years.length) return null;
    const y = years[years.length - 1];
    const months = Object.keys(avail[y]).sort();
    if (!months.length) return null;
    const m = months[months.length - 1];
    const days = avail[y][m].sort((a, b) => a - b);
    if (!days.length) return null;
    const d = days[days.length - 1];
    return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
}

/**
 * Update the calendar button display text and calendar month/year to match a date.
 */
function syncDateUI(dateStr) {
    const displayBtn = document.getElementById('open-calendar-btn');
    if (displayBtn) {
        const [year, month, day] = dateStr.split('-').map(Number);
        const textEl = displayBtn.querySelector('.date-display-text');
        if (textEl) {
            textEl.textContent = `${String(day).padStart(2, '0')}/${String(month).padStart(2, '0')}/${year}`;
        }
    }
}

// ─────────────────────────────────────────────────────────
// 3. CALENDAR UI LOGIC
// ─────────────────────────────────────────────────────────
function attachCalendarEvents() {
    const openBtn = document.getElementById('open-calendar-btn');
    const closeBtn = document.getElementById('close-calendar-btn');
    const modal = document.getElementById('calendar-modal');
    const prevBtn = document.getElementById('prev-month-btn');
    const nextBtn = document.getElementById('next-month-btn');
    const yearSelect = document.getElementById('calendar-year-select');

    if (openBtn) openBtn.addEventListener('click', openCalendar);
    if (closeBtn) closeBtn.addEventListener('click', closeCalendar);
    if (modal) {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) closeCalendar();
        });
    }

    if (prevBtn) {
        prevBtn.addEventListener('click', () => {
            currentCalendarMonth--;
            if (currentCalendarMonth < 0) {
                currentCalendarMonth = 11;
                currentCalendarYear--;
            }
            renderCalendar();
        });
    }

    if (nextBtn) {
        nextBtn.addEventListener('click', () => {
            currentCalendarMonth++;
            if (currentCalendarMonth > 11) {
                currentCalendarMonth = 0;
                currentCalendarYear++;
            }
            renderCalendar();
        });
    }

    if (yearSelect) {
        yearSelect.addEventListener('change', (e) => {
            currentCalendarYear = parseInt(e.target.value);
            renderCalendar();
        });
    }

    // React to data loads bounds
    document.addEventListener('dataLoaded', (e) => {
        const data = e.detail?.data;
        if (data && data.dateRange) {
            const startDate = new Date(data.dateRange.start + 'T00:00:00Z');
            const lastDate = new Date(data.dateRange.end + 'T00:00:00Z');

            const minYear = startDate.getUTCFullYear();
            const maxYear = lastDate.getUTCFullYear();

            updateYearSelector(minYear, maxYear);

            if (currentCalendarYear > maxYear) {
                currentCalendarYear = maxYear;
                currentCalendarMonth = lastDate.getUTCMonth();
            }
        }
        renderCalendar();
    });
}

function openCalendar() {
    const modal = document.getElementById('calendar-modal');
    if (!modal) return;
    renderCalendar();
    modal.classList.remove('hidden');
    modal.classList.add('flex');
}

function closeCalendar() {
    const modal = document.getElementById('calendar-modal');
    if (!modal) return;
    modal.classList.add('hidden');
    modal.classList.remove('flex');
}

function renderCalendar() {
    const calendarGrid = document.getElementById('calendar-grid');
    const monthYearDisplay = document.getElementById('calendar-month-year');
    const yearSelect = document.getElementById('calendar-year-select');

    if (!calendarGrid || !monthYearDisplay) return;

    monthYearDisplay.textContent = `${monthNames[currentCalendarMonth]} ${currentCalendarYear}`;
    if (yearSelect) yearSelect.value = currentCalendarYear;

    calendarGrid.innerHTML = '';

    const firstDay = new Date(currentCalendarYear, currentCalendarMonth, 1).getDay();
    const daysInMonth = new Date(currentCalendarYear, currentCalendarMonth + 1, 0).getDate();
    const today = new Date();
    const isCurrentMonth = today.getMonth() === currentCalendarMonth && today.getFullYear() === currentCalendarYear;

    for (let i = 0; i < firstDay; i++) {
        const emptyCell = document.createElement('div');
        emptyCell.className = 'w-full aspect-square pointer-events-none';
        calendarGrid.appendChild(emptyCell);
    }

    for (let day = 1; day <= daysInMonth; day++) {
        const dayCell = document.createElement('button');
        dayCell.className = 'w-full aspect-square flex items-center justify-center text-sm font-medium rounded-lg transition-all cursor-pointer text-slate-700 dark:text-slate-300';
        dayCell.textContent = day;

        const dateStr = `${currentCalendarYear}-${String(currentCalendarMonth + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
        const hasData = window.dataLoader ? window.dataLoader.isDateAvailable(dateStr) : true;

        if (!hasData) {
            dayCell.classList.add('opacity-50', 'text-slate-400');
            dayCell.title = 'Không có dữ liệu chi tiết';
        }

        let isSelected = (dateStr === selectedCalendarDate);
        let isToday = (isCurrentMonth && day === today.getDate());

        if (isSelected) {
            dayCell.classList.add('bg-primary', 'text-white', 'font-bold', 'shadow-md');
            dayCell.classList.remove('text-slate-700', 'dark:text-slate-300');
        } else if (isToday) {
            dayCell.classList.add('bg-blue-100', 'dark:bg-blue-900/30', 'text-primary', 'font-bold');
            dayCell.classList.add('hover:bg-primary/10', 'hover:text-primary', 'hover:scale-105');
        } else {
            dayCell.classList.add('hover:bg-primary/10', 'hover:text-primary', 'hover:scale-105');
        }

        dayCell.addEventListener('click', () => {
            selectUnifiedDate(dateStr);
            closeCalendar();
        });

        calendarGrid.appendChild(dayCell);
    }
}

function updateYearSelector(minYear, maxYear) {
    const yearSelect = document.getElementById('calendar-year-select');
    if (!yearSelect) return;
    yearSelect.innerHTML = '';

    for (let y = minYear; y <= maxYear; y++) {
        const option = document.createElement('option');
        option.value = y;
        option.textContent = y;
        option.className = 'text-slate-800';
        if (y === currentCalendarYear) option.selected = true;
        yearSelect.appendChild(option);
    }

    if (currentCalendarYear > maxYear) {
        const option = document.createElement('option');
        option.value = currentCalendarYear;
        option.textContent = currentCalendarYear;
        option.selected = true;
        yearSelect.appendChild(option);
    }
}

// ─────────────────────────────────────────────────────────
// 3. MANUAL INPUT LOGIC
// ─────────────────────────────────────────────────────────
function initManualInputEvents() {
    const input = document.getElementById('manual-date-input');
    const goBtn = document.getElementById('manual-date-go-btn');
    const errorMsg = document.getElementById('manual-date-error');

    if (!input || !goBtn) return;

    const processInput = () => {
        const value = input.value.trim();
        if (!value) return showDateError(input, errorMsg, 'Vui lòng nhập ngày');

        const parsedDate = parseManualDate(value);
        if (!parsedDate) return showDateError(input, errorMsg, 'Định dạng không hợp lệ. Sử dụng: DD/MM/YYYY');

        const minDate = new Date('2020-01-01');
        const maxDate = new Date();
        maxDate.setDate(maxDate.getDate() + 7);

        if (parsedDate < minDate || parsedDate > maxDate) {
            return showDateError(input, errorMsg, 'Ngày phải từ 01/01/2020 đến 7 ngày tới');
        }

        const dateStr = `${parsedDate.getFullYear()}-${String(parsedDate.getMonth() + 1).padStart(2, '0')}-${String(parsedDate.getDate()).padStart(2, '0')}`;

        input.value = '';
        if (errorMsg) errorMsg.classList.add('hidden');
        input.classList.remove('border-red-500');
        input.classList.add('border-green-500');
        setTimeout(() => input.classList.remove('border-green-500'), 1000);

        selectUnifiedDate(dateStr); // Route to unified handler
    };

    goBtn.addEventListener('click', processInput);
    input.addEventListener('keypress', (e) => { if (e.key === 'Enter') processInput(); });
    input.addEventListener('input', () => {
        if (errorMsg) errorMsg.classList.add('hidden');
        input.classList.remove('border-red-500');
    });
}

function parseManualDate(value) {
    value = value.replace(/\s+/g, '');
    const ddmmyyyyMatch = value.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (ddmmyyyyMatch) {
        const day = parseInt(ddmmyyyyMatch[1]), month = parseInt(ddmmyyyyMatch[2]), year = parseInt(ddmmyyyyMatch[3]);
        if (month < 1 || month > 12 || day < 1 || day > 31) return null;
        const date = new Date(year, month - 1, day);
        if (date.getDate() !== day || date.getMonth() !== month - 1) return null;
        return date;
    }
    const yyyymmddMatch = value.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
    if (yyyymmddMatch) {
        const year = parseInt(yyyymmddMatch[1]), month = parseInt(yyyymmddMatch[2]), day = parseInt(yyyymmddMatch[3]);
        if (month < 1 || month > 12 || day < 1 || day > 31) return null;
        const date = new Date(year, month - 1, day);
        if (date.getDate() !== day || date.getMonth() !== month - 1) return null;
        return date;
    }
    return null;
}

function showDateError(input, errorMsg, message) {
    if (errorMsg) {
        errorMsg.textContent = message;
        errorMsg.classList.remove('hidden');
    }
    input.classList.add('border-red-500', 'animate-shake');
    setTimeout(() => input.classList.remove('animate-shake'), 500);
}

// ─────────────────────────────────────────────────────────
// 4. UNIFIED STATE DISPATCHER
// ─────────────────────────────────────────────────────────
async function selectUnifiedDate(dateStr) {
    selectedCalendarDate = dateStr;
    const currentRegion = window.currentRegion || 'DaNang';

    // Update Top Display UI
    const displayBtn = document.getElementById('open-calendar-btn');
    if (displayBtn) {
        const [year, month, day] = dateStr.split('-').map(Number);
        displayBtn.querySelector('.date-display-text').textContent =
            `${String(day).padStart(2, '0')}/${String(month).padStart(2, '0')}/${year}`;
    }

    // Modal opacity state
    const modal = document.getElementById('calendar-modal');
    if (modal) {
        modal.style.opacity = '0.7';
        modal.style.pointerEvents = 'none';
    }

    console.log(`🚀 Dispatching Date Change: ${dateStr}`);

    if (window.dataLoader) {
        try {
            await window.dataLoader.loadHeatmapData(currentRegion, dateStr, 'rain');
            if (typeof window.dataLoader.prefetchNextDate === 'function') {
                window.dataLoader.prefetchNextDate(currentRegion, dateStr);
            }
        } catch (e) {
            console.warn("⚠️ Load Heatmap Warning:", e);
        }
    }

    // Call External Handlers
    if (typeof updateHeatmap === 'function') updateHeatmap(dateStr, currentRegion);
    if (typeof updateTimeline === 'function') updateTimeline(dateStr);

    document.dispatchEvent(new CustomEvent('dateChanged', { detail: { date: dateStr } }));

    if (modal) {
        modal.style.opacity = '1';
        modal.style.pointerEvents = 'auto';
    }
}

// ─────────────────────────────────────────────────────────
// Init Entry
// ─────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => { setTimeout(initDateManager, 500); });
if (typeof window !== 'undefined') window.selectedCalendarDate = selectedCalendarDate;
