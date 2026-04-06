let selectedProjects = new Set();
let expandedProjects = new Set();
let projectDetailCache = new Map();
let projectDetailLoading = new Set();
let lastMonitorLogCount = 0;
let currentLang = localStorage.getItem('train_monitor_lang') || 'en';
let stateVersion = 0;
let changeLoopStarted = false;
let authRedirecting = false;
let historyBootstrapped = false;
let seenHistoryKeys = new Set();
let labelingState = new Map();
let lv2Verified = false;
let lastLogProjectLoaded = '';
let lastLogRefreshTs = 0;
let logRefreshInFlight = false;
let latestStateData = null;
let confusionViewState = new Map();

let sortState = {
    key: 'project',
    dir: 'asc'
};

let notifyState = {
    enabled: false,
    configured: false
};

let searchQuery = '';
let statusFilter = 'all';

const I18N = {
    vi: {
        app_title: 'AI Train Monitor',
        app_subtitle: 'Hiá»ƒn thá»‹ project Ä‘ang cháº¡y vĂ  pháº§n trÄƒm train trá»±c tiáº¿p tá»« Train_model_AI.py.',
        language_label: 'NgĂ´n ngá»¯',
        root_dir: 'ROOT_DIR',
        last_scan: 'Scan gáº§n nháº¥t',
        current_training: 'Äang train',
        current_train_percent: '% train hiá»‡n táº¡i',
        project_count: 'Sá»‘ project',
        train_monitor: 'Train monitor',
        scan_projects: 'QuĂ©t project',
        upload_project: 'Upload project (.zip)',
        train_selected: 'Train project Ä‘Ă£ chá»n',
        download_success_outputs: 'Tai tat ca Output success',
        train_all: 'Train táº¥t cáº£',
        retry_failed: 'Retry failed',
        clear_history: 'Xoa history',
        stop_queue: 'Stop queue',
        stop_current_train: 'Stop current train',
        select_all: 'Chá»n táº¥t cáº£',
        clear_all: 'Bá» chá»n táº¥t cáº£',
        expand_all: 'Má»Ÿ táº¥t cáº£',
        collapse_all: 'Thu gá»n táº¥t cáº£',
        refresh: 'Refresh',
        project_list: 'Danh sĂ¡ch project',
        project_list_desc: 'Báº¥m vĂ o tĂªn project Ä‘á»ƒ xem sá»‘ lÆ°á»£ng áº£nh vĂ  file weights trong Output',
        selected_count_prefix: 'ÄĂ£ chá»n:',
        selected_count_suffix: 'project',
        selected_help: 'CĂ³ thá»ƒ chá»n nhiá»u project rá»“i báº¥m Train project Ä‘Ă£ chá»n',
        showing_count: 'Hiá»ƒn thá»‹:',
        project: 'Project',
        status: 'Tráº¡ng thĂ¡i',
        train_percent: '% Train',
        start_time: 'Báº¯t Ä‘áº§u',
        end_time: 'Káº¿t thĂºc',
        action: 'HĂ nh Ä‘á»™ng',
        loading: 'Äang táº£i dá»¯ liá»‡u...',
        train_progress_panel: 'Tiáº¿n trĂ¬nh tá»« Train_model_AI.py',
        train_progress_desc: 'Äá»c tá»« /status vĂ  /history cá»§a port 8008',
        eta: 'ETA',
        elapsed: 'Thá»i gian cháº¡y',
        loss: 'Loss',
        loss_parts: 'Loss box / cls / dfl',
        images_per_sec: 'Images / sec',
        epoch_avg_time: 'Epoch / Avg time',
        batch_imgsize: 'Batch / ImgSize',
        precision: 'Precision',
        recall: 'Recall',
        best_map: 'Best mAP50-95',
        device: 'Thiáº¿t bá»‹',
        gpu_memory: 'Bá»™ nhá»› GPU',
        dataset_train_val: 'Dataset train / val',
        optimizer: 'Optimizer',
        run_dir: 'ThÆ° má»¥c run',
        weights: 'Weights',
        realtime_log: 'Log realtime tá»« Train_model_AI.py',
        train_order: 'Thá»© tá»± train',
        current_queue: 'Queue hiá»‡n táº¡i',
        empty_queue: 'Queue trá»‘ng',
        train_history: 'Lá»‹ch sá»­ train',
        latest_50: '50 báº£n ghi gáº§n nháº¥t',
        no_history: 'ChÆ°a cĂ³ lá»‹ch sá»­',
        subprocess_log: 'Log subprocess',
        stdout_stderr_log: 'Log stdout/stderr cá»§a file train',
        no_log: 'ChÆ°a cĂ³ log',
        monitor_no_data: 'ChÆ°a Ä‘á»c Ä‘Æ°á»£c dá»¯ liá»‡u tá»« Train_model_AI.py á»Ÿ port 8008',
        project_not_found: 'KhĂ´ng tĂ¬m tháº¥y project nĂ o. Kiá»ƒm tra láº¡i ROOT_DIR.',
        selected_none: 'Báº¡n chÆ°a chá»n project nĂ o',
        queue_order: 'Thá»© tá»± train',
        queue_empty: 'Queue trá»‘ng',
        history_empty: 'ChÆ°a cĂ³ lá»‹ch sá»­',
        history_status: 'Tráº¡ng thĂ¡i',
        history_time: 'Time',
        train_button: 'Train',
        retry_button: 'Retry',
        loading_detail: 'Äang táº£i chi tiáº¿t project...',
        image_counts: 'Sá»‘ lÆ°á»£ng áº£nh',
        image_folder_count: 'Sá»‘ áº£nh trong image',
        train_images_count: 'Sá»‘ áº£nh train/images',
        valid_images_count: 'Sá»‘ áº£nh valid/images',
        test_images_count: 'So anh test/images',
        output_weights_files: 'File trong Output / Model_Train*',
        model_train_details: 'Chi tiet Model_Train',
        run_images: 'Anh trong run',
        results_csv: 'Bang results.csv',
        model_quality: 'Danh gia model',
        best_epoch: 'Best epoch',
        latest_epoch: 'Latest epoch',
        quality_label: 'Chat luong',
        loss_summary: 'Loss cuoi',
        error_by_class: 'Class detect sai',
        confusion_pairs: 'Cap class nham lan',
        error_rate: 'Ti le sai',
        no_run_images: 'Khong co anh trong thu muc run.',
        no_results_csv: 'Khong co results.csv',
        results_rows: 'So dong results',
        run_folder: 'ThÆ° má»¥c run',
        file_name: 'TĂªn file',
        modified_at: 'NgĂ y giá» sá»­a',
        size_kb: 'KB',
        no_output_folder: 'KhĂ´ng cĂ³ thÆ° má»¥c Output.',
        no_weights_files: 'Khong co file trong cac thu muc Output/Model_Train*.',
        click_to_expand: 'Báº¥m Ä‘á»ƒ xem chi tiáº¿t',
        weights_file_count: 'Sá»‘ file weights',
        model_train_folder_count: 'Sá»‘ thÆ° má»¥c Model_Train',
        download: 'Táº£i ZIP',
        duplicate_button: 'Nhan ban',
        clear_dataset_button: 'Clear dataset',
        dataset_config_title: 'Cau hinh dataset',
        train_ratio: 'Train %',
        valid_ratio: 'Validation %',
        test_ratio: 'Test %',
        save_config: 'Luu config',
        create_dataset: 'Tao dataset',
        create_dataset_by_class: 'Chia theo class',
        merge_train_valid: 'Train toan bo data',
        split_by_class: 'Chia theo class',
        train_all_data: 'Train toan bo data',
        dataset_ratio_hint: 'Tong Train + Validation + Test phai bang 100',
        shuffle_dataset: 'Shuffle',
        seed_value: 'Seed',
        delete_button: 'Xoa',
        rename_prompt: 'Nhap ten moi cho project:',
        duplicate_prompt: 'Nhap ten project moi de nhan ban:',
        clear_dataset_confirm: 'Ban chac chan muon xoa cac thu muc runs, test, train, valid trong project nay?',
        delete_confirm: 'Ban chac chan muon xoa project nay?',
        clear_history_confirm: 'Ban chac chan muon xoa toan bo training history?',
        lv2_prompt: 'Nhap mat khau LV2:',
        modal_cancel: 'Cancel',
        modal_confirm: 'Confirm',
        lv2_title: 'LV2 verification',
        rename_title: 'Rename project',
        duplicate_title: 'Duplicate project',
        clear_history_title: 'Clear training history',
        clear_dataset_title: 'Clear dataset',
        delete_project_title: 'Delete project',
        model_findings: 'Model findings',
        model_quality_notes: 'Quality analysis',
        wrong_classes_summary: 'Wrong class summary',
        top_n_label: 'Top N',
        all_rows: 'All',
        low_sample_classes: 'Low sample classes',
        click_class_hint: 'Click a class below to filter confusion pairs',
        confused_with: 'Confused with',
        misclassified_samples: 'Misclassified samples',
        revalidate_run: 'Re-validate',
        stop_button: 'Stop',
        stopped_count: 'Stopped',
        notify_on: 'ÄĂ£ báº­t thĂ´ng bĂ¡o Telegram',
        notify_off: 'ÄĂ£ táº¯t thĂ´ng bĂ¡o Telegram',
        notify_not_configured: 'Telegram chÆ°a cáº¥u hĂ¬nh token/chat_id',
        notify_title_on: 'Táº¯t thĂ´ng bĂ¡o Telegram',
        notify_title_off: 'Báº­t thĂ´ng bĂ¡o Telegram',
        no_match_filter: 'KhĂ´ng cĂ³ project phĂ¹ há»£p vá»›i search/filter hiá»‡n táº¡i.',
        all_count: 'All',
        running_count: 'Running',
        queued_count: 'Queued',
        success_count: 'Success',
        failed_count: 'Failed',
        idle_count: 'Idle',
        train_finish_success: 'Train hoĂ n táº¥t',
        train_finish_failed: 'Train tháº¥t báº¡i',
        edit_button: 'Edit',
        label_editor: 'Label editor',
        load_images: 'Load áº£nh',
        image_list: 'Danh sĂ¡ch áº£nh',
        label_text: 'Ná»™i dung label (.txt)',
        save_label: 'LÆ°u label',
        select_image_first: 'Chá»n áº£nh Ä‘á»ƒ xem/sá»­a label',
        uploading: 'Äang upload...',
    },
    en: {
        app_title: 'AI Train Monitor',
        app_subtitle: 'Display the active project and training percentage',
        language_label: 'Language',
        root_dir: 'ROOT_DIR',
        last_scan: 'Last scan',
        current_training: 'Training now',
        current_train_percent: 'Current train %',
        project_count: 'Projects',
        train_monitor: 'Train monitor',
        scan_projects: 'Scan projects',
        upload_project: 'Upload project (.zip)',
        train_selected: 'Train selected projects',
        download_success_outputs: 'Download success outputs',
        train_all: 'Train all',
        retry_failed: 'Retry failed',
        clear_history: 'Clear history',
        stop_queue: 'Stop queue',
        stop_current_train: 'Stop current train',
        select_all: 'Select all',
        clear_all: 'Clear selection',
        expand_all: 'Expand all',
        collapse_all: 'Collapse all',
        refresh: 'Refresh',
        project_list: 'Project list',
        project_list_desc: 'Click the project name to view image counts and weight files in Output',
        selected_count_prefix: 'Selected:',
        selected_count_suffix: 'projects',
        selected_help: 'You can select multiple projects and click Train selected projects',
        showing_count: 'Showing:',
        project: 'Project',
        status: 'Status',
        train_percent: 'Train %',
        start_time: 'Start',
        end_time: 'End',
        action: 'Action',
        loading: 'Loading data...',
        train_progress_panel: 'Progress from Train_model_AI.py',
        train_progress_desc: 'Read from /status and /history on port 8008',
        eta: 'ETA',
        elapsed: 'Elapsed',
        loss: 'Loss',
        loss_parts: 'Loss box / cls / dfl',
        images_per_sec: 'Images / sec',
        epoch_avg_time: 'Epoch / Avg time',
        batch_imgsize: 'Batch / ImgSize',
        precision: 'Precision',
        recall: 'Recall',
        best_map: 'Best mAP50-95',
        device: 'Device',
        gpu_memory: 'GPU Memory',
        dataset_train_val: 'Dataset train / val',
        optimizer: 'Optimizer',
        run_dir: 'Run dir',
        weights: 'Weights',
        realtime_log: 'Realtime log from Train_model_AI.py',
        train_order: 'Training order',
        current_queue: 'Current queue',
        empty_queue: 'Queue is empty',
        train_history: 'Training history',
        latest_50: 'Latest 50 records',
        no_history: 'No history yet',
        subprocess_log: 'Subprocess log',
        stdout_stderr_log: 'stdout/stderr log from training file',
        no_log: 'No logs yet',
        monitor_no_data: 'Cannot read data from Train_model_AI.py on port 8008',
        project_not_found: 'No project found. Please check ROOT_DIR.',
        selected_none: 'No project selected',
        queue_order: 'Training order',
        queue_empty: 'Queue is empty',
        history_empty: 'No history yet',
        history_status: 'Status',
        history_time: 'Time',
        train_button: 'Train',
        retry_button: 'Retry',
        loading_detail: 'Loading project details...',
        image_counts: 'Image counts',
        image_folder_count: 'Images in image',
        train_images_count: 'Images in train/images',
        valid_images_count: 'Images in valid/images',
        test_images_count: 'Images in test/images',
        output_weights_files: 'Files in Output / Model_Train*',
        model_train_details: 'Model_Train details',
        run_images: 'Images in run',
        results_csv: 'results.csv table',
        model_quality: 'Model quality',
        best_epoch: 'Best epoch',
        latest_epoch: 'Latest epoch',
        quality_label: 'Quality',
        loss_summary: 'Latest loss',
        error_by_class: 'Misdetected classes',
        confusion_pairs: 'Top confusion pairs',
        error_rate: 'Error rate',
        no_run_images: 'No images found in this run folder.',
        no_results_csv: 'No results.csv found',
        results_rows: 'Result rows',
        run_folder: 'Run folder',
        file_name: 'File name',
        modified_at: 'Modified at',
        size_kb: 'KB',
        no_output_folder: 'No Output folder.',
        no_weights_files: 'No files found in Output/Model_Train*.',
        click_to_expand: 'Click to expand',
        weights_file_count: 'Weight files',
        model_train_folder_count: 'Model_Train folders',
        download: 'Download ZIP',
        duplicate_button: 'Duplicate',
        clear_dataset_button: 'Clear dataset',
        dataset_config_title: 'Dataset config',
        train_ratio: 'Train %',
        valid_ratio: 'Validation %',
        test_ratio: 'Test %',
        save_config: 'Save config',
        create_dataset: 'Create dataset',
        create_dataset_by_class: 'Split by class',
        merge_train_valid: 'Train all data',
        split_by_class: 'Split by class',
        train_all_data: 'Train all data',
        dataset_ratio_hint: 'Train + Validation + Test must equal 100',
        shuffle_dataset: 'Shuffle',
        seed_value: 'Seed',
        delete_button: 'Delete',
        rename_prompt: 'Enter the new project name:',
        duplicate_prompt: 'Enter the new project name for the duplicate:',
        clear_dataset_confirm: 'Are you sure you want to delete the runs, test, train, and valid folders in this project?',
        delete_confirm: 'Are you sure you want to delete this project?',
        clear_history_confirm: 'Are you sure you want to clear the entire training history?',
        lv2_prompt: 'Enter LV2 password:',
        modal_cancel: 'Cancel',
        modal_confirm: 'Confirm',
        lv2_title: 'LV2 verification',
        rename_title: 'Rename project',
        duplicate_title: 'Duplicate project',
        clear_history_title: 'Clear training history',
        clear_dataset_title: 'Clear dataset',
        delete_project_title: 'Delete project',
        model_findings: 'Model findings',
        model_quality_notes: 'Quality analysis',
        wrong_classes_summary: 'Wrong class summary',
        top_n_label: 'Top N',
        all_rows: 'All',
        low_sample_classes: 'Low sample classes',
        click_class_hint: 'Click a class below to filter confusion pairs',
        confused_with: 'Confused with',
        misclassified_samples: 'Misclassified samples',
        revalidate_run: 'Re-validate',
        stop_button: 'Stop',
        stopped_count: 'Stopped',
        notify_on: 'Telegram notification enabled',
        notify_off: 'Telegram notification disabled',
        notify_not_configured: 'Telegram token/chat_id is not configured',
        notify_title_on: 'Disable Telegram notification',
        notify_title_off: 'Enable Telegram notification',
        no_match_filter: 'No project matches the current search/filter.',
        all_count: 'All',
        running_count: 'Running',
        queued_count: 'Queued',
        success_count: 'Success',
        failed_count: 'Failed',
        idle_count: 'Idle',
        train_finish_success: 'Training completed',
        train_finish_failed: 'Training failed',
        edit_button: 'Edit',
        label_editor: 'Label editor',
        load_images: 'Load images',
        image_list: 'Image list',
        label_text: 'Label text (.txt)',
        save_label: 'Save label',
        select_image_first: 'Select an image to view/edit label',
        uploading: 'Uploading...',
    }
};

function t(key) {
    return (I18N[currentLang] && I18N[currentLang][key]) || key;
}

function renderNotifyBell() {
    const bell = document.getElementById('notifyBell');
    if (!bell) return;

    bell.classList.toggle('active', notifyState.enabled);
    bell.classList.toggle('off', !notifyState.enabled);
    bell.title = notifyState.enabled ? t('notify_title_on') : t('notify_title_off');
}

function updateSortIndicators() {
    const projectEl = document.getElementById('sortProjectIndicator');
    const statusEl = document.getElementById('sortStatusIndicator');

    if (!projectEl || !statusEl) return;

    projectEl.textContent = sortState.key === 'project'
        ? (sortState.dir === 'asc' ? '^' : 'v')
        : '<->';

    statusEl.textContent = sortState.key === 'status'
        ? (sortState.dir === 'asc' ? '^' : 'v')
        : '<->';
}

function updateSummaryBadgeActive() {
    const map = {
        all: 'badgeAll',
        running: 'badgeRunning',
        queued: 'badgeQueued',
        success: 'badgeSuccess',
        failed: 'badgeFailed',
        stopped: 'badgeStopped',
        idle: 'badgeIdle'
    };

    Object.values(map).forEach(id => {
        const el = document.getElementById(id);
        if (el) el.classList.remove('active');
    });

    const activeId = map[statusFilter] || 'badgeAll';
    const activeEl = document.getElementById(activeId);
    if (activeEl) activeEl.classList.add('active');
}

function applyLanguage() {
    document.documentElement.lang = currentLang;
    document.querySelectorAll('[data-i18n]').forEach(el => {
        const key = el.getAttribute('data-i18n');
        el.textContent = t(key);
    });

    document.getElementById('btnLangVi').classList.toggle('active', currentLang === 'vi');
    document.getElementById('btnLangEn').classList.toggle('active', currentLang === 'en');

    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
        searchInput.placeholder = currentLang === 'vi' ? 'Tim theo ten project...' : 'Search by project name...';
    }

    const statusSelect = document.getElementById('statusFilter');
    if (statusSelect) {
        statusSelect.value = statusFilter;
    }

    localStorage.setItem('train_monitor_lang', currentLang);

    renderNotifyBell();
    updateSortIndicators();
    updateSummaryBadgeActive();
}

function setLanguage(lang) {
    currentLang = lang;
    applyLanguage();
    refreshAll();
}

async function apiGet(url, opts = {}) {
    const res = await fetch(url, opts);

    if (res.status === 401) {
        if (!authRedirecting) {
            authRedirecting = true;
            const next = encodeURIComponent(window.location.pathname + window.location.search);
            window.location.href = `/login?next=${next}`;
        }
        throw new Error('Unauthorized');
    }

    let data = {};
    try {
        data = await res.json();
    } catch (e) {
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }
        return {};
    }

    if (!res.ok) {
        throw new Error(msg(data.message, `HTTP ${res.status}`));
    }

    return data;
}

async function ensureLv2() {
    if (lv2Verified) return true;

    const password = await showInputDialog({
        title: t('lv2_title'),
        message: t('lv2_prompt'),
        confirmText: t('modal_confirm'),
        cancelText: t('modal_cancel'),
        confirmClass: 'btn-primary',
        input: {
            type: 'password',
            value: '',
            autocomplete: 'current-password'
        }
    });
    if (password === null) return false;

    try {
        const data = await apiGet('/api/lv2/verify', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ password })
        });
        lv2Verified = !!(data && data.ok);
        return lv2Verified;
    } catch (e) {
        alert(msg(String(e), 'LV2 verify failed'));
        return false;
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function ensureToastContainer() {
    let box = document.getElementById('webToastContainer');
    if (box) return box;

    box = document.createElement('div');
    box.id = 'webToastContainer';
    box.className = 'web-toast-container';
    document.body.appendChild(box);
    return box;
}

function showWebToast(message, type = 'info') {
    const container = ensureToastContainer();
    const toast = document.createElement('div');
    toast.className = `web-toast ${type}`;
    toast.textContent = message;
    container.appendChild(toast);

    setTimeout(() => {
        toast.classList.add('hide');
        setTimeout(() => toast.remove(), 220);
    }, 4500);
}

function historyKey(item) {
    const p = String(item.project || '');
    const t = String(item.time || '');
    const rc = String(item.returncode ?? '');
    return `${p}|${t}|${rc}`;
}

function processHistoryNotifications(historyItems) {
    const rows = Array.isArray(historyItems) ? historyItems : [];

    if (!historyBootstrapped) {
        rows.forEach(x => seenHistoryKeys.add(historyKey(x)));
        historyBootstrapped = true;
        return;
    }

    for (const item of rows) {
        const key = historyKey(item);
        if (seenHistoryKeys.has(key)) continue;
        seenHistoryKeys.add(key);

        const st = String(item.status || '').toLowerCase();
        if (st !== 'success' && st !== 'failed') continue;

        const title = st === 'success' ? t('train_finish_success') : t('train_finish_failed');
        const project = String(item.project || '-');
        const rc = item.returncode === null || item.returncode === undefined ? '-' : item.returncode;
        showWebToast(`${title}: ${project} (RC: ${rc})`, st === 'success' ? 'success' : 'error');
    }

    if (seenHistoryKeys.size > 300) {
        const keep = rows.slice(-120).map(historyKey);
        seenHistoryKeys = new Set(keep);
    }
}

function badge(status) {
    return `<span class="status ${status}">${status}</span>`;
}

function escapeHtml(text) {
    return String(text ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
}

function decodeMojibake(text) {
    const s = String(text ?? '');
    if (!/[ĂƒĂ„Ă¡Â»]/.test(s)) return s;
    try {
        return decodeURIComponent(escape(s));
    } catch (e) {
        return s;
    }
}

function msg(text, fallback = 'OK') {
    const s = String(text ?? '').trim();
    if (!s) return fallback;
    return decodeMojibake(s);
}

function ensureModalHost() {
    let host = document.getElementById('webModalHost');
    if (host) return host;

    host = document.createElement('div');
    host.id = 'webModalHost';
    document.body.appendChild(host);
    return host;
}

function escapeAttr(text) {
    return escapeHtml(text).replaceAll('`', '&#096;');
}

function showModalDialog({
    title = '',
    message = '',
    confirmText = t('modal_confirm'),
    cancelText = t('modal_cancel'),
    confirmClass = 'btn-primary',
    input = null
} = {}) {
    const host = ensureModalHost();

    return new Promise((resolve) => {
        host.innerHTML = `
            <div class="web-modal-backdrop">
                <div class="web-modal" role="dialog" aria-modal="true" aria-label="${escapeAttr(title || 'Dialog')}">
                    <div class="web-modal-head">
                        <div class="web-modal-title">${escapeHtml(title)}</div>
                    </div>
                    <div class="web-modal-body">
                        ${message ? `<div class="web-modal-text">${escapeHtml(message).replaceAll('\n', '<br>')}</div>` : ''}
                        ${input ? `
                            <input
                                id="webModalInput"
                                class="web-modal-input"
                                type="${escapeAttr(input.type || 'text')}"
                                value="${escapeAttr(input.value || '')}"
                                placeholder="${escapeAttr(input.placeholder || '')}"
                                ${input.autocomplete ? `autocomplete="${escapeAttr(input.autocomplete)}"` : ''}
                            >
                        ` : ''}
                    </div>
                    <div class="web-modal-actions">
                        <button type="button" class="btn-light" id="webModalCancel">${escapeHtml(cancelText)}</button>
                        <button type="button" class="${escapeAttr(confirmClass)}" id="webModalConfirm">${escapeHtml(confirmText)}</button>
                    </div>
                </div>
            </div>
        `;

        const backdrop = host.querySelector('.web-modal-backdrop');
        const inputEl = host.querySelector('#webModalInput');
        const cancelBtn = host.querySelector('#webModalCancel');
        const confirmBtn = host.querySelector('#webModalConfirm');
        const onKeyDown = (e) => {
            if (e.key === 'Escape') {
                e.preventDefault();
                close(null);
                return;
            }
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                close(inputEl ? inputEl.value : true);
            }
        };
        const close = (result) => {
            document.removeEventListener('keydown', onKeyDown, true);
            host.innerHTML = '';
            resolve(result);
        };

        cancelBtn?.addEventListener('click', () => close(null));
        confirmBtn?.addEventListener('click', () => {
            close(inputEl ? inputEl.value : true);
        });
        backdrop?.addEventListener('click', (e) => {
            if (e.target === backdrop) close(null);
        });

        document.addEventListener('keydown', onKeyDown, true);

        if (inputEl) {
            inputEl.focus();
            inputEl.select();
        } else {
            confirmBtn?.focus();
        }
    });
}

async function showConfirmDialog(options = {}) {
    const result = await showModalDialog({
        ...options,
        input: null
    });
    return result === true;
}

async function showInputDialog(options = {}) {
    const result = await showModalDialog(options);
    if (result === null) return null;
    return String(result ?? '');
}

function projectDomId(projectName) {
    return encodeURIComponent(String(projectName || '')).replaceAll('%', '_');
}

function ensureLabelingState(projectName) {
    if (!labelingState.has(projectName)) {
        labelingState.set(projectName, {
            images: [],
            currentRel: ''
        });
    }
    return labelingState.get(projectName);
}

function f(x, n = 4) {
    if (x === null || x === undefined || Number.isNaN(Number(x))) return '-';
    return Number(x).toFixed(n);
}

function fmtT(sec) {
    if (sec === null || sec === undefined || Number.isNaN(Number(sec))) return '-';
    sec = Math.max(0, Math.floor(Number(sec)));
    const h = String(Math.floor(sec / 3600)).padStart(2, '0');
    const m = String(Math.floor((sec % 3600) / 60)).padStart(2, '0');
    const s = String(sec % 60).padStart(2, '0');
    return `${h}:${m}:${s}`;
}

function updateSelectedCount() {
    document.getElementById('selectedCount').textContent = selectedProjects.size;
}

function setSearchQuery(value) {
    searchQuery = String(value || '').trim().toLowerCase();
    refreshData(latestStateData || undefined);
}

function setStatusFilter(value) {
    statusFilter = String(value || 'all').toLowerCase();

    const select = document.getElementById('statusFilter');
    if (select) {
        select.value = statusFilter;
    }

    updateSummaryBadgeActive();
    refreshData(latestStateData || undefined);
}

function selectAllProjects() {
    const checkboxes = document.querySelectorAll('.project-check');
    checkboxes.forEach(cb => {
        cb.checked = true;
        selectedProjects.add(cb.value);
    });
    document.getElementById('checkAll').checked = checkboxes.length > 0;
    updateSelectedCount();
}

function clearAllProjects() {
    const checkboxes = document.querySelectorAll('.project-check');
    checkboxes.forEach(cb => cb.checked = false);
    selectedProjects.clear();
    document.getElementById('checkAll').checked = false;
    updateSelectedCount();
}

function resetProjectSelection() {
    selectedProjects.clear();

    const checkboxes = document.querySelectorAll('.project-check');
    checkboxes.forEach(cb => {
        cb.checked = false;
    });

    const checkAll = document.getElementById('checkAll');
    if (checkAll) {
        checkAll.checked = false;
    }

    updateSelectedCount();
}

async function expandAllProjects() {
    const rows = document.querySelectorAll('.project-check');
    const names = Array.from(rows).map(cb => cb.value);

    for (const name of names) {
        expandedProjects.add(name);
        await ensureProjectDetailLoaded(name);
    }
    refreshData(latestStateData || undefined);
}

function collapseAllProjects() {
    expandedProjects.clear();
    refreshData(latestStateData || undefined);
}

function toggleCheckAll(master) {
    const checked = master.checked;
    const checkboxes = document.querySelectorAll('.project-check');
    checkboxes.forEach(cb => {
        cb.checked = checked;
        if (checked) selectedProjects.add(cb.value);
        else selectedProjects.delete(cb.value);
    });
    updateSelectedCount();
}

function toggleProjectSelection(checkbox) {
    const name = checkbox.value;
    if (checkbox.checked) selectedProjects.add(name);
    else selectedProjects.delete(name);

    const allChecks = document.querySelectorAll('.project-check');
    const checkedChecks = document.querySelectorAll('.project-check:checked');
    document.getElementById('checkAll').checked = allChecks.length > 0 && allChecks.length === checkedChecks.length;

    updateSelectedCount();
}

function toggleSort(key) {
    if (sortState.key === key) {
        sortState.dir = sortState.dir === 'asc' ? 'desc' : 'asc';
    } else {
        sortState.key = key;
        sortState.dir = 'asc';
    }
    updateSortIndicators();
    refreshData(latestStateData || undefined);
}

function getStatusSortValue(status) {
    const order = {
        idle: 1,
        queued: 2,
        running: 3,
        success: 4,
        failed: 5,
        stopped: 6
    };
    return order[String(status || '').toLowerCase()] || 999;
}

function sortProjects(projects) {
    const arr = [...projects];

    arr.sort((a, b) => {
        let cmp = 0;

        if (sortState.key === 'project') {
            cmp = String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' });
        } else if (sortState.key === 'status') {
            const sa = getStatusSortValue(a.status);
            const sb = getStatusSortValue(b.status);
            cmp = sa - sb;

            if (cmp === 0) {
                cmp = String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' });
            }
        }

        return sortState.dir === 'asc' ? cmp : -cmp;
    });

    return arr;
}

function filterProjects(projects) {
    return projects.filter(p => {
        const name = String(p.name || '').toLowerCase();
        const status = String(p.status || '').toLowerCase();

        const matchSearch = !searchQuery || name.includes(searchQuery);
        const matchStatus = statusFilter === 'all' || status === statusFilter;

        return matchSearch && matchStatus;
    });
}

function updateSummaryBadges(projects) {
    const counts = {
        all: 0,
        running: 0,
        queued: 0,
        success: 0,
        failed: 0,
        stopped: 0,
        idle: 0
    };

    (projects || []).forEach(p => {
        counts.all += 1;

        const st = String(p.status || 'idle').toLowerCase();
        if (counts.hasOwnProperty(st)) {
            counts[st] += 1;
        } else {
            counts.idle += 1;
        }
    });

    document.getElementById('countAll').textContent = counts.all;
    document.getElementById('countRunning').textContent = counts.running;
    document.getElementById('countQueued').textContent = counts.queued;
    document.getElementById('countSuccess').textContent = counts.success;
    document.getElementById('countFailed').textContent = counts.failed;
    const stoppedBox = document.getElementById('countStopped');
    if (stoppedBox) stoppedBox.textContent = counts.stopped;
    document.getElementById('countIdle').textContent = counts.idle;

    updateSummaryBadgeActive();
}

async function ensureProjectDetailLoaded(projectName) {
    if (projectDetailCache.has(projectName) || projectDetailLoading.has(projectName)) {
        return;
    }

    projectDetailLoading.add(projectName);
    try {
        const data = await apiGet('/api/project_detail?project=' + encodeURIComponent(projectName));
        projectDetailCache.set(projectName, data);
    } catch (e) {
        projectDetailCache.set(projectName, {
            ok: false,
            message: String(e)
        });
    } finally {
        projectDetailLoading.delete(projectName);
    }
}

function toggleProjectDetail(encodedName) {
    const projectName = decodeURIComponent(encodedName);

    if (expandedProjects.has(projectName)) {
        expandedProjects.delete(projectName);
        refreshData(latestStateData || undefined);
        return;
    }

    expandedProjects.add(projectName);
    ensureProjectDetailLoaded(projectName).then(() => refreshData(latestStateData || undefined));
    refreshData(latestStateData || undefined);
}

function renderInsightCards(items = []) {
    const rows = Array.isArray(items) ? items.filter(Boolean) : [];
    if (!rows.length) return '';

    return `
        <div class="analysis-cards mb8">
            ${rows.map(item => `
                <div class="analysis-card ${escapeHtml(item.severity || 'info')}">
                    <div class="analysis-card-title">${escapeHtml(item.title || '-')}</div>
                    <div class="analysis-card-text">${escapeHtml(item.message || '-')}</div>
                </div>
            `).join('')}
        </div>
    `;
}

function getRunConfusionViewState(runKey) {
    const key = String(runKey || '');
    if (!confusionViewState.has(key)) {
        confusionViewState.set(key, {
            className: '',
            predClassName: '',
            topN: 10
        });
    }
    return confusionViewState.get(key);
}

function setRunConfusionClassFilter(projectName, runKey, className = '') {
    const state = getRunConfusionViewState(runKey);
    state.className = decodeURIComponent(String(className || ''));
    state.predClassName = '';
    refreshData(latestStateData || undefined);
}

function setRunConfusionTopN(projectName, runKey, topN) {
    const state = getRunConfusionViewState(runKey);
    const value = String(topN || '10').toLowerCase();
    state.topN = value === 'all' ? 'all' : Math.max(1, Number(topN || 10));
    refreshData(latestStateData || undefined);
}

function setRunConfusionPairFilter(projectName, runKey, gtClassName = '', predClassName = '') {
    const state = getRunConfusionViewState(runKey);
    state.className = decodeURIComponent(String(gtClassName || ''));
    state.predClassName = decodeURIComponent(String(predClassName || ''));
    refreshData(latestStateData || undefined);
}

function renderProjectDetailContent(projectName) {
    if (projectDetailLoading.has(projectName)) {
        return `<div class="detail-panel"><div class="muted-box">${t('loading_detail')}</div></div>`;
    }

    const data = projectDetailCache.get(projectName);
    if (!data) {
        return `<div class="detail-panel"><div class="muted-box">${t('loading_detail')}</div></div>`;
    }

    if (!data.ok) {
        return `<div class="detail-panel"><div class="muted-box">${escapeHtml(data.message || 'Error')}</div></div>`;
    }

    const datasetConfig = data.dataset_config || {};

    const weightsHtml = (data.weight_files && data.weight_files.length > 0)
        ? `
            <div class="output-list">
                <table class="output-table">
                    <thead>
                        <tr>
                            <th>${t('run_folder')}</th>
                            <th>${t('file_name')}</th>
                            <th>${t('modified_at')}</th>
                            <th>${t('size_kb')}</th>
                            <th>${t('action')}</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.weight_files.map(file => `
                            <tr>
                                <td>${escapeHtml(file.run_folder)}</td>
                                <td>${escapeHtml(file.file_name)}</td>
                                <td>${escapeHtml(file.modified_at)}</td>
                                <td>${escapeHtml(file.size_kb)}</td>
                                <td>
                                    <a
                                        class="download-link"
                                        href="/api/download_weight?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(file.relative_path)}"
                                        download
                                    >${t('download')}</a>
                                </td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `
        : `<div class="muted-box">${data.output_folder_exists ? t('no_weights_files') : t('no_output_folder')}</div>`;

    const runDetailsHtml = (data.run_details && data.run_details.length > 0)
        ? data.run_details.map(run => {
            const imageItems = Array.isArray(run.image_files) ? run.image_files : [];
            const csvInfo = run.results_csv || {};
            const csvColumns = Array.isArray(csvInfo.columns) ? csvInfo.columns : [];
            const csvRows = Array.isArray(csvInfo.rows) ? csvInfo.rows : [];
            const csvSummary = csvInfo.summary || {};
            const confusion = run.confusion_analysis || {};
            const topErrorClasses = Array.isArray(confusion.top_error_classes) ? confusion.top_error_classes : [];
            const topConfusions = Array.isArray(confusion.top_confusions) ? confusion.top_confusions : [];
            const qualityNotes = Array.isArray(csvSummary.analysis_notes) ? csvSummary.analysis_notes : [];
            const confusionInsights = Array.isArray(confusion.insights) ? confusion.insights : [];
            const classOverview = Array.isArray(confusion.class_overview) ? confusion.class_overview : [];
            const lowSampleClasses = Array.isArray(confusion.low_sample_classes) ? confusion.low_sample_classes : [];
            const runKey = `${projectName}::${run.run_folder}`;
            const confusionState = getRunConfusionViewState(runKey);
            const selectedClassName = String(confusionState.className || '');
            const selectedPredClassName = String(confusionState.predClassName || '');
            const topNValue = confusionState.topN === 'all' ? 'all' : Math.max(1, Number(confusionState.topN || 10));
            const visibleErrorClasses = topNValue === 'all' ? topErrorClasses : topErrorClasses.slice(0, topNValue);
            const visibleLowSampleClasses = topNValue === 'all' ? lowSampleClasses : lowSampleClasses.slice(0, topNValue);
            const filteredConfusionsBase = selectedClassName
                ? topConfusions.filter(row => String(row.gt_class_name || '') === selectedClassName)
                : topConfusions;
            const visibleConfusions = topNValue === 'all' ? filteredConfusionsBase : filteredConfusionsBase.slice(0, topNValue);
            const confusionClassOptions = topErrorClasses.map(row => String(row.gt_class_name || '')).filter(Boolean);
            const sampleItems = Array.isArray(confusion.sample_items) ? confusion.sample_items : [];
            const filteredSamplesBase = sampleItems.filter(row => {
                const gtName = String(row.gt_class_name || '');
                const predName = String(row.pred_class_name || '');
                if (selectedClassName && gtName !== selectedClassName) return false;
                if (selectedPredClassName && predName !== selectedPredClassName) return false;
                return true;
            });
            const visibleSamples = (topNValue === 'all' ? filteredSamplesBase : filteredSamplesBase.slice(0, Math.max(12, Number(topNValue) * 4)));

            const imagesHtml = imageItems.length > 0
                ? `
                    <div class="run-image-grid">
                        ${imageItems.map(img => `
                            <a class="run-image-card" href="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(img.relative_path)}" target="_blank">
                                <img src="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(img.relative_path)}" alt="${escapeHtml(img.name)}">
                                <div class="run-image-name">${escapeHtml(img.name)}</div>
                            </a>
                        `).join('')}
                    </div>
                `
                : `<div class="muted-box">${t('no_run_images')}</div>`;

            const csvHtml = (csvInfo.exists && csvColumns.length > 0)
                ? `
                    <div class="detail-stats mb8">
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('quality_label')}</div>
                            <div class="detail-stat-value">${escapeHtml(csvSummary.quality_label ?? '-')}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('best_epoch')}</div>
                            <div class="detail-stat-value">${escapeHtml(csvSummary.best_epoch ?? '-')}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">mAP50-95</div>
                            <div class="detail-stat-value">${escapeHtml(csvSummary.map5095 ?? '-')}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">mAP50</div>
                            <div class="detail-stat-value">${escapeHtml(csvSummary.map50 ?? '-')}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">Precision</div>
                            <div class="detail-stat-value">${escapeHtml(csvSummary.precision ?? '-')}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">Recall</div>
                            <div class="detail-stat-value">${escapeHtml(csvSummary.recall ?? '-')}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('latest_epoch')}</div>
                            <div class="detail-stat-value">${escapeHtml(csvSummary.latest_epoch ?? '-')}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('loss_summary')}</div>
                            <div class="detail-stat-value">
                                box ${escapeHtml(csvSummary.val_box_loss ?? '-')} |
                                cls ${escapeHtml(csvSummary.val_cls_loss ?? '-')} |
                                dfl ${escapeHtml(csvSummary.val_dfl_loss ?? '-')}
                            </div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">Train/Val total loss</div>
                            <div class="detail-stat-value">
                                ${escapeHtml(csvSummary.latest_train_total_loss ?? '-')} |
                                ${escapeHtml(csvSummary.latest_val_total_loss ?? '-')}
                            </div>
                        </div>
                    </div>
                    ${qualityNotes.length > 0 ? `
                        <div class="detail-title">${t('model_quality_notes')}</div>
                        ${renderInsightCards(qualityNotes)}
                    ` : ''}
                    ${confusionInsights.length > 0 ? `
                        <div class="detail-title">${t('model_findings')}</div>
                        ${renderInsightCards(confusionInsights)}
                    ` : ''}
                    ${classOverview.length > 0 ? `
                        <div class="detail-box mb8">
                            <div class="detail-title">${t('wrong_classes_summary')}</div>
                            <div class="small">${escapeHtml(classOverview.join(', '))}</div>
                        </div>
                    ` : ''}
                    ${visibleLowSampleClasses.length > 0 ? `
                        <div class="detail-box mb8">
                            <div class="detail-title">${t('low_sample_classes')}</div>
                            <div class="small">
                                ${escapeHtml(visibleLowSampleClasses.map(row => `${row.gt_class_name} (${row.gt_total})`).join(', '))}
                            </div>
                        </div>
                    ` : ''}
                    ${(topErrorClasses.length > 0 || topConfusions.length > 0) ? `
                        <div class="confusion-toolbar mb8">
                            <div class="small">${t('click_class_hint')}</div>
                            <div class="confusion-toolbar-right">
                                <label class="small" for="topN_${projectDomId(runKey)}">${t('top_n_label')}</label>
                                <select
                                    id="topN_${projectDomId(runKey)}"
                                    class="confusion-topn-select"
                                    onchange="setRunConfusionTopN('${encodeURIComponent(projectName)}', '${encodeURIComponent(runKey)}', this.value)"
                                >
                                    <option value="5" ${String(topNValue) === '5' ? 'selected' : ''}>5</option>
                                    <option value="10" ${String(topNValue) === '10' ? 'selected' : ''}>10</option>
                                    <option value="20" ${String(topNValue) === '20' ? 'selected' : ''}>20</option>
                                    <option value="all" ${String(topNValue) === 'all' ? 'selected' : ''}>${t('all_rows')}</option>
                                </select>
                            </div>
                        </div>
                        <div class="class-chip-wrap mb8">
                            <button
                                class="class-chip ${selectedClassName ? '' : 'active'}"
                                onclick="setRunConfusionClassFilter('${encodeURIComponent(projectName)}', '${encodeURIComponent(runKey)}', '')"
                            >${t('all_rows')}</button>
                            ${confusionClassOptions.map(className => `
                                <button
                                    class="class-chip ${selectedClassName === className ? 'active' : ''}"
                                    onclick="setRunConfusionClassFilter('${encodeURIComponent(projectName)}', '${encodeURIComponent(runKey)}', '${encodeURIComponent(className)}')"
                                >${escapeHtml(className)}</button>
                            `).join('')}
                        </div>
                    ` : ''}
                    ${visibleErrorClasses.length > 0 ? `
                        <div class="output-list mb8">
                            <div class="detail-title">${t('error_by_class')}</div>
                            <table class="output-table">
                                <thead>
                                    <tr>
                                        <th>Class</th>
                                        <th>${t('error_rate')}</th>
                                        <th>Errors</th>
                                        <th>Total</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${visibleErrorClasses.map(row => `
                                        <tr class="${selectedClassName === String(row.gt_class_name || '') ? 'row-selected' : ''}">
                                            <td>
                                                <button
                                                    class="table-link-btn"
                                                    onclick="setRunConfusionClassFilter('${encodeURIComponent(projectName)}', '${encodeURIComponent(runKey)}', '${encodeURIComponent(String(row.gt_class_name || ''))}')"
                                                >${escapeHtml(row.gt_class_name ?? '-')}</button>
                                            </td>
                                            <td>${escapeHtml(((Number(row.error_rate || 0)) * 100).toFixed(2))}%</td>
                                            <td>${escapeHtml(row.total_errors ?? 0)}</td>
                                            <td>${escapeHtml(row.gt_total ?? 0)}</td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        </div>
                    ` : ''}
                    ${topConfusions.length > 0 ? `
                        <div class="output-list mb8">
                            <div class="detail-title">${selectedClassName ? `${t('confusion_pairs')} | ${selectedClassName}` : t('confusion_pairs')}</div>
                            ${visibleConfusions.length > 0 ? `
                                <table class="output-table">
                                    <thead>
                                        <tr>
                                            <th>GT class</th>
                                            <th>${t('confused_with')}</th>
                                            <th>${t('error_rate')}</th>
                                            <th>Count</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${visibleConfusions.map(row => `
                                            <tr class="${selectedClassName === String(row.gt_class_name || '') && selectedPredClassName === String(row.pred_class_name || '') ? 'row-selected' : ''}">
                                                <td>
                                                    <button
                                                        class="table-link-btn"
                                                        onclick="setRunConfusionPairFilter('${encodeURIComponent(projectName)}', '${encodeURIComponent(runKey)}', '${encodeURIComponent(String(row.gt_class_name || ''))}', '${encodeURIComponent(String(row.pred_class_name || ''))}')"
                                                    >${escapeHtml(row.gt_class_name ?? '-')}</button>
                                                </td>
                                                <td>
                                                    <button
                                                        class="table-link-btn"
                                                        onclick="setRunConfusionPairFilter('${encodeURIComponent(projectName)}', '${encodeURIComponent(runKey)}', '${encodeURIComponent(String(row.gt_class_name || ''))}', '${encodeURIComponent(String(row.pred_class_name || ''))}')"
                                                    >${escapeHtml(row.pred_class_name ?? '-')}</button>
                                                </td>
                                                <td>${escapeHtml(((Number(row.rate_over_gt || 0)) * 100).toFixed(2))}%</td>
                                                <td>${escapeHtml(row.count ?? 0)}</td>
                                            </tr>
                                        `).join('')}
                                    </tbody>
                                </table>
                            ` : `<div class="muted-box">No confusion rows for this class in the current Top-N view.</div>`}
                        </div>
                    ` : ''}
                    ${sampleItems.length > 0 ? `
                        <div class="detail-box detail-box-full mb8">
                            <div class="detail-title">${selectedClassName ? `${t('misclassified_samples')} | ${selectedClassName}${selectedPredClassName ? ` -> ${selectedPredClassName}` : ''}` : t('misclassified_samples')}</div>
                            ${visibleSamples.length > 0 ? `
                                <div class="run-image-grid">
                                    ${visibleSamples.map(item => `
                                        <a class="run-image-card" href="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(item.relative_path)}" target="_blank">
                                            <img src="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(item.relative_path)}" alt="${escapeHtml(item.image_name)}">
                                            <div class="run-image-name">${escapeHtml(item.gt_class_name || '-')} -> ${escapeHtml(item.pred_class_name || '-')} | ${escapeHtml(item.image_name || '-')}</div>
                                        </a>
                                    `).join('')}
                                </div>
                            ` : `<div class="muted-box">No sample images match the current class/pair filter.</div>`}
                        </div>
                    ` : ''}
                    <div class="output-list">
                        <div class="small mb8">${t('results_rows')}: ${escapeHtml(csvInfo.row_count ?? 0)}</div>
                        <table class="output-table">
                            <thead>
                                <tr>${csvColumns.map(col => `<th>${escapeHtml(col)}</th>`).join('')}</tr>
                            </thead>
                            <tbody>
                                ${csvRows.map(row => {
                                    const epochValue = Number(row.epoch ?? NaN);
                                    const rowClass = Number.isFinite(epochValue) && epochValue === Number(csvSummary.best_epoch) ? 'row-best-epoch' : '';
                                    return `
                                    <tr class="${rowClass}">
                                        ${csvColumns.map(col => `<td>${escapeHtml(row[col] ?? '')}</td>`).join('')}
                                    </tr>
                                `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                `
                : `<div class="muted-box">${t('no_results_csv')}</div>`;

            return `
                <details class="run-detail-card">
                    <summary>
                        <span>${escapeHtml(run.run_folder)}</span>
                        <span class="small">Images: ${escapeHtml(run.image_count ?? 0)} | CSV: ${csvInfo.exists ? escapeHtml(csvInfo.row_count ?? 0) : 0}</span>
                        <button class="btn-light btn-mini" onclick="event.preventDefault(); event.stopPropagation(); revalidateRun('${encodeURIComponent(projectName)}', '${encodeURIComponent(run.run_folder)}')">${t('revalidate_run')}</button>
                    </summary>
                    <div class="run-detail-body">
                        <div class="detail-box detail-box-full">
                            <div class="detail-title">${t('run_images')}</div>
                            ${imagesHtml}
                        </div>
                        <div class="detail-box detail-box-full">
                            <div class="detail-title">${t('results_csv')}</div>
                            ${csvHtml}
                        </div>
                    </div>
                </details>
            `;
        }).join('')
        : `<div class="muted-box">${data.output_folder_exists ? t('no_weights_files') : t('no_output_folder')}</div>`;

    return `
        <div class="detail-panel">
            <div class="detail-grid">
                <div class="detail-box">
                    <div class="detail-title">${t('dataset_config_title')}</div>
                    <div class="detail-stats">
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('train_ratio')}</div>
                            <div class="detail-stat-value"><input type="number" min="0" max="100" id="dsTrain_${projectDomId(projectName)}" value="${escapeHtml(datasetConfig.train_percent ?? 80)}"></div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('valid_ratio')}</div>
                            <div class="detail-stat-value"><input type="number" min="0" max="100" id="dsValid_${projectDomId(projectName)}" value="${escapeHtml(datasetConfig.valid_percent ?? 20)}"></div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('test_ratio')}</div>
                            <div class="detail-stat-value"><input type="number" min="0" max="100" id="dsTest_${projectDomId(projectName)}" value="${escapeHtml(datasetConfig.test_percent ?? 0)}"></div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('shuffle_dataset')}</div>
                            <div class="detail-stat-value"><input type="checkbox" id="dsShuffle_${projectDomId(projectName)}" ${(datasetConfig.shuffle ?? true) ? 'checked' : ''}></div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('seed_value')}</div>
                            <div class="detail-stat-value"><input type="number" id="dsSeed_${projectDomId(projectName)}" value="${escapeHtml(datasetConfig.seed ?? 42)}"></div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('split_by_class')}</div>
                            <div class="detail-stat-value"><input type="checkbox" id="dsSplitClass_${projectDomId(projectName)}" ${(datasetConfig.split_by_class ?? false) ? 'checked' : ''}></div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('train_all_data')}</div>
                            <div class="detail-stat-value"><input type="checkbox" id="dsTrainAll_${projectDomId(projectName)}" ${(datasetConfig.train_all_data ?? false) ? 'checked' : ''}></div>
                        </div>
                    </div>
                    <div class="small mb8">${t('dataset_ratio_hint')}</div>
                    <div class="action-wrap">
                        <button class="btn-light btn-mini" onclick="saveDatasetConfig('${encodeURIComponent(projectName)}')">${t('save_config')}</button>
                        <button class="btn-primary btn-mini" onclick="createDataset('${encodeURIComponent(projectName)}')">${t('create_dataset')}</button>
                    </div>
                </div>

                <div class="detail-box">
                    <div class="detail-title">${t('image_counts')}</div>
                    <div class="detail-stats">
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('image_folder_count')}</div>
                            <div class="detail-stat-value">${data.image_folder_count ?? 0}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('train_images_count')}</div>
                            <div class="detail-stat-value">${data.train_images_count ?? 0}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('valid_images_count')}</div>
                            <div class="detail-stat-value">${data.valid_images_count ?? 0}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('test_images_count')}</div>
                            <div class="detail-stat-value">${data.test_images_count ?? 0}</div>
                        </div>
                    </div>
                </div>

                <div class="detail-box">
                    <div class="detail-title">${t('output_weights_files')}</div>
                    <div class="detail-stats">
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('model_train_folder_count')}</div>
                            <div class="detail-stat-value">${(data.model_train_folders || []).length}</div>
                        </div>
                        <div class="detail-stat-item">
                            <div class="detail-stat-label">${t('weights_file_count')}</div>
                            <div class="detail-stat-value">${data.weight_files_count ?? 0}</div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="detail-box detail-box-full">
                <div class="detail-title">${t('output_weights_files')}</div>
                ${weightsHtml}
            </div>

            <div class="detail-box detail-box-full">
                <div class="detail-title">${t('model_train_details')}</div>
                ${runDetailsHtml}
            </div>

        </div>
    `;
}

function getDatasetConfigFromInputs(projectName) {
    const id = projectDomId(projectName);
    return {
        project: projectName,
        train_percent: Number(document.getElementById(`dsTrain_${id}`)?.value || 0),
        valid_percent: Number(document.getElementById(`dsValid_${id}`)?.value || 0),
        test_percent: Number(document.getElementById(`dsTest_${id}`)?.value || 0),
        shuffle: !!document.getElementById(`dsShuffle_${id}`)?.checked,
        seed: Number(document.getElementById(`dsSeed_${id}`)?.value || 42),
        split_by_class: !!document.getElementById(`dsSplitClass_${id}`)?.checked,
        train_all_data: !!document.getElementById(`dsTrainAll_${id}`)?.checked,
    };
}

async function saveDatasetConfig(encodedName) {
    const projectName = decodeURIComponent(encodedName);
    const payload = getDatasetConfigFromInputs(projectName);
    const data = await apiGet('/api/project/dataset_config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    });
    projectDetailCache.delete(projectName);
    alert(msg(data.message, 'OK'));
    await ensureProjectDetailLoaded(projectName);
    refreshData();
}

async function createDataset(encodedName) {
    const projectName = decodeURIComponent(encodedName);
    const payload = getDatasetConfigFromInputs(projectName);
    let data;
    if (payload.train_all_data) {
        data = await apiGet('/api/project/merge_train_valid', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ project: projectName })
        });
    } else {
        data = await apiGet('/api/project/create_dataset', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                ...payload,
                split_mode: payload.split_by_class ? 'class' : 'count'
            })
        });
    }
    projectDetailCache.delete(projectName);
    const counts = data.counts || {};
    alert(`${msg(data.message, 'OK')}\nTrain=${counts.train ?? 0}, Valid=${counts.valid ?? 0}, Test=${counts.test ?? 0}`);
    await ensureProjectDetailLoaded(projectName);
    refreshData();
}

function renderProjectProgress(progressValue, statusValue) {
    const p = Number(progressValue || 0);
    const display = statusValue === 'running' || statusValue === 'success' ? `${p.toFixed(1)}%` : '-';

    if (statusValue !== 'running' && statusValue !== 'success') {
        return `<div class="small">-</div>`;
    }

    return `
        <div class="progress-cell">
            <div class="mini-progress">
                <div class="mini-progress-fill" style="width:${Math.max(0, Math.min(100, p))}%"></div>
            </div>
            <div class="small">${display}</div>
        </div>
    `;
}

async function refreshData(prefetchedData = null, options = {}) {
    const data = prefetchedData || await apiGet('/api/state');
    const logData = options.logData || null;
    latestStateData = data;
    stateVersion = Math.max(stateVersion, Number(data.version || 0));
    processHistoryNotifications(data.history || []);

    document.getElementById('lastScan').textContent = data.last_scan || '-';
    document.getElementById('current').textContent = data.current_train_project || '-';
    document.getElementById('currentProgress').textContent = `${Number(data.current_train_progress || 0).toFixed(1)}%`;
    document.getElementById('projectCount').textContent = data.projects.length || 0;
    updateSummaryBadges(data.projects || []);

    const body = document.getElementById('projectTableBody');
    body.innerHTML = '';

    const logProject = document.getElementById('logProject');
    const currentLogSelection = logProject.value;
    logProject.innerHTML = '';

    const allSortedProjects = sortProjects(data.projects || []);
    let renderedProjects = filterProjects(allSortedProjects);

    document.getElementById('visibleCount').textContent = renderedProjects.length;

    allSortedProjects.forEach((p) => {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = p.name;
        logProject.appendChild(opt);
    });

    renderedProjects.forEach((p, index) => {
        const checked = selectedProjects.has(p.name) ? 'checked' : '';
        const runningClass = p.status === 'running' ? 'running-row' : '';
        const isExpanded = expandedProjects.has(p.name);

        const actionButtons = p.status === 'running'
            ? `
                <div class="action-wrap">
                    <button class="btn-warning btn-mini btn-icon" onclick="stopCurrentTrain()" title="${t('stop_button')}">■</button>
                    <button class="btn-light btn-mini btn-icon" onclick="openProjectEditor('${encodeURIComponent(p.name)}')" title="${t('edit_button')}">✎</button>
                    <button class="btn-light btn-mini btn-icon" onclick="duplicateProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('duplicate_button')}">⧉</button>
                    <button class="btn-warning btn-mini btn-icon" onclick="clearDatasetPrompt('${encodeURIComponent(p.name)}')" title="${t('clear_dataset_button')}">🧹</button>
                    <button class="btn-danger btn-mini btn-icon" onclick="deleteProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('delete_button')}">🗑</button>
                </div>
            `
            : p.status === 'failed'
            ? `
                <div class="action-wrap">
                    <button class="btn-danger btn-mini btn-icon" onclick="queueSingle('${encodeURIComponent(p.name)}')" title="${t('retry_button')}">↻</button>
                    <button class="btn-light btn-mini btn-icon" onclick="openProjectEditor('${encodeURIComponent(p.name)}')" title="${t('edit_button')}">✎</button>
                    <button class="btn-light btn-mini btn-icon" onclick="duplicateProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('duplicate_button')}">⧉</button>
                    <button class="btn-warning btn-mini btn-icon" onclick="clearDatasetPrompt('${encodeURIComponent(p.name)}')" title="${t('clear_dataset_button')}">🧹</button>
                    <button class="btn-danger btn-mini btn-icon" onclick="deleteProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('delete_button')}">🗑</button>
                </div>
            `
            : `
                <div class="action-wrap">
                    <button class="btn-primary btn-mini btn-icon" onclick="queueSingle('${encodeURIComponent(p.name)}')" title="${t('train_button')}">▶</button>
                    <button class="btn-light btn-mini btn-icon" onclick="openProjectEditor('${encodeURIComponent(p.name)}')" title="${t('edit_button')}">✎</button>
                    <button class="btn-light btn-mini btn-icon" onclick="duplicateProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('duplicate_button')}">⧉</button>
                    <button class="btn-warning btn-mini btn-icon" onclick="clearDatasetPrompt('${encodeURIComponent(p.name)}')" title="${t('clear_dataset_button')}">🧹</button>
                    <button class="btn-danger btn-mini btn-icon" onclick="deleteProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('delete_button')}">🗑</button>
                </div>
            `;

        const tr = document.createElement('tr');
        tr.className = runningClass;
        tr.innerHTML = `
            <td>${index + 1}</td>
            <td>
                <input
                    type="checkbox"
                    class="project-check"
                    value="${escapeHtml(p.name)}"
                    ${checked}
                    onchange="toggleProjectSelection(this)"
                >
            </td>
            <td class="name-cell">
                <div class="project-name-toggle">
                    <span class="expand-icon" onclick="toggleProjectDetail('${encodeURIComponent(p.name)}')" title="${t('click_to_expand')}">${isExpanded ? 'v' : '>'}</span>
                    <span class="project-name-link" onclick="renameProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('rename_prompt')}">${escapeHtml(p.name)}</span>
                </div>
            </td>
            <td>${badge(p.status)}</td>
            <td>${renderProjectProgress(p.progress, p.status)}</td>
            <td>${p.last_start || '-'}</td>
            <td>${p.last_end || '-'}</td>
            <td>${p.last_returncode === null ? '-' : p.last_returncode}</td>
            <td>${actionButtons}</td>
        `;
        body.appendChild(tr);

        if (isExpanded) {
            const detailTr = document.createElement('tr');
            detailTr.className = 'detail-row';
            detailTr.innerHTML = `<td colspan="9">${renderProjectDetailContent(p.name)}</td>`;
            body.appendChild(detailTr);
        }
    });

    if ((data.projects || []).length === 0) {
        body.innerHTML = `<tr><td colspan="9"><div class="muted-box">${t('project_not_found')}</div></td></tr>`;
    } else if (renderedProjects.length === 0) {
        body.innerHTML = `<tr><td colspan="9"><div class="muted-box">${t('no_match_filter')}</div></td></tr>`;
    }

    if (currentLogSelection && allSortedProjects.some(x => x.name === currentLogSelection)) {
        logProject.value = currentLogSelection;
    } else if (logData && logData.project && allSortedProjects.some(x => x.name === logData.project)) {
        logProject.value = logData.project;
    } else if (!logProject.value && allSortedProjects.length > 0) {
        logProject.value = allSortedProjects[0].name;
    }

    const allChecks = document.querySelectorAll('.project-check');
    const checkedChecks = document.querySelectorAll('.project-check:checked');
    document.getElementById('checkAll').checked = allChecks.length > 0 && allChecks.length === checkedChecks.length;

    const queueList = document.getElementById('queueList');
    if (data.queue.length > 0) {
        queueList.innerHTML = data.queue.map(item => `
            <div class="queue-item">
                <div class="queue-head">
                    <div class="queue-order">${item.order}</div>
                    <div class="small">${t('queue_order')}</div>
                </div>
                <div><b>${escapeHtml(item.name)}</b></div>
            </div>
        `).join('');
    } else {
        queueList.innerHTML = `<div class="muted-box">${t('queue_empty')}</div>`;
    }

    const historyList = document.getElementById('historyList');
    if (data.history.length > 0) {
        historyList.innerHTML = data.history.slice().reverse().map(item => `
            <div class="history-item">
                <div><b>${escapeHtml(item.project)}</b></div>
                <div class="small">${t('history_status')}: ${escapeHtml(item.status)} | ${t('history_time')}: ${escapeHtml(item.time)} | RC: ${item.returncode}</div>
            </div>
        `).join('');
    } else {
        historyList.innerHTML = `<div class="muted-box">${t('history_empty')}</div>`;
    }

    updateSelectedCount();
    updateSortIndicators();
    if (logData && logProject.value === String(logData.project || '')) {
        applyProjectLog(logData, currentLogSelection !== logProject.value);
    } else {
        refreshLogThrottled(currentLogSelection !== logProject.value);
    }
}

function renderMonitorState(s) {
    const progress = Number(s.progress || 0);
    document.getElementById('trainProgressText').textContent = `epoch ${s.epoch || 0}/${s.epochs || 0} | ${progress.toFixed(1)}%`;
    document.getElementById('trainProgressFill').style.width = `${progress}%`;

    document.getElementById('m_eta').textContent = fmtT(s.eta_sec);
    document.getElementById('m_elapsed').textContent = fmtT(s.elapsed_sec);
    document.getElementById('m_lr').textContent = f(s.lr, 5);
    document.getElementById('m_loss').textContent = f(s.loss, 4);
    document.getElementById('m_lossx').textContent = `${f(s.loss_box, 4)} / ${f(s.loss_cls, 4)} / ${f(s.loss_dfl, 4)}`;
    document.getElementById('m_ips').textContent = f(s.ips, 2);
    document.getElementById('m_etime').textContent = `${f(s.epoch_time, 2)} / ${f(s.avg_epoch_time, 2)}`;
    document.getElementById('m_bi').textContent = `${s.batch_size ?? '-'} / ${s.img_size ?? '-'}`;
    document.getElementById('m_prec').textContent = f(s.precision, 4);
    document.getElementById('m_rec').textContent = f(s.recall, 4);
    document.getElementById('m_m50').textContent = f(s.map50, 4);
    document.getElementById('m_m5095').textContent = f(s.map5095, 4);
    document.getElementById('m_best').textContent = f(s.best_map5095, 4);
    document.getElementById('m_sdir').textContent = s.save_dir || '-';
    document.getElementById('m_wts').textContent = s.weights || '-';
    document.getElementById('m_start').textContent = s.started_at || '-';

    const env = s.env || {};
    document.getElementById('m_dev').textContent = env.device || '-';
    document.getElementById('m_mem').textContent =
        (env.vram_used !== null && env.vram_used !== undefined && env.vram_total !== null && env.vram_total !== undefined)
            ? `${env.vram_used} / ${env.vram_total} GB`
            : '-';

    const ds = s.dataset || {};
    document.getElementById('m_ds').textContent = `${ds.train_images ?? '-'} / ${ds.val_images ?? '-'}`;

    const hp = s.hparams || {};
    document.getElementById('m_opt').textContent =
        `${hp.optimizer || '-'} | lr0 ${hp.lr0 ?? '-'} | mom ${hp.momentum ?? '-'} | wd ${hp.weight_decay ?? '-'}`;
}

function applyNotifyState(data = {}) {
    notifyState.enabled = !!data.enabled;
    notifyState.configured = !!data.configured;
    renderNotifyBell();
}

async function refreshTrainMonitor(prefetchedData = null) {
    const data = prefetchedData || await apiGet('/api/train_monitor/history');
    stateVersion = Math.max(stateVersion, Number(data.version || 0));
    const box = document.getElementById('trainMonitorLog');

    if (!data.ok) {
        renderMonitorState({});
        const parts = [t('monitor_no_data')];
        if (data.error) parts.push(`Error: ${data.error}`);
        if (data.url) parts.push(`URL: ${data.url}`);
        box.textContent = parts.join('\n');
        lastMonitorLogCount = 0;
        return;
    }

    renderMonitorState(data.state || {});

    const logs = data.logs || [];
    box.textContent = logs.map(x => `[${x.t}] ${x.msg}`).join('\n') || t('no_log');
    if (logs.length !== lastMonitorLogCount) {
        box.scrollTop = box.scrollHeight;
        lastMonitorLogCount = logs.length;
    }
}

function applyProjectLog(data = null, scrollToBottom = true) {
    const box = document.getElementById('logBox');
    if (!box) return;

    const log = Array.isArray(data?.log) ? data.log : [];
    box.textContent = log.join('\n') || t('no_log');
    if (scrollToBottom) {
        box.scrollTop = box.scrollHeight;
    }

    lastLogProjectLoaded = String(data?.project || '');
    lastLogRefreshTs = Date.now();
}

async function loadLog(scrollToBottom = true) {
    const sel = document.getElementById('logProject');
    if (!sel.value) {
        document.getElementById('logBox').textContent = t('no_log');
        return;
    }

    const data = await apiGet('/api/log?project=' + encodeURIComponent(sel.value) + '&tail=150');
    applyProjectLog(data, scrollToBottom);
}

async function refreshLogThrottled(force = false) {
    const sel = document.getElementById('logProject');
    const project = String(sel?.value || '');
    if (!project) {
        const box = document.getElementById('logBox');
        if (box) box.textContent = t('no_log');
        return;
    }

    const now = Date.now();
    if (!force && project === lastLogProjectLoaded && (now - lastLogRefreshTs) < 4000) {
        return;
    }
    if (logRefreshInFlight) {
        return;
    }

    logRefreshInFlight = true;
    try {
        await loadLog(false);
        lastLogProjectLoaded = project;
        lastLogRefreshTs = Date.now();
    } finally {
        logRefreshInFlight = false;
    }
}

function getRequestedLogProject() {
    const sel = document.getElementById('logProject');
    return String(sel?.value || '');
}

async function applySnapshot(snapshot = null) {
    const data = snapshot || {};
    const stateData = data.state || {};
    const monitorData = data.monitor || {};
    const notifyData = data.notify || {};
    const logData = data.log || {};

    await refreshData(stateData, { logData });
    applyNotifyState(notifyData);
    await refreshTrainMonitor(monitorData);
}

async function queueSingle(encodedName) {
    const name = decodeURIComponent(encodedName);
    resetProjectSelection();
    const data = await apiGet('/api/queue', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ project: name })
    });
    alert(msg(data.message));
    refreshAll();
}

async function queueSelected() {
    const projects = Array.from(selectedProjects);
    if (projects.length === 0) {
        alert(t('selected_none'));
        return;
    }

    const data = await apiGet('/api/queue_selected', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ projects })
    });

    resetProjectSelection();
    alert(msg(data.message, 'OK'));
    refreshAll();
}

function downloadSuccessOutputs() {
    window.location.href = '/api/download_success_outputs';
}

async function clearHistory() {
    const confirmed = await showConfirmDialog({
        title: t('clear_history_title'),
        message: t('clear_history_confirm'),
        confirmText: t('modal_confirm'),
        cancelText: t('modal_cancel'),
        confirmClass: 'btn-danger'
    });
    if (!confirmed) return;
    if (!await ensureLv2()) return;

    const data = await apiGet('/api/history/clear', { method: 'POST' });
    alert(msg(data.message, 'OK'));
    historyBootstrapped = false;
    seenHistoryKeys = new Set();
    refreshAll();
}

async function renameProjectPrompt(encodedName) {
    const project = decodeURIComponent(encodedName);
    const newName = await showInputDialog({
        title: t('rename_title'),
        message: `${project}\n\n${t('rename_prompt')}`,
        confirmText: t('modal_confirm'),
        cancelText: t('modal_cancel'),
        confirmClass: 'btn-primary',
        input: {
            type: 'text',
            value: project
        }
    });
    if (newName === null) return;
    if (!String(newName).trim()) return;
    if (!await ensureLv2()) return;

    const data = await apiGet('/api/project/rename', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ project, new_name: String(newName).trim() })
    });

    projectDetailCache.clear();
    expandedProjects.delete(project);
    selectedProjects.delete(project);
    alert(msg(data.message, 'OK'));
    refreshAll();
}

async function duplicateProjectPrompt(encodedName) {
    const project = decodeURIComponent(encodedName);
    const newName = await showInputDialog({
        title: t('duplicate_title'),
        message: `${project}\n\n${t('duplicate_prompt')}`,
        confirmText: t('modal_confirm'),
        cancelText: t('modal_cancel'),
        confirmClass: 'btn-primary',
        input: {
            type: 'text',
            value: `${project} - Copy`
        }
    });
    if (newName === null) return;
    if (!await ensureLv2()) return;

    const data = await apiGet('/api/project/duplicate', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ project, new_name: String(newName || '').trim() })
    });

    alert(msg(data.message, 'OK'));
    refreshAll();
}

async function clearDatasetPrompt(encodedName) {
    const project = decodeURIComponent(encodedName);
    const confirmed = await showConfirmDialog({
        title: t('clear_dataset_title'),
        message: `${project}\n\n${t('clear_dataset_confirm')}`,
        confirmText: t('modal_confirm'),
        cancelText: t('modal_cancel'),
        confirmClass: 'btn-warning'
    });
    if (!confirmed) return;
    if (!await ensureLv2()) return;

    const data = await apiGet('/api/project/clear_dataset', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ project })
    });

    projectDetailCache.delete(project);
    expandedProjects.delete(project);
    alert(msg(data.message, 'OK'));
    refreshAll();
}

async function deleteProjectPrompt(encodedName) {
    const project = decodeURIComponent(encodedName);
    const confirmed = await showConfirmDialog({
        title: t('delete_project_title'),
        message: `${project}\n\n${t('delete_confirm')}`,
        confirmText: t('modal_confirm'),
        cancelText: t('modal_cancel'),
        confirmClass: 'btn-danger'
    });
    if (!confirmed) return;
    if (!await ensureLv2()) return;

    const data = await apiGet('/api/project/delete', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ project })
    });

    projectDetailCache.delete(project);
    expandedProjects.delete(project);
    selectedProjects.delete(project);
    alert(msg(data.message, 'OK'));
    refreshAll();
}

async function queueAll() {
    const data = await apiGet('/api/queue_all', { method: 'POST' });
    alert(msg(data.message, `Added ${data.added.length} project(s)`));
    refreshAll();
}

async function retryFailed() {
    const data = await apiGet('/api/retry_failed', { method: 'POST' });
    alert(msg(data.message, 'OK'));
    refreshAll();
}

async function stopQueue() {
    const data = await apiGet('/api/stop_queue', { method: 'POST' });
    alert(msg(data.message, 'OK'));
    refreshAll();
}

async function scanProjects() {
    const data = await apiGet('/api/scan', { method: 'POST' });
    alert(msg(data.message));
    refreshAll();
}

async function stopCurrentTrain() {
    const data = await apiGet('/api/stop_current_train', { method: 'POST' });
    alert(msg(data.message, 'OK'));
    refreshAll();
}

async function revalidateRun(encodedProject, encodedRunFolder) {
    const project = decodeURIComponent(encodedProject);
    const runFolder = decodeURIComponent(encodedRunFolder);
    const data = await apiGet('/api/project/revalidate_run', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ project, run_folder: runFolder })
    });
    projectDetailCache.delete(project);
    alert(msg(data.message, 'OK'));
    await ensureProjectDetailLoaded(project);
    refreshData(latestStateData || undefined);
}

function openProjectUpload() {
    const input = document.getElementById('projectZipInput');
    if (!input) return;
    input.value = '';
    input.click();
}

function openProjectEditor(encodedName) {
    const projectName = decodeURIComponent(encodedName);
    const url = `/project_editor?project=${encodeURIComponent(projectName)}`;
    window.open(url, '_blank');
}

async function handleProjectUploadInput(inputEl) {
    try {
        const file = inputEl && inputEl.files && inputEl.files[0];
        if (!file) return;

        const fd = new FormData();
        fd.append('file', file);

        const data = await apiGet('/api/upload_project', {
            method: 'POST',
            body: fd
        });

        alert(msg(data.message, 'Upload success'));
        await refreshAll();
    } catch (e) {
        alert(String(e.message || e));
    } finally {
        if (inputEl) inputEl.value = '';
    }
}

async function refreshNotifyState() {
    try {
        const data = await apiGet('/api/notify/state');
        if (data.ok) {
            applyNotifyState(data);
        }
    } catch (e) {
        // ignore
    }
}

async function toggleNotify() {
    if (!notifyState.enabled && !notifyState.configured) {
        alert(t('notify_not_configured'));
        return;
    }

    const targetEnabled = !notifyState.enabled;

    const data = await apiGet('/api/notify/toggle', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ enabled: targetEnabled })
    });

    if (!data.ok) {
        alert(msg(data.message, t('notify_not_configured')));
        return;
    }

    notifyState.enabled = !!data.enabled;
    renderNotifyBell();
    alert(notifyState.enabled ? t('notify_on') : t('notify_off'));
}

async function logout() {
    try {
        await fetch('/logout', { method: 'POST' });
    } catch (e) {
        // ignore network errors on logout
    }
    window.location.href = '/login';
}

async function refreshAll() {
    const logProject = getRequestedLogProject();
    const snapshot = await apiGet(`/api/snapshot?log_project=${encodeURIComponent(logProject)}&log_tail=150`);
    await applySnapshot(snapshot);
}

async function watchStateChanges() {
    if (changeLoopStarted) return;
    changeLoopStarted = true;

    while (true) {
        try {
            const logProject = getRequestedLogProject();
            const data = await apiGet(`/api/state/changes?since=${encodeURIComponent(stateVersion)}&timeout=25&with_snapshot=1&log_project=${encodeURIComponent(logProject)}&log_tail=150`);
            if (data && data.version !== undefined && data.version !== null) {
                stateVersion = Math.max(stateVersion, Number(data.version || 0));
            }
            if (data && data.changed && data.snapshot) {
                await applySnapshot(data.snapshot);
            } else if (data && data.changed) {
                await refreshAll();
            }
        } catch (e) {
            await sleep(1500);
        }
    }
}

applyLanguage();
refreshAll().finally(() => {
    watchStateChanges();
});

