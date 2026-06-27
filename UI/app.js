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
let detailTabState = new Map();
let runDetailExpandedState = new Map();
let datasetDraftState = new Map();
let pendingProjectDataUpload = '';
let uploadProgressVisible = false;
let sidePanelHidden = localStorage.getItem('train_monitor_side_panel_hidden') === '1';
let queueRecoveryPromptShown = false;

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
        logout: 'Dang xuat',
        root_dir: 'ROOT_DIR',
        last_scan: 'Scan gáº§n nháº¥t',
        current_training: 'Äang train',
        current_train_percent: '% train hiá»‡n táº¡i',
        estimated_finish: 'Dự kiến xong',
        project_count: 'Sá»‘ project',
        train_monitor: 'Train monitor',
        scan_projects: 'QuĂ©t project',
        upload_project: 'Upload project (.zip)',
        add_data_zip: 'Them data (.zip)',
        backup_project: 'Backup project',
        backup_selected: 'Backup da chon',
        backup_starting: 'Dang bat dau backup...',
        backup_progress: 'Dang backup project...',
        backup_done: 'Backup hoan thanh',
        backup_eta: 'Du kien con',
        backup_target_exists: 'Thu muc backup da ton tai',
        backup_batch_progress: 'Dang backup project da chon...',
        backup_batch_done: 'Backup selected hoan thanh',
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
        telegram_notify: 'Thong bao Telegram',
        hide_side_panel: 'An panel phai',
        show_side_panel: 'Hien panel phai',
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
        project_task_starting: 'Dang bat dau xu ly project...',
        project_task_progress: 'Dang xu ly project...',
        project_task_done: 'Xu ly project hoan thanh',
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
        dataset_task_starting: 'Dang bat dau xu ly dataset...',
        dataset_task_progress: 'Dang xu ly dataset...',
        dataset_task_done: 'Xu ly dataset hoan thanh',
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
        label_source_image: 'Relabel -> Train',
        source_image_missing: 'Khong tim thay anh goc trong image',
        revalidate_run: 'Re-validate',
        revalidate_starting: 'Dang bat dau revalidate...',
        revalidate_progress: 'Dang revalidate...',
        revalidate_done: 'Revalidate hoan thanh',
        detail_tab_testing: 'Model Testing',
        model_testing: 'Model Testing',
        run_model_testing: 'Run Testing',
        model_testing_results: 'Testing results',
        model_testing_exports: 'Export CSV',
        testing_valid_images: 'Valid sample images',
        testing_misclassified_images: 'Misclassified sample images',
        testing_correct_objects: 'Correct objects',
        testing_error_objects: 'Error objects',
        testing_metric: 'Metric',
        testing_value: 'Value',
        correct_count: 'Correct',
        wrong_class_count: 'Wrong class',
        missed_bg_count: 'Missed as bg',
        model_testing_starting: 'Dang bat dau model testing...',
        model_testing_progress: 'Dang model testing...',
        model_testing_done: 'Model testing hoan thanh',
        no_model_testing: 'Chua co ket qua Model Testing',
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
        uploading_data: 'Äang thêm data...',
        server_processing: 'Server dang xu ly file vua upload...',
        detail_tab_dataset: 'Dataset',
        detail_tab_output: 'Output',
        detail_tab_metrics: 'Metrics',
        detail_tab_labels: 'Labels',
        labels_panel_desc: 'Mo Label Editor hoac them data zip cho project nay.',
        open_label_editor: 'Mo Label Editor',
        add_data_zip_button: 'Them data zip',
        continue_last_session_title: 'Phuc hoi session truoc',
        continue_last_session_message: 'Phat hien queue session truoc$PROJECTS$.\n\nBan muon tiep tuc hay bo qua?',
        continue_last_session_confirm: 'Continue last session',
        continue_last_session_ignore: 'Ignore',
        continue_last_session_done: 'Da tiep tuc session truoc',
        continue_last_session_ignored: 'Da bo qua session truoc',
    },
    en: {
        app_title: 'AI Train Monitor',
        app_subtitle: 'Display the active project and training percentage',
        language_label: 'Language',
        logout: 'Logout',
        root_dir: 'ROOT_DIR',
        last_scan: 'Last scan',
        current_training: 'Training now',
        current_train_percent: 'Current train %',
        estimated_finish: 'Estimated finish',
        project_count: 'Projects',
        train_monitor: 'Train monitor',
        scan_projects: 'Scan projects',
        upload_project: 'Upload project (.zip)',
        add_data_zip: 'Add data (.zip)',
        backup_project: 'Backup project',
        backup_selected: 'Backup selected',
        backup_starting: 'Starting backup...',
        backup_progress: 'Backing up project...',
        backup_done: 'Backup completed',
        backup_eta: 'ETA',
        backup_target_exists: 'Backup target already exists',
        backup_batch_progress: 'Backing up selected projects...',
        backup_batch_done: 'Selected backup completed',
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
        telegram_notify: 'Telegram notification',
        hide_side_panel: 'Hide right panel',
        show_side_panel: 'Show right panel',
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
        project_task_starting: 'Starting project task...',
        project_task_progress: 'Processing project...',
        project_task_done: 'Project task completed',
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
        dataset_task_starting: 'Starting dataset task...',
        dataset_task_progress: 'Processing dataset...',
        dataset_task_done: 'Dataset task completed',
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
        label_source_image: 'Relabel -> Train',
        source_image_missing: 'Source image not found in image folder',
        revalidate_run: 'Re-validate',
        revalidate_starting: 'Starting re-validation...',
        revalidate_progress: 'Re-validating...',
        revalidate_done: 'Re-validation completed',
        detail_tab_testing: 'Model Testing',
        model_testing: 'Model Testing',
        run_model_testing: 'Run Testing',
        model_testing_results: 'Testing results',
        model_testing_exports: 'Export CSV',
        testing_valid_images: 'Valid sample images',
        testing_misclassified_images: 'Misclassified sample images',
        testing_correct_objects: 'Correct objects',
        testing_error_objects: 'Error objects',
        testing_metric: 'Metric',
        testing_value: 'Value',
        correct_count: 'Correct',
        wrong_class_count: 'Wrong class',
        missed_bg_count: 'Missed as bg',
        model_testing_starting: 'Starting model testing...',
        model_testing_progress: 'Running model testing...',
        model_testing_done: 'Model testing completed',
        no_model_testing: 'No Model Testing results yet',
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
        uploading_data: 'Uploading data...',
        server_processing: 'Server is processing the uploaded file...',
        detail_tab_dataset: 'Dataset',
        detail_tab_output: 'Output',
        detail_tab_metrics: 'Metrics',
        detail_tab_labels: 'Labels',
        labels_panel_desc: 'Open Label Editor or import a data zip for this project.',
        open_label_editor: 'Open Label Editor',
        add_data_zip_button: 'Add data zip',
        continue_last_session_title: 'Restore previous session',
        continue_last_session_message: 'A previous queue session was found$PROJECTS$.\n\nDo you want to continue it or ignore it?',
        continue_last_session_confirm: 'Continue last session',
        continue_last_session_ignore: 'Ignore',
        continue_last_session_done: 'Previous session continued',
        continue_last_session_ignored: 'Previous session ignored',
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
    const title = notifyState.enabled ? t('notify_title_on') : t('notify_title_off');
    bell.title = title;
    bell.setAttribute('aria-label', title);
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

function updateHeroSubtitle(projectName = '') {
    const el = document.getElementById('heroSubtitle');
    if (!el) return;
    const label = 'Project';
    const name = String(projectName || '').trim() || '-';
    el.textContent = `${label}: ${name}`;
}

function applySidePanelState() {
    const layout = document.querySelector('.layout');
    const toggleBtn = document.getElementById('toggleSidePanelBtn');
    if (layout) {
        layout.classList.toggle('layout-side-hidden', sidePanelHidden);
    }
    if (toggleBtn) {
        toggleBtn.textContent = sidePanelHidden ? t('show_side_panel') : t('hide_side_panel');
    }
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
        if (el.classList && el.classList.contains('toolbar-icon-btn')) {
            const textEl = el.querySelector('.toolbar-text');
            if (textEl) {
                textEl.textContent = t(key);
            }
            return;
        }
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

    document.querySelectorAll('[data-tooltip-key]').forEach(el => {
        const key = el.getAttribute('data-tooltip-key');
        const text = t(key);
        el.title = text;
        el.setAttribute('aria-label', text);
    });

    localStorage.setItem('train_monitor_lang', currentLang);

    renderNotifyBell();
    updateSortIndicators();
    updateSummaryBadgeActive();
    updateHeroSubtitle((latestStateData && latestStateData.current_train_project) || '');
}

function applyToolbarIcons() {
    const cfg = window.TOOLBAR_ICON_CONFIG || {};
    const iconMap = cfg.icons || {};

    document.querySelectorAll('[data-icon-key]').forEach(btn => {
        const key = btn.getAttribute('data-icon-key');
        const iconEl = btn.querySelector('.toolbar-icon');
        if (!iconEl) return;

        const iconInfo = iconMap[key];
        if (!iconInfo || !iconInfo.path) {
            iconEl.classList.remove('has-image');
            return;
        }

        const altText = t(btn.getAttribute('data-tooltip-key') || key);
        iconEl.innerHTML = `<img src="${escapeAttr(iconInfo.path)}" alt="${escapeAttr(altText)}">`;
        iconEl.classList.add('has-image');
    });
}

function updateStickyLayoutMetrics() {
    const root = document.documentElement;
    const stickyPanel = document.querySelector('.sticky-top-panel');
    const projectBadges = document.getElementById('projectSummaryBadges');
    if (!root || !stickyPanel) return;

    const rect = stickyPanel.getBoundingClientRect();
    const height = Math.ceil(rect.height || 0);
    if (height > 0) {
        root.style.setProperty('--sticky-header-height', `${height}px`);
    }

    if (projectBadges) {
        const badgeHeight = Math.ceil(projectBadges.getBoundingClientRect().height || 0);
        if (badgeHeight > 0) {
            root.style.setProperty('--project-filter-height', `${badgeHeight}px`);
        }
    }
}

function setLanguage(lang) {
    currentLang = lang;
    applyLanguage();
    applyToolbarIcons();
    applySidePanelState();
    updateStickyLayoutMetrics();
    refreshAll();
}

function toggleSidePanel() {
    sidePanelHidden = !sidePanelHidden;
    localStorage.setItem('train_monitor_side_panel_hidden', sidePanelHidden ? '1' : '0');
    applySidePanelState();
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

let modalSearchLockState = null;

function lockSearchInputForModal() {
    const searchEl = document.getElementById('searchInput');
    if (!searchEl) return;
    modalSearchLockState = {
        value: String(searchEl.value || ''),
        readOnly: !!searchEl.readOnly,
        disabled: !!searchEl.disabled,
    };
    searchEl.blur();
    searchEl.readOnly = true;
    searchEl.disabled = true;
}

function unlockSearchInputForModal() {
    const searchEl = document.getElementById('searchInput');
    if (!searchEl || !modalSearchLockState) return;
    searchEl.disabled = !!modalSearchLockState.disabled;
    searchEl.readOnly = !!modalSearchLockState.readOnly;
    if (String(searchEl.value || '') !== String(modalSearchLockState.value || '')) {
        searchEl.value = modalSearchLockState.value;
    }
    modalSearchLockState = null;
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
    const inputType = escapeAttr(input?.type || 'text');
    const inputValue = escapeAttr(input?.value || '');
    const inputPlaceholder = escapeAttr(input?.placeholder || '');
    const inputAutocomplete = escapeAttr(input?.autocomplete || 'off');
    const modalInputName = `webModalInput_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    lockSearchInputForModal();

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
                                name="${modalInputName}"
                                type="${inputType}"
                                value="${inputValue}"
                                placeholder="${inputPlaceholder}"
                                autocomplete="${inputAutocomplete}"
                                autocapitalize="off"
                                autocorrect="off"
                                spellcheck="false"
                                data-lpignore="true"
                                data-1p-ignore="true"
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
            unlockSearchInputForModal();
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

function fmtEstimatedFinish(sec) {
    if (sec === null || sec === undefined || Number.isNaN(Number(sec))) return '-';
    sec = Number(sec);
    if (sec <= 0) return '-';

    const dt = new Date(Date.now() + (sec * 1000));
    if (Number.isNaN(dt.getTime())) return '-';

    const dd = String(dt.getDate()).padStart(2, '0');
    const mm = String(dt.getMonth() + 1).padStart(2, '0');
    const hh = String(dt.getHours()).padStart(2, '0');
    const mi = String(dt.getMinutes()).padStart(2, '0');
    const ss = String(dt.getSeconds()).padStart(2, '0');
    return `${dd}/${mm} ${hh}:${mi}:${ss}`;
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
    refreshData(latestStateData || undefined);
}

function clearAllProjects() {
    const checkboxes = document.querySelectorAll('.project-check');
    checkboxes.forEach(cb => cb.checked = false);
    selectedProjects.clear();
    document.getElementById('checkAll').checked = false;
    updateSelectedCount();
    refreshData(latestStateData || undefined);
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
    refreshData(latestStateData || undefined);
}

function toggleProjectSelection(checkbox) {
    const name = checkbox.value;
    if (checkbox.checked) selectedProjects.add(name);
    else selectedProjects.delete(name);

    const allChecks = document.querySelectorAll('.project-check');
    const checkedChecks = document.querySelectorAll('.project-check:checked');
    document.getElementById('checkAll').checked = allChecks.length > 0 && allChecks.length === checkedChecks.length;

    updateSelectedCount();
    refreshData(latestStateData || undefined);
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

function shouldUseLightweightProjectRefresh(nextStateData) {
    if (expandedProjects.size === 0) return false;
    if (searchQuery || statusFilter !== 'all') return false;

    const prevProjects = Array.isArray(latestStateData?.projects) ? latestStateData.projects : [];
    const nextProjects = Array.isArray(nextStateData?.projects) ? nextStateData.projects : [];
    if (prevProjects.length !== nextProjects.length) return false;

    for (let i = 0; i < prevProjects.length; i += 1) {
        if (String(prevProjects[i]?.name || '') !== String(nextProjects[i]?.name || '')) {
            return false;
        }
    }
    return true;
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

function getProjectDetailTab(projectName) {
    const key = String(projectName || '');
    if (!detailTabState.has(key)) {
        detailTabState.set(key, 'dataset');
    }
    return detailTabState.get(key);
}

function setProjectDetailTab(encodedName, tabKey = 'dataset') {
    const projectName = decodeURIComponent(String(encodedName || ''));
    detailTabState.set(projectName, String(tabKey || 'dataset'));
    refreshData(latestStateData || undefined);
}

function getDatasetDraft(projectName, fallback = {}) {
    const key = String(projectName || '');
    if (!datasetDraftState.has(key)) {
        datasetDraftState.set(key, { ...(fallback || {}) });
    }
    return datasetDraftState.get(key);
}

function updateDatasetDraft(projectName, field, value) {
    const draft = getDatasetDraft(projectName, {});
    draft[String(field || '')] = value;
}

function isRunDetailExpanded(runKey) {
    return runDetailExpandedState.get(String(runKey || '')) === true;
}

function toggleRunDetail(encodedProjectName, encodedRunFolder) {
    const projectName = decodeURIComponent(String(encodedProjectName || ''));
    const runFolder = decodeURIComponent(String(encodedRunFolder || ''));
    const runKey = `${projectName}::${runFolder}`;
    runDetailExpandedState.set(runKey, !isRunDetailExpanded(runKey));
    refreshData(latestStateData || undefined);
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
    const datasetDraft = getDatasetDraft(projectName, datasetConfig);
    const activeTab = getProjectDetailTab(projectName);

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
            const runExpanded = isRunDetailExpanded(runKey);
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
                                        <th>${t('correct_count')}</th>
                                        <th>${t('wrong_class_count')}</th>
                                        <th>${t('missed_bg_count')}</th>
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
                                            <td>${escapeHtml(row.correct ?? 0)}</td>
                                            <td>${escapeHtml(row.mis_as_other_classes ?? 0)}</td>
                                            <td>${escapeHtml(row.missed_as_background ?? 0)}</td>
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
                                        <div class="run-image-card">
                                            <a class="run-image-preview-link" href="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(item.relative_path)}" target="_blank">
                                                <img src="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(item.relative_path)}" alt="${escapeHtml(item.image_name)}">
                                            </a>
                                            <div class="run-image-name">
                                                <div>${escapeHtml(item.gt_class_name || '-')} -> ${escapeHtml(item.pred_class_name || '-')}</div>
                                                <div>${escapeHtml(item.image_name || '-')}</div>
                                                ${item.source_image_rel ? `<div class="small">image/${escapeHtml(item.source_image_rel)}</div>` : `<div class="small">${t('source_image_missing')}</div>`}
                                            </div>
                                            <div class="run-image-actions">
                                                ${item.source_image_rel ? `
                                                    <button
                                                        type="button"
                                                        class="btn-primary btn-mini"
                                                        onclick="openProjectEditorAtImage('${encodeURIComponent(projectName)}', '${encodeURIComponent(item.source_image_rel)}', '${encodeURIComponent(item.val_image_rel_path || item.source_image_rel || '')}')"
                                                    >${t('label_source_image')}</button>
                                                ` : ''}
                                            </div>
                                        </div>
                                    `).join('')}
                                </div>
                            ` : `<div class="muted-box">No sample images match the current class/pair filter.</div>`}
                        </div>
                    ` : ''}
                    <div class="detail-actions mb8">
                        <button class="btn-light btn-mini" onclick="revalidateRun('${encodeURIComponent(projectName)}', '${encodeURIComponent(run.run_folder)}')">${t('revalidate_run')}</button>
                    </div>
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
                <details class="run-detail-card" ${runExpanded ? 'open' : ''}>
                    <summary onclick="event.preventDefault(); toggleRunDetail('${encodeURIComponent(projectName)}', '${encodeURIComponent(run.run_folder)}')">
                        <span>${escapeHtml(run.run_folder)}</span>
                        <span class="small">Images: ${escapeHtml(run.image_count ?? 0)} | CSV: ${csvInfo.exists ? escapeHtml(csvInfo.row_count ?? 0) : 0}</span>
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

    const datasetTabHtml = `
        <div class="detail-grid">
            <div class="detail-box">
                <div class="detail-title">${t('dataset_config_title')}</div>
                <div class="detail-stats">
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">${t('train_ratio')}</div>
                        <div class="detail-stat-value"><input type="number" min="0" max="100" id="dsTrain_${projectDomId(projectName)}" value="${escapeHtml(datasetDraft.train_percent ?? 80)}" oninput="updateDatasetDraft('${encodeURIComponent(projectName)}', 'train_percent', Number(this.value || 0))"></div>
                    </div>
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">${t('valid_ratio')}</div>
                        <div class="detail-stat-value"><input type="number" min="0" max="100" id="dsValid_${projectDomId(projectName)}" value="${escapeHtml(datasetDraft.valid_percent ?? 20)}" oninput="updateDatasetDraft('${encodeURIComponent(projectName)}', 'valid_percent', Number(this.value || 0))"></div>
                    </div>
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">${t('test_ratio')}</div>
                        <div class="detail-stat-value"><input type="number" min="0" max="100" id="dsTest_${projectDomId(projectName)}" value="${escapeHtml(datasetDraft.test_percent ?? 0)}" oninput="updateDatasetDraft('${encodeURIComponent(projectName)}', 'test_percent', Number(this.value || 0))"></div>
                    </div>
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">${t('shuffle_dataset')}</div>
                        <div class="detail-stat-value"><input type="checkbox" id="dsShuffle_${projectDomId(projectName)}" ${(datasetDraft.shuffle ?? true) ? 'checked' : ''} onchange="updateDatasetDraft('${encodeURIComponent(projectName)}', 'shuffle', this.checked)"></div>
                    </div>
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">${t('seed_value')}</div>
                        <div class="detail-stat-value"><input type="number" id="dsSeed_${projectDomId(projectName)}" value="${escapeHtml(datasetDraft.seed ?? 42)}" oninput="updateDatasetDraft('${encodeURIComponent(projectName)}', 'seed', Number(this.value || 42))"></div>
                    </div>
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">${t('split_by_class')}</div>
                        <div class="detail-stat-value"><input type="checkbox" id="dsSplitClass_${projectDomId(projectName)}" ${(datasetDraft.split_by_class ?? false) ? 'checked' : ''} onchange="updateDatasetDraft('${encodeURIComponent(projectName)}', 'split_by_class', this.checked)"></div>
                    </div>
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">${t('train_all_data')}</div>
                        <div class="detail-stat-value"><input type="checkbox" id="dsTrainAll_${projectDomId(projectName)}" ${(datasetDraft.train_all_data ?? false) ? 'checked' : ''} onchange="updateDatasetDraft('${encodeURIComponent(projectName)}', 'train_all_data', this.checked)"></div>
                    </div>
                </div>
                <div class="small mb8">${t('dataset_ratio_hint')}</div>
                <div class="detail-actions">
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
        </div>
    `;

    const outputTabHtml = `
        <div class="detail-grid">
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
    `;

    const metricsTabHtml = `
        <div class="detail-box detail-box-full">
            <div class="detail-title">${t('model_train_details')}</div>
            ${runDetailsHtml}
        </div>
    `;

    const testingTabHtml = (data.run_details && data.run_details.length > 0)
        ? data.run_details.map(run => {
            const testing = run.model_testing || {};
            const testingCsv = testing.results_csv || {};
            const testingSummary = testingCsv.summary || {};
            const testingConfusion = testing.confusion_analysis || {};
            const topErrorClasses = Array.isArray(testingConfusion.top_error_classes) ? testingConfusion.top_error_classes : [];
            const topConfusions = Array.isArray(testingConfusion.top_confusions) ? testingConfusion.top_confusions : [];
            const sampleItems = Array.isArray(testingConfusion.sample_items) ? testingConfusion.sample_items : [];
            const exportFiles = Array.isArray(testing.export_files) ? testing.export_files : [];
            const summaryCounts = testing.summary_counts || {};

            return `
                <div class="detail-box detail-box-full">
                    <div class="detail-title">${t('model_testing')} | ${escapeHtml(run.run_folder)}</div>
                    <div class="detail-actions mb8">
                        <button class="btn-primary btn-mini" onclick="runModelTesting('${encodeURIComponent(projectName)}', '${encodeURIComponent(run.run_folder)}')">${t('run_model_testing')}</button>
                        <button class="btn-success btn-mini" onclick="showMsaSampleUpload('${encodeURIComponent(projectName)}', '${encodeURIComponent(run.run_folder)}')">MSA Sample Test</button>
                    </div>
                    ${testing.exists ? `
                        <div class="detail-stats mb8">
                            <div class="detail-stat-item">
                                <div class="detail-stat-label">${t('testing_valid_images')}</div>
                                <div class="detail-stat-value">${escapeHtml(summaryCounts.valid_sample_images ?? 0)}</div>
                            </div>
                            <div class="detail-stat-item">
                                <div class="detail-stat-label">${t('testing_misclassified_images')}</div>
                                <div class="detail-stat-value">${escapeHtml(summaryCounts.misclassified_sample_images ?? 0)}</div>
                            </div>
                            <div class="detail-stat-item">
                                <div class="detail-stat-label">${t('testing_correct_objects')}</div>
                                <div class="detail-stat-value">${escapeHtml(summaryCounts.total_correct_objects ?? 0)}</div>
                            </div>
                            <div class="detail-stat-item">
                                <div class="detail-stat-label">${t('testing_error_objects')}</div>
                                <div class="detail-stat-value">${escapeHtml(summaryCounts.total_error_objects ?? 0)}</div>
                            </div>
                            <div class="detail-stat-item">
                                <div class="detail-stat-label">mAP50-95</div>
                                <div class="detail-stat-value">${escapeHtml(testingSummary.map5095 ?? '-')}</div>
                            </div>
                            <div class="detail-stat-item">
                                <div class="detail-stat-label">Precision / Recall</div>
                                <div class="detail-stat-value">${escapeHtml(testingSummary.precision ?? '-')} | ${escapeHtml(testingSummary.recall ?? '-')}</div>
                            </div>
                        </div>
                        <div class="output-list mb8">
                            <div class="detail-title">${t('model_testing_results')}</div>
                            <table class="output-table">
                                <thead>
                                    <tr>
                                        <th>${t('testing_metric')}</th>
                                        <th>${t('testing_value')}</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td>${t('testing_valid_images')}</td>
                                        <td>${escapeHtml(summaryCounts.valid_sample_images ?? 0)}</td>
                                    </tr>
                                    <tr>
                                        <td>${t('testing_misclassified_images')}</td>
                                        <td>${escapeHtml(summaryCounts.misclassified_sample_images ?? 0)}</td>
                                    </tr>
                                    <tr>
                                        <td>${t('testing_correct_objects')}</td>
                                        <td>${escapeHtml(summaryCounts.total_correct_objects ?? 0)}</td>
                                    </tr>
                                    <tr>
                                        <td>${t('testing_error_objects')}</td>
                                        <td>${escapeHtml(summaryCounts.total_error_objects ?? 0)}</td>
                                    </tr>
                                    <tr>
                                        <td>mAP50-95</td>
                                        <td>${escapeHtml(testingSummary.map5095 ?? '-')}</td>
                                    </tr>
                                    <tr>
                                        <td>Precision</td>
                                        <td>${escapeHtml(testingSummary.precision ?? '-')}</td>
                                    </tr>
                                    <tr>
                                        <td>Recall</td>
                                        <td>${escapeHtml(testingSummary.recall ?? '-')}</td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                        ${exportFiles.length > 0 ? `
                            <div class="detail-title">${t('model_testing_exports')}</div>
                            <div class="detail-actions mb8">
                                ${exportFiles.map(file => `
                                    <a
                                        class="download-link"
                                        href="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(file.relative_path)}"
                                        download
                                    >${escapeHtml(file.name)}</a>
                                `).join('')}
                            </div>
                        ` : ''}
                        ${topErrorClasses.length > 0 ? `
                            <div class="output-list mb8">
                                <div class="detail-title">${t('error_by_class')}</div>
                                <table class="output-table">
                                <thead>
                                    <tr>
                                        <th>Class</th>
                                        <th>${t('error_rate')}</th>
                                        <th>${t('correct_count')}</th>
                                        <th>${t('wrong_class_count')}</th>
                                        <th>${t('missed_bg_count')}</th>
                                        <th>Errors</th>
                                        <th>Total</th>
                                    </tr>
                                </thead>
                                <tbody>
                                        ${topErrorClasses.map(row => `
                                            <tr>
                                                <td>${escapeHtml(row.gt_class_name ?? '-')}</td>
                                                <td>${escapeHtml(((Number(row.error_rate || 0)) * 100).toFixed(2))}%</td>
                                                <td>${escapeHtml(row.correct ?? 0)}</td>
                                                <td>${escapeHtml(row.mis_as_other_classes ?? 0)}</td>
                                                <td>${escapeHtml(row.missed_as_background ?? 0)}</td>
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
                                <div class="detail-title">${t('confusion_pairs')}</div>
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
                                        ${topConfusions.map(row => `
                                            <tr>
                                                <td>${escapeHtml(row.gt_class_name ?? '-')}</td>
                                                <td>${escapeHtml(row.pred_class_name ?? '-')}</td>
                                                <td>${escapeHtml(((Number(row.rate_over_gt || 0)) * 100).toFixed(2))}%</td>
                                                <td>${escapeHtml(row.count ?? 0)}</td>
                                            </tr>
                                        `).join('')}
                                    </tbody>
                                </table>
                            </div>
                        ` : ''}
                        ${sampleItems.length > 0 ? `
                            <div class="detail-box detail-box-full mb8">
                                <div class="detail-title">${t('misclassified_samples')}</div>
                                <div class="run-image-grid">
                                    ${sampleItems.map(item => `
                                        <div class="run-image-card">
                                            <a class="run-image-preview-link" href="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(item.relative_path)}" target="_blank">
                                                <img src="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(item.relative_path)}" alt="${escapeHtml(item.image_name)}">
                                            </a>
                                            <div class="run-image-name">
                                                <div>${escapeHtml(item.gt_class_name || '-')} -> ${escapeHtml(item.pred_class_name || '-')}</div>
                                                <div>${escapeHtml(item.image_name || '-')}</div>
                                            </div>
                                        </div>
                                    `).join('')}
                                </div>
                            </div>
                        ` : ''}
                        ${run.msa_testing && run.msa_testing.exists ? `
                            <div class="detail-box detail-box-full mb8" style="border: 2px solid #4CAF50;">
                                <div class="detail-title">📊 MSA Sample Results</div>
                                <div class="detail-stats mb8">
                                    <div class="detail-stat-item">
                                        <div class="detail-stat-label">Total Samples</div>
                                        <div class="detail-stat-value">${run.msa_testing.results?.total_images || 0}</div>
                                    </div>
                                    <div class="detail-stat-item">
                                        <div class="detail-stat-label">Incorrect</div>
                                        <div class="detail-stat-value">${run.msa_testing.results?.wrong_images || 0}</div>
                                    </div>
                                    <div class="detail-stat-item">
                                        <div class="detail-stat-label">Error Rate</div>
                                        <div class="detail-stat-value" style="color: ${Number(run.msa_testing.results?.error_rate || 0) > 20 ? '#f44336' : '#4CAF50'};">${Number(run.msa_testing.results?.error_rate || 0).toFixed(2)}%</div>
                                    </div>
                                </div>
                                <div class="run-image-grid">
                                    ${(run.msa_testing.results?.sample_items || []).map(item => `
                                        <div class="run-image-card">
                                            <div style="display:flex; gap:4px; height:200px;">
                                                <div style="flex:1; overflow:hidden; border-right:2px solid #ccc;">
                                                    <img src="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(item.gt_vis_relative_path)}" alt="GT" style="width:100%; height:100%; object-fit:cover;">
                                                    <div style="background:#e8f5e9; padding:4px; text-align:center; font-size:11px;">GT</div>
                                                </div>
                                                <div style="flex:1; overflow:hidden;">
                                                    <img src="/api/output_file?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(item.pred_vis_relative_path)}" alt="Pred" style="width:100%; height:100%; object-fit:cover;">
                                                    <div style="background:#fff3e0; padding:4px; text-align:center; font-size:11px;">Pred</div>
                                                </div>
                                            </div>
                                            <div class="run-image-name">
                                                <div style="font-size:11px;">${item.status === 'wrong' ? '❌' : '✓'} ${escapeHtml(item.image_name)}</div>
                                            </div>
                                        </div>
                                    `).join('')}
                                </div>
                            </div>
                        ` : ''}
                    ` : `<div class="muted-box">${t('no_model_testing')}</div>`}
                </div>
            `;
        }).join('')
        : `<div class="muted-box">${t('no_model_testing')}</div>`;

    const labelsTabHtml = `
        <div class="detail-box detail-box-full">
            <div class="detail-title">${t('label_editor')}</div>
            <div class="small mb18">${t('labels_panel_desc')}</div>
            <div class="detail-actions">
                <button class="btn-light btn-mini" onclick="openProjectEditor('${encodeURIComponent(projectName)}')">${t('open_label_editor')}</button>
                <button class="btn-success btn-mini" onclick="openProjectDataUpload('${encodeURIComponent(projectName)}')">${t('add_data_zip_button')}</button>
            </div>
        </div>
    `;

    return `
        <div class="detail-panel">
            <div class="detail-tabs">
                <button class="detail-tab-btn ${activeTab === 'dataset' ? 'active' : ''}" onclick="setProjectDetailTab('${encodeURIComponent(projectName)}', 'dataset')">${t('detail_tab_dataset')}</button>
                <button class="detail-tab-btn ${activeTab === 'output' ? 'active' : ''}" onclick="setProjectDetailTab('${encodeURIComponent(projectName)}', 'output')">${t('detail_tab_output')}</button>
                <button class="detail-tab-btn ${activeTab === 'metrics' ? 'active' : ''}" onclick="setProjectDetailTab('${encodeURIComponent(projectName)}', 'metrics')">${t('detail_tab_metrics')}</button>
                <button class="detail-tab-btn ${activeTab === 'testing' ? 'active' : ''}" onclick="setProjectDetailTab('${encodeURIComponent(projectName)}', 'testing')">${t('detail_tab_testing')}</button>
                <button class="detail-tab-btn ${activeTab === 'labels' ? 'active' : ''}" onclick="setProjectDetailTab('${encodeURIComponent(projectName)}', 'labels')">${t('detail_tab_labels')}</button>
            </div>
            <div class="detail-tab-panel ${activeTab === 'dataset' ? 'active' : ''}">${datasetTabHtml}</div>
            <div class="detail-tab-panel ${activeTab === 'output' ? 'active' : ''}">${outputTabHtml}</div>
            <div class="detail-tab-panel ${activeTab === 'metrics' ? 'active' : ''}">${metricsTabHtml}</div>
            <div class="detail-tab-panel ${activeTab === 'testing' ? 'active' : ''}">${testingTabHtml}</div>
            <div class="detail-tab-panel ${activeTab === 'labels' ? 'active' : ''}">${labelsTabHtml}</div>
        </div>
    `;
}

function getDatasetConfigFromInputs(projectName) {
    const draft = getDatasetDraft(projectName, {});
    const id = projectDomId(projectName);
    return {
        project: projectName,
        train_percent: Number(document.getElementById(`dsTrain_${id}`)?.value ?? draft.train_percent ?? 0),
        valid_percent: Number(document.getElementById(`dsValid_${id}`)?.value ?? draft.valid_percent ?? 0),
        test_percent: Number(document.getElementById(`dsTest_${id}`)?.value ?? draft.test_percent ?? 0),
        shuffle: !!(document.getElementById(`dsShuffle_${id}`)?.checked ?? draft.shuffle),
        seed: Number(document.getElementById(`dsSeed_${id}`)?.value ?? draft.seed ?? 42),
        split_by_class: !!(document.getElementById(`dsSplitClass_${id}`)?.checked ?? draft.split_by_class),
        train_all_data: !!(document.getElementById(`dsTrainAll_${id}`)?.checked ?? draft.train_all_data),
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
    datasetDraftState.set(projectName, { ...(data.config || payload) });
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
        data = await runDatasetTask(
            projectName,
            '/api/project/merge_train_valid',
            { project: projectName },
            t('merge_train_valid')
        );
    } else {
        data = await runDatasetTask(
            projectName,
            '/api/project/create_dataset',
            {
                ...payload,
                split_mode: payload.split_by_class ? 'class' : 'count'
            },
            t('create_dataset')
        );
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
    const preserveScroll = options.preserveScroll !== false;
    const lightweight = options.lightweight === true;
    const prevScrollX = preserveScroll ? window.scrollX : 0;
    const prevScrollY = preserveScroll ? window.scrollY : 0;
    const data = prefetchedData || await apiGet('/api/state');
    const logData = options.logData || null;
    latestStateData = data;
    stateVersion = Math.max(stateVersion, Number(data.version || 0));
    processHistoryNotifications(data.history || []);

    document.getElementById('lastScan').textContent = data.last_scan || '-';
    updateHeroSubtitle(data.current_train_project || '');
    document.getElementById('currentProgress').textContent = `${Number(data.current_train_progress || 0).toFixed(1)}%`;
    document.getElementById('projectCount').textContent = data.projects.length || 0;
    updateSummaryBadges(data.projects || []);

    const allSortedProjects = sortProjects(data.projects || []);
    let renderedProjects = filterProjects(allSortedProjects);
    document.getElementById('visibleCount').textContent = renderedProjects.length;

    const logProject = document.getElementById('logProject');
    const currentLogSelection = logProject.value;

    const body = document.getElementById('projectTableBody');
    const canLightweightRender = lightweight
        && !!body
        && (!!body.querySelector('.project-check'))
        && shouldUseLightweightProjectRefresh(data);

    if (canLightweightRender) {
        allSortedProjects.forEach((p) => {
            const row = document.querySelector(`input.project-check[value="${CSS.escape(p.name)}"]`)?.closest('tr');
            if (!row) return;

            const cells = row.children;
            if (cells[3]) cells[3].innerHTML = badge(p.status);
            if (cells[4]) cells[4].innerHTML = renderProjectProgress(p.progress, p.status);
            if (cells[5]) cells[5].textContent = p.last_start || '-';
            if (cells[6]) cells[6].textContent = p.last_end || '-';
            if (cells[7]) cells[7].textContent = p.last_returncode === null ? '-' : p.last_returncode;
            row.classList.toggle('running-row', p.status === 'running');
        });

        logProject.innerHTML = '';
        allSortedProjects.forEach((p) => {
            const opt = document.createElement('option');
            opt.value = p.name;
            opt.textContent = p.name;
            logProject.appendChild(opt);
        });

        if (currentLogSelection && allSortedProjects.some(x => x.name === currentLogSelection)) {
            logProject.value = currentLogSelection;
        } else if (logData && logData.project && allSortedProjects.some(x => x.name === logData.project)) {
            logProject.value = logData.project;
        } else if (!logProject.value && allSortedProjects.length > 0) {
            logProject.value = allSortedProjects[0].name;
        }

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

        if (preserveScroll) {
            window.requestAnimationFrame(() => {
                window.scrollTo(prevScrollX, prevScrollY);
            });
        }
        return;
    }

    body.innerHTML = '';
    logProject.innerHTML = '';

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
        const isSelected = selectedProjects.has(p.name);
        const activeClass = (isExpanded || isSelected) ? 'project-row-active' : '';

        const actionButtons = p.status === 'running'
            ? `
                <div class="action-wrap">
                    <button class="btn-warning btn-mini btn-icon" onclick="stopCurrentTrain()" title="${t('stop_button')}">■</button>
                    <button class="btn-light btn-mini btn-icon" onclick="openProjectEditor('${encodeURIComponent(p.name)}')" title="${t('edit_button')}">✎</button>
                    <button class="btn-success btn-mini btn-icon" onclick="openProjectDataUpload('${encodeURIComponent(p.name)}')" title="${t('add_data_zip')}">＋</button>
                    <button class="btn-light btn-mini btn-icon" onclick="backupProject('${encodeURIComponent(p.name)}')" title="${t('backup_project')}">⇪</button>
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
                    <button class="btn-success btn-mini btn-icon" onclick="openProjectDataUpload('${encodeURIComponent(p.name)}')" title="${t('add_data_zip')}">＋</button>
                    <button class="btn-light btn-mini btn-icon" onclick="backupProject('${encodeURIComponent(p.name)}')" title="${t('backup_project')}">⇪</button>
                    <button class="btn-light btn-mini btn-icon" onclick="duplicateProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('duplicate_button')}">⧉</button>
                    <button class="btn-warning btn-mini btn-icon" onclick="clearDatasetPrompt('${encodeURIComponent(p.name)}')" title="${t('clear_dataset_button')}">🧹</button>
                    <button class="btn-danger btn-mini btn-icon" onclick="deleteProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('delete_button')}">🗑</button>
                </div>
            `
            : `
                <div class="action-wrap">
                    <button class="btn-primary btn-mini btn-icon" onclick="queueSingle('${encodeURIComponent(p.name)}')" title="${t('train_button')}">▶</button>
                    <button class="btn-light btn-mini btn-icon" onclick="openProjectEditor('${encodeURIComponent(p.name)}')" title="${t('edit_button')}">✎</button>
                    <button class="btn-success btn-mini btn-icon" onclick="openProjectDataUpload('${encodeURIComponent(p.name)}')" title="${t('add_data_zip')}">＋</button>
                    <button class="btn-light btn-mini btn-icon" onclick="backupProject('${encodeURIComponent(p.name)}')" title="${t('backup_project')}">⇪</button>
                    <button class="btn-light btn-mini btn-icon" onclick="duplicateProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('duplicate_button')}">⧉</button>
                    <button class="btn-warning btn-mini btn-icon" onclick="clearDatasetPrompt('${encodeURIComponent(p.name)}')" title="${t('clear_dataset_button')}">🧹</button>
                    <button class="btn-danger btn-mini btn-icon" onclick="deleteProjectPrompt('${encodeURIComponent(p.name)}')" title="${t('delete_button')}">🗑</button>
                </div>
            `;

        const tr = document.createElement('tr');
        tr.className = [runningClass, activeClass].filter(Boolean).join(' ');
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

    if (preserveScroll) {
        window.requestAnimationFrame(() => {
            window.scrollTo(prevScrollX, prevScrollY);
        });
    }
}

function renderMonitorState(s) {
    const progress = Number(s.progress || 0);
    document.getElementById('trainProgressText').textContent = `epoch ${s.epoch || 0}/${s.epochs || 0} | ${progress.toFixed(1)}%`;
    document.getElementById('trainProgressFill').style.width = `${progress}%`;

    document.getElementById('estimatedFinish').textContent = fmtEstimatedFinish(s.eta_sec);
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

    const hasExpandedDetails = expandedProjects.size > 0;
    await refreshData(stateData, { logData, preserveScroll: true, lightweight: hasExpandedDetails });
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

    try {
        const trimmedNewName = String(newName).trim();
        const data = await runProjectFsTask(
            project,
            '/api/project/rename',
            { project, new_name: trimmedNewName },
            t('rename_title'),
            trimmedNewName
        );

        projectDetailCache.clear();
        expandedProjects.delete(project);
        selectedProjects.delete(project);
        if (data.project && data.project !== project) {
            expandedProjects.delete(data.project);
            selectedProjects.delete(data.project);
        }
        alert(msg(data.message, 'OK'));
        refreshAll();
    } catch (e) {
        alert(String(e.message || e));
    }
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

    try {
        const trimmedNewName = String(newName || '').trim();
        const data = await runProjectFsTask(
            project,
            '/api/project/duplicate',
            { project, new_name: trimmedNewName },
            t('duplicate_title'),
            trimmedNewName
        );

        alert(msg(data.message, 'OK'));
        refreshAll();
    } catch (e) {
        alert(String(e.message || e));
    }
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

    const data = await runDatasetTask(
        project,
        '/api/project/clear_dataset',
        { project },
        t('clear_dataset_button')
    );

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

    try {
        const data = await runProjectFsTask(
            project,
            '/api/project/delete',
            { project },
            t('delete_project_title')
        );

        projectDetailCache.delete(project);
        expandedProjects.delete(project);
        selectedProjects.delete(project);
        alert(msg(data.message, 'OK'));
        refreshAll();
    } catch (e) {
        alert(String(e.message || e));
    }
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
    setUploadProgress(0, t('revalidate_starting'), `${project} | ${runFolder}`);
    try {
        const data = await apiGet('/api/project/revalidate_run', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ project, run_folder: runFolder })
        });

        const taskId = String(data.task_id || '');
        let finalMessage = data.message || t('revalidate_done');
        while (true) {
            const status = await apiGet('/api/project/revalidate_run/status');
            const pct = Math.max(0, Math.min(100, Number(status.progress) || 0));
            const detail = msg(status.detail || `${project} | ${runFolder}`);
            setUploadProgress(pct, t('revalidate_progress'), detail);

            if (String(status.id || '') !== taskId) {
                throw new Error('Re-validation task changed unexpectedly');
            }
            if (status.status === 'success') {
                finalMessage = status.message || finalMessage;
                setUploadProgress(100, t('revalidate_done'), detail);
                await sleep(350);
                break;
            }
            if (status.status === 'failed') {
                throw new Error(msg(status.message, 'Re-validation failed'));
            }
            await sleep(1200);
        }

        projectDetailCache.delete(project);
        await ensureProjectDetailLoaded(project);
        refreshData(latestStateData || undefined);
        hideUploadProgress();
        alert(msg(finalMessage, 'OK'));
    } catch (e) {
        hideUploadProgress();
        alert(msg(String(e), 'Re-validation failed'));
    }
}

async function runDatasetTask(project, url, body, labelText = '') {
    const fallbackDetail = labelText ? `${project} | ${labelText}` : project;
    setUploadProgress(0, t('dataset_task_starting'), fallbackDetail);
    try {
        const data = await apiGet(url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body || {})
        });

        const taskId = String(data.task_id || '');
        let finalMessage = data.message || t('dataset_task_done');
        while (true) {
            const status = await apiGet('/api/project/dataset_task/status');
            const pct = Math.max(0, Math.min(100, Number(status.progress) || 0));
            const detail = msg(status.detail || fallbackDetail);
            setUploadProgress(pct, t('dataset_task_progress'), detail);

            if (String(status.id || '') !== taskId) {
                throw new Error('Dataset task changed unexpectedly');
            }
            if (status.status === 'success') {
                finalMessage = status.message || finalMessage;
                setUploadProgress(100, t('dataset_task_done'), detail);
                await sleep(350);
                hideUploadProgress();
                return {
                    ...(status.result || {}),
                    message: finalMessage,
                };
            }
            if (status.status === 'failed') {
                throw new Error(msg(status.message, 'Dataset task failed'));
            }
            await sleep(800);
        }
    } catch (e) {
        hideUploadProgress();
        throw e;
    }
}

async function runProjectFsTask(project, url, body, labelText = '', targetName = '') {
    const fallbackDetail = [project, labelText, targetName].filter(Boolean).join(' | ');
    setUploadProgress(0, t('project_task_starting'), fallbackDetail);
    try {
        const data = await apiGet(url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body || {})
        });

        const taskId = String(data.task_id || '');
        let finalMessage = data.message || t('project_task_done');
        while (true) {
            const status = await apiGet('/api/project/fs_task/status');
            const pct = Math.max(0, Math.min(100, Number(status.progress) || 0));
            const detail = msg(status.detail || fallbackDetail);
            setUploadProgress(pct, t('project_task_progress'), detail);

            if (String(status.id || '') !== taskId) {
                throw new Error('Project task changed unexpectedly');
            }
            if (status.status === 'success') {
                finalMessage = status.message || finalMessage;
                setUploadProgress(100, t('project_task_done'), detail);
                await sleep(350);
                hideUploadProgress();
                return {
                    ...(status.result || {}),
                    message: finalMessage,
                };
            }
            if (status.status === 'failed') {
                throw new Error(msg(status.message, 'Project task failed'));
            }
            await sleep(800);
        }
    } catch (e) {
        hideUploadProgress();
        throw e;
    }
}

async function runModelTesting(encodedProject, encodedRunFolder) {
    const project = decodeURIComponent(encodedProject);
    const runFolder = decodeURIComponent(encodedRunFolder);
    setUploadProgress(0, t('model_testing_starting'), `${project} | ${runFolder}`);
    try {
        const data = await apiGet('/api/project/model_testing', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ project, run_folder: runFolder })
        });

        const taskId = String(data.task_id || '');
        let finalMessage = data.message || t('model_testing_done');
        while (true) {
            const status = await apiGet('/api/project/model_testing/status');
            const pct = Math.max(0, Math.min(100, Number(status.progress) || 0));
            const detail = msg(status.detail || `${project} | ${runFolder}`);
            setUploadProgress(pct, t('model_testing_progress'), detail);

            if (String(status.id || '') !== taskId) {
                throw new Error('Model testing task changed unexpectedly');
            }
            if (status.status === 'success') {
                finalMessage = status.message || finalMessage;
                setUploadProgress(100, t('model_testing_done'), detail);
                await sleep(350);
                break;
            }
            if (status.status === 'failed') {
                throw new Error(msg(status.message, 'Model testing failed'));
            }
            await sleep(1200);
        }

        projectDetailCache.delete(project);
        await ensureProjectDetailLoaded(project);
        refreshData(latestStateData || undefined);
        hideUploadProgress();
        alert(msg(finalMessage, 'OK'));
    } catch (e) {
        hideUploadProgress();
        alert(msg(String(e), 'Model testing failed'));
    }
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

function openProjectEditorAtImage(encodedName, encodedRel, encodedValidRel = '') {
    const projectName = decodeURIComponent(encodedName);
    const rel = decodeURIComponent(encodedRel);
    const validRel = decodeURIComponent(encodedValidRel || '');
    const url = `/project_editor?project=${encodeURIComponent(projectName)}&rel=${encodeURIComponent(rel)}${validRel ? `&valid_rel=${encodeURIComponent(validRel)}` : ''}`;
    window.open(url, '_blank');
}

function setUploadProgress(percent, text = 'Uploading...', detail = '') {
    const host = ensureModalHost();
    if (!host) return;

    const pct = Math.max(0, Math.min(100, Number(percent) || 0));
    const safeText = escapeHtml(text);
    const safeValue = `${pct.toFixed(0)}%`;
    const safeDetail = escapeHtml(detail || '');

    host.innerHTML = `
        <div class="web-modal-backdrop upload-progress-backdrop">
            <div class="web-modal upload-progress-modal" role="dialog" aria-modal="true" aria-label="${escapeAttr(text || 'Uploading')}">
                <div class="web-modal-head">
                    <div class="web-modal-title">${safeText}</div>
                </div>
                <div class="web-modal-body">
                    <div class="upload-progress-head">
                        <span>${safeText}</span>
                        <span>${safeValue}</span>
                    </div>
                    ${safeDetail ? `<div class="small mb8">${safeDetail}</div>` : ''}
                    <div class="upload-progress-bar">
                        <div class="upload-progress-fill" style="width:${pct}%"></div>
                    </div>
                </div>
            </div>
        </div>
    `;
    uploadProgressVisible = true;
}

function hideUploadProgress() {
    if (!uploadProgressVisible) return;
    const host = ensureModalHost();
    if (host) host.innerHTML = '';
    uploadProgressVisible = false;
}

function fmtEtaSeconds(sec) {
    const n = Number(sec);
    if (!Number.isFinite(n) || n < 0) return '-';
    const s = Math.floor(n % 60);
    const m = Math.floor((n / 60) % 60);
    const h = Math.floor(n / 3600);
    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
}

function fmtBytes(bytes) {
    const n = Number(bytes);
    if (!Number.isFinite(n) || n < 0) return '-';
    if (n < 1024) return `${n.toFixed(0)} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function buildBackupProgressDetail(status, fallbackText = '') {
    const parts = [];
    const project = String(status.project || '').trim();
    if (project) parts.push(project);

    const copied = Number(status.copied_bytes || 0);
    const total = Number(status.total_bytes || 0);
    if (total > 0) {
        parts.push(`${fmtBytes(copied)} / ${fmtBytes(total)}`);
    } else if (copied > 0) {
        parts.push(fmtBytes(copied));
    }

    if (status.eta_sec !== null && status.eta_sec !== undefined) {
        parts.push(`${t('backup_eta')}: ${fmtEtaSeconds(status.eta_sec)}`);
    }

    const targetPath = String(status.target_path || '').trim();
    if (targetPath) {
        parts.push(targetPath);
    }

    const message = msg(status.message || '');
    if (message && !parts.includes(message)) {
        parts.push(message);
    }

    return parts.length ? parts.join(' | ') : fallbackText;
}

async function backupProject(encodedName) {
    try {
        const project = decodeURIComponent(encodedName);
        const result = await runBackupTask(project, (status) => {
            const detailText = buildBackupProgressDetail(status, project);
            setUploadProgress(status.progress || 0, t('backup_progress'), detailText);
        });
        setUploadProgress(100, t('backup_done'), buildBackupProgressDetail(result, result.target_path || project));
        await sleep(500);
        hideUploadProgress();
        alert(msg(result.message, t('backup_done')));
        refreshAll();
    } catch (e) {
        hideUploadProgress();
        alert(String(e.message || e));
    }
}

async function runBackupTask(project, progressRenderer = null) {
    setUploadProgress(0, t('backup_starting'), project);

    const data = await apiGet('/api/project/backup', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ project })
    });

    const taskId = String(data.task_id || '');
    if (!taskId) {
        throw new Error(msg(data.message, 'Cannot start backup'));
    }

    while (true) {
        const status = await apiGet('/api/project/backup_status');
        if (String(status.id || '') !== taskId) continue;

        if (typeof progressRenderer === 'function') {
            progressRenderer(status);
        }

        if (status.status === 'success') {
            return status;
        }
        if (status.status === 'failed') {
            throw new Error(msg(status.message, 'Backup failed'));
        }
        await sleep(600);
    }
}

async function backupSelectedProjects() {
    const projects = Array.from(selectedProjects);
    if (projects.length === 0) {
        alert(t('selected_none'));
        return;
    }

    try {
        for (let i = 0; i < projects.length; i += 1) {
            const project = String(projects[i] || '');
            await runBackupTask(project, (status) => {
                const overall = ((i + ((Number(status.progress || 0)) / 100)) / projects.length) * 100;
                const detailText = `${project} (${i + 1}/${projects.length}) | ${buildBackupProgressDetail(status, project)}`;
                setUploadProgress(overall, t('backup_batch_progress'), detailText);
            });
        }
        setUploadProgress(100, t('backup_batch_done'), `${projects.length} project(s)`);
        await sleep(600);
        hideUploadProgress();
        alert(t('backup_batch_done'));
        refreshAll();
    } catch (e) {
        hideUploadProgress();
        alert(String(e.message || e));
    }
}

function uploadFileWithProgress(url, file, extraFields = {}, progressText = null) {
    return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        const fd = new FormData();
        fd.append('file', file);
        Object.entries(extraFields || {}).forEach(([key, value]) => {
            fd.append(key, value);
        });

        xhr.open('POST', url);
        xhr.responseType = 'json';
        xhr.withCredentials = true;

        xhr.upload.onprogress = (e) => {
            if (e.lengthComputable) {
                const pct = (e.loaded / e.total) * 100;
                setUploadProgress(pct, progressText || t('uploading'));
            } else {
                setUploadProgress(0, progressText || t('uploading'));
            }
        };

        xhr.upload.onload = () => {
            setUploadProgress(100, progressText || t('uploading'), t('server_processing'));
        };

        xhr.onload = () => {
            const resp = xhr.response || {};
            if (xhr.status >= 200 && xhr.status < 300) {
                resolve(resp);
                return;
            }
            reject(new Error(msg(resp.message, `HTTP ${xhr.status}`)));
        };

        xhr.onerror = () => reject(new Error('Network error while uploading'));
        xhr.onabort = () => reject(new Error('Upload aborted'));

        xhr.send(fd);
    });
}

async function handleProjectUploadInput(inputEl) {
    try {
        const file = inputEl && inputEl.files && inputEl.files[0];
        if (!file) return;

        setUploadProgress(0, t('uploading'));
        const data = await uploadFileWithProgress('/api/upload_project', file, {}, t('uploading'));

        alert(msg(data.message, 'Upload success'));
        await refreshAll();
    } catch (e) {
        alert(String(e.message || e));
    } finally {
        if (inputEl) inputEl.value = '';
        hideUploadProgress();
    }
}

function openProjectDataUpload(encodedName) {
    const input = document.getElementById('projectDataZipInput');
    if (!input) return;
    pendingProjectDataUpload = decodeURIComponent(encodedName);
    input.value = '';
    input.click();
}

async function handleProjectDataUploadInput(inputEl) {
    try {
        const file = inputEl && inputEl.files && inputEl.files[0];
        const project = String(pendingProjectDataUpload || '');
        if (!file || !project) return;

        setUploadProgress(0, t('uploading_data'));
        const data = await uploadFileWithProgress(
            '/api/project/import_data_zip',
            file,
            { project },
            t('uploading_data')
        );

        projectDetailCache.delete(project);
        expandedProjects.add(project);
        alert(msg(data.message, 'Data imported'));
        await ensureProjectDetailLoaded(project);
        await refreshAll();
    } catch (e) {
        alert(String(e.message || e));
    } finally {
        pendingProjectDataUpload = '';
        if (inputEl) inputEl.value = '';
        hideUploadProgress();
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

async function maybePromptQueueSessionRecovery() {
    if (queueRecoveryPromptShown) return;
    queueRecoveryPromptShown = true;

    try {
        const data = await apiGet('/api/queue_session/status');
        if (!data.ok || !data.pending) return;

        const projects = Array.isArray(data.projects) ? data.projects.filter(Boolean) : [];
        const projectText = projects.length ? `:\n- ${projects.join('\n- ')}` : '';
        const message = t('continue_last_session_message').replace('$PROJECTS$', projectText);

        const shouldContinue = await showConfirmDialog({
            title: t('continue_last_session_title'),
            message,
            confirmText: t('continue_last_session_confirm'),
            cancelText: t('continue_last_session_ignore'),
            confirmClass: 'btn-primary'
        });

        if (shouldContinue) {
            const result = await apiGet('/api/queue_session/continue', { method: 'POST' });
            alert(msg(result.message, t('continue_last_session_done')));
            await refreshAll();
            return;
        }

        const ignored = await apiGet('/api/queue_session/ignore', { method: 'POST' });
        alert(msg(ignored.message, t('continue_last_session_ignored')));
    } catch (e) {
        console.error(e);
    }
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
applyToolbarIcons();
applySidePanelState();
updateStickyLayoutMetrics();
window.addEventListener('resize', updateStickyLayoutMetrics);
if (typeof ResizeObserver !== 'undefined') {
    const stickyPanel = document.querySelector('.sticky-top-panel');
    if (stickyPanel) {
        new ResizeObserver(() => updateStickyLayoutMetrics()).observe(stickyPanel);
    }
    const projectBadges = document.getElementById('projectSummaryBadges');
    if (projectBadges) {
        new ResizeObserver(() => updateStickyLayoutMetrics()).observe(projectBadges);
    }
}
refreshAll().finally(() => {
    updateStickyLayoutMetrics();
    maybePromptQueueSessionRecovery().finally(() => {
        watchStateChanges();
    });
});

function showMsaSampleUpload(encodedProject, encodedRunFolder) {
    const projectName = decodeURIComponent(encodedProject);
    const runFolder = decodeURIComponent(encodedRunFolder);

    const host = ensureModalHost();
    if (!host) return;

    host.innerHTML = `
        <div class="web-modal-backdrop">
            <div class="web-modal" role="dialog" aria-modal="true" aria-label="MSA Sample Testing">
                <div class="web-modal-head">
                    <div class="web-modal-title">MSA Sample Testing</div>
                    <button class="web-modal-close" onclick="closeMsaSampleUpload()" aria-label="Close">×</button>
                </div>
                <div class="web-modal-body">
                    <div class="small mb8">Upload a ZIP file containing sample images (.jpg/.png) and their labels (.txt) in YOLO format.</div>
                    <div class="upload-area" id="msaUploadArea">
                        <p>Click to select ZIP file or drag & drop</p>
                        <input type="file" id="msaFileInput" accept=".zip" style="display:none;" onchange="handleMsaFileSelect(event, '${encodeURIComponent(projectName)}', '${encodeURIComponent(runFolder)}')">
                    </div>
                    <div id="msaUploadStatus" style="margin-top:12px;"></div>
                </div>
            </div>
        </div>
    `;

    const uploadArea = document.getElementById('msaUploadArea');
    const fileInput = document.getElementById('msaFileInput');

    if (uploadArea && fileInput) {
        uploadArea.addEventListener('click', () => fileInput.click());
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.style.backgroundColor = '#e8f5e9';
        });
        uploadArea.addEventListener('dragleave', (e) => {
            e.preventDefault();
            uploadArea.style.backgroundColor = '';
        });
        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadArea.style.backgroundColor = '';
            const files = e.dataTransfer.files;
            if (files && files[0]) {
                fileInput.files = files;
                handleMsaFileSelect({ target: fileInput }, encodedProject, encodedRunFolder);
            }
        });
    }
}

function closeMsaSampleUpload() {
    const host = ensureModalHost();
    if (host) host.innerHTML = '';
}

async function handleMsaFileSelect(event, encodedProject, encodedRunFolder) {
    const projectName = decodeURIComponent(encodedProject);
    const runFolder = decodeURIComponent(encodedRunFolder);
    const fileInput = event.target;
    const file = fileInput.files && fileInput.files[0];

    if (!file) return;

    if (!file.name.toLowerCase().endsWith('.zip')) {
        alert('Please select a ZIP file');
        return;
    }

    try {
        const statusDiv = document.getElementById('msaUploadStatus');
        if (statusDiv) {
            statusDiv.innerHTML = '<div class="small">Uploading and processing...</div>';
        }

        const data = await uploadFileWithProgress(
            '/api/project/model_testing_msa',
            file,
            { project: projectName, run_folder: runFolder },
            'Processing MSA Sample'
        );

        if (!data.ok) {
            if (statusDiv) {
                statusDiv.innerHTML = `<div class="error-msg">${escapeHtml(data.message || 'Upload failed')}</div>`;
            }
            alert(data.message || 'MSA upload failed');
            return;
        }

        if (statusDiv) {
            const msa = data.msa_summary || {};
            const results = msa.results || {};
            const errorRate = Number(results.error_rate || 0).toFixed(2);
            const html = `
                <div class="success-msg mb8">MSA Sample Testing Completed!</div>
                <div class="detail-stats">
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">Total Images</div>
                        <div class="detail-stat-value">${results.total_images || 0}</div>
                    </div>
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">Wrong Predictions</div>
                        <div class="detail-stat-value">${results.wrong_images || 0}</div>
                    </div>
                    <div class="detail-stat-item">
                        <div class="detail-stat-label">Error Rate</div>
                        <div class="detail-stat-value">${errorRate}%</div>
                    </div>
                </div>
                <button class="btn-primary" onclick="refreshProjectDetail('${encodeURIComponent(projectName)}'); closeMsaSampleUpload();">View Results</button>
            `;
            statusDiv.innerHTML = html;
        }

        setTimeout(() => {
            refreshProjectDetail(projectName);
            closeMsaSampleUpload();
        }, 2000);

    } catch (e) {
        const statusDiv = document.getElementById('msaUploadStatus');
        if (statusDiv) {
            statusDiv.innerHTML = `<div class="error-msg">${escapeHtml(String(e.message || e))}</div>`;
        }
        alert(String(e.message || e));
    }
}

function refreshProjectDetail(projectName) {
    projectDetailCache.delete(projectName);
    refreshData(latestStateData || undefined);
}

