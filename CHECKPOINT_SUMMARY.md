# Checkpoint Summary

Last updated: 2026-06-24

## Project overview

This repository is a Flask-based AI training control system for managing object detection projects.

Key components:
- `TrainControl.py`: single main application entrypoint with server startup, runtime state, queue management, project and dataset APIs, model testing, export/downloads, backup orchestration, and audit/history logging.
- `tc_config.py`: environment-backed configuration values, root and backup paths, waitresses settings, auth defaults, and file names.
- `tc_runtime.py`: shared runtime state values, thread-safe locks/condition, and the training queue.
- `tc_persistence.py`: persistence helpers for user credentials, audit logs, and training history.
- `tc_backup.py`: backup task implementation with safe path resolution, progress updates, ETA estimation, and async backup thread.
- `tc_auth.py`: session auth helpers, LV2 verification, and auth state management.
- `tc_dataset.py`: project dataset helpers, zip upload/import, dataset config validation, safe naming, and path security.
- `tc_notify.py`: notification state and async Telegram send helper.
- `tc_routes_basic.py`: basic public routes for login/logout, health checks, main page, and project editor page.
- `Train_model_AI.py`: YOLO training service with FastAPI monitoring endpoints, training state, and log streaming support.
- `notify.py`: Telegram notifier implementation used by `tc_notify.py`.

## Runtime features and behavior

- Queue session state persists to `queue_session.json` and can recover between restarts.
- Recover stale running jobs at startup using monitor status, PID checks, and output artifacts detection.
- Prevent concurrent training by queueing the next project when a train is already running.
- Support ZIP project upload with safe project naming and legacy label layout compatibility.
- Background tasks for dataset operations, project filesystem operations, backups, and model testing.
- Built-in project/image path resolution and validation to protect against path traversal.
- YOLO label parsing and class extraction utilities with support for `image/`, `images/`, and legacy `labels` layout.
- Bbox anomaly analysis with sensitivity levels and normalized class grouping for better label diagnostics.

## API and UI capabilities

- Authentication: login/logout, session expiry, LV2 protected JSON actions.
- Health and runtime state endpoints for worker, queue, project count, and monitor state.
- Project management: scan, upload, rename, duplicate, delete, backup.
- Dataset management: clear project dataset folders, import data zip, save dataset configuration, create dataset splits, merge train/valid.
- Model testing: start, monitor status, and results export.
- Train monitor integration: `/api/train_monitor/status` and `/api/train_monitor/history`.
- Project detail endpoints, image listing/detail, label fetch/save, and output browsing.
- Download endpoints for model weights and success-output bundles.
- Notification endpoints for Telegram state and toggling.

## Validation results

- All Python source files were successfully compiled with `py_compile`.
- No syntax errors were detected in the repository modules.

## Checked repository files

- `TrainControl.py`
- `tc_config.py`
- `tc_runtime.py`
- `tc_persistence.py`
- `tc_backup.py`
- `tc_auth.py`
- `tc_dataset.py`
- `tc_notify.py`
- `tc_routes_basic.py`
- `Train_model_AI.py`
- `notify.py`

## Important runtime files

- `queue_session.json`: saved queue/resume state.
- `train_history.jsonl`: persisted training history.
- `audit_log.jsonl`: audit log persistence.
- `user.json`: auth credentials file.
- `dataset_config.json`: shared dataset configuration.

## Known refactor opportunities

- `TrainControl.py` remains large and should continue to be split into smaller modules:
  - project route handlers
  - train runtime/worker logic
  - dataset/export/analysis routes
- UI update logic is improved but still tightly coupled to current backend endpoints.

## Resume guidance for next chat

1. Use `CHECKPOINT_SUMMARY.md` as the current handoff file.
2. The main application entrypoint is still `TrainControl.py`.
3. Queue session recovery, stale-running recovery, dataset/task background flows, and model testing are already implemented.
4. Update this checkpoint after any meaningful backend or frontend change.
