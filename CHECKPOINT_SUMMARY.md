# Checkpoint Summary

Last updated: 2026-10-02

Workspace: `D:\TrainControl`

## Purpose

This repository is a Flask/Waitress-based AI training control system for object detection projects. The user is actively improving the web UI, especially the Label Editor and project training table. If this file is read in a new chat, continue from the current codebase in `D:\TrainControl` and inspect existing files before editing.

## Main Files

- `TrainControl.py`: main Flask app, runtime state, queue management, project APIs, dataset APIs, Label Editor APIs, prediction/import/export, backup, user activity audit, restart server, HTTPS startup.
- `UI/index.html`: main train monitor page, toolbar, project table, modals.
- `UI/app.js`: main frontend logic for train monitor, table rendering, toolbar actions, account/log modals, queue/session recovery, project notes.
- `UI/style.css`: main UI styling, project table layout, action dropdown, note highlight, toolbar, modals.
- `UI/project_editor.html`: Label Editor frontend.
- `tc_config.py`: env-backed config, ports, passwords, HTTPS settings, file names, root paths.
- `tc_runtime.py`: shared `STATE`, locks, condition variable, train queue.
- `tc_persistence.py`: user account persistence, credentials, audit log, training history.
- `tc_auth.py`: auth/session helpers including LV2/LV3 state.
- `tc_routes_basic.py`: login/logout/root/project editor routes.
- `tc_dataset.py`: dataset import/clear helpers, robust delete, safe paths.
- `Train_model_AI.py`: YOLO training service with monitor endpoints.

## Runtime Files

- `queue_session.json`: queue/current project recovery across restarts.
- `project_notes.json`: per-project notes shown in the main project table.
- `audit_log.jsonl`: activity log with user, session id, IP, action, status, time.
- `train_history.jsonl`: persisted training history.
- `dataset_config.json`: shared dataset config.
- `user.json` / `user.js`: account credentials, depending on current persistence compatibility.

Do not treat runtime files as source changes unless the user explicitly asks.

## Current UI Behavior

Main page:
- Toolbar has compact action buttons including account manager, user log, restart server, selected best.pt download, backup, train, clear dataset, notification.
- Per-project row actions are in a `...` dropdown to keep the table compact.
- Per-project action dropdown includes `Quick Train`. It confirms first, requires LV2, clears the current dataset, creates a dataset with `train_all_data: true` using 80/20/0 split so all images go to train while validation remains available, then queues the project for training. If any step fails, it stops and shows the error.
- The old toolbar `Train all` button was repurposed as `Quick Train Selected Project`. It runs Quick Train sequentially for checked projects, continues past individual project failures, and shows a final OK/error summary.
- The old duplicate `Edit` / `Label edit` actions were consolidated.
- `Download success output` was changed to download selected projects' latest `best.pt`.
- Project table has a `Note` column. Notes use an auto-growing textarea so long notes expand the row and remain fully visible. Press `Enter` in the note field to save. The `x` button clears the note.
- Project table order is `Action` before `Note`, so row actions sit on the left side of the note input. The `Action` column is intentionally narrow because it only contains the popup `...` toggle.
- Projects with non-empty note are highlighted with a subtle yellow background and left accent.
- The project table uses lightweight row refresh when the visible row structure has not changed, reducing flicker, preserving focused note inputs, and making multi-client usage smoother.
- Project row expand opens immediately with a loading detail row, then updates that row when `/api/project_detail` finishes. It no longer waits for detail data before showing the expanded row.
- Expanded project detail tabs (`Dataset`, `Output`, `Metrics`, `Model Testing`, `Labels`) update only the expanded detail row via `updateProjectDetailRow(projectName)`, not the whole project table. Metrics/testing sub-controls such as run expand and confusion filters use the same row-only update.
- Data-changing actions refresh project detail automatically. Clear dataset now keeps/reopens the project detail row, reloads `/api/project_detail`, and updates counts without requiring a manual page refresh. Clear selected dataset reloads detail for selected projects that are currently expanded.
- Long-poll `/api/state/changes` no longer requests `with_snapshot=1` on every loop; when state changes, the frontend fetches lightweight `/api/state` and patches rows. This prevents heavy snapshot polling from blocking expand/click interactions.
- Performance update: `/api/state/changes` now supports `with_state=1`, so the long-poll response can include the lightweight state payload directly. `UI/app.js` uses this to avoid an extra `/api/state` request on every state change.
- Performance update: lightweight table refresh no longer rebuilds the project log dropdown on every progress tick; it rebuilds only when the project-name list changes.
- Frontend state polling pauses while the user is actively interacting: action dropdown, note editing, modal dialogs, progress overlays, queue actions, and project expand. Polling resumes when the action finishes, or automatically after the 20-second pause timeout.

Label Editor:
- UI was simplified for faster labeling.
- Image list panel is compact and focused on BBOX count, ABN, and Class filters.
- Class list and raw bbox list panes were removed where not needed.
- Class filter supports dropdown/tick style multi-select.
- Floating toolbar on the left can collapse/expand.
- Removed unused controls like frame width slider and redundant hide/select/delete buttons.
- Bbox labels are colored by class suffix: `_NG` red, `_OK` green.
- Space toggles bbox visibility.
- Mouse wheel zoom works when cursor is inside the label/image area.
- Ctrl+A selects all bboxes and allows moving them together.
- A selected bbox has eight resize handles: four corners plus the midpoint of each edge. Edge handles resize only along that edge's axis.
- Each bbox label/action has delete and duplicate actions near the class name.
- Duplicate bbox cleanup/action support was added.
- Image delete exists in the Label Editor: `X` before image or delete current image removes image plus matching label file.
- Quick actions include Add Data and Quick Predict from the Label Editor floating toolbar.
- Switching images auto-saves labels when needed.

## Backend Behavior Added Recently

Auth/account/log:
- Account manager supports creating and deleting accounts.
- Activity log records current username, session id, IP, timestamp, action, project/target, and status.
- Activity log also records Label Editor activity with explicit `label_editor_*` actions: open editor, list images, load classes, view image, image info, delete image, load label, and save label. These entries include project, image rel target, bbox/anomaly counts where relevant, and success/failed status.
- Activity log page/button is protected by LV3 password.
- LV3 password is `060400`.
- Admin login password was reset to `admin` when requested.

Server/restart/HTTPS:
- Optional HTTPS startup exists.
- Waitress/Flask can run on all addresses; DDNS access depends on router/firewall/cert/browser trust.
- `start_waitress.cmd` now starts Waitress with explicit tuning: 48 threads, connection limit 1000, channel timeout 120s, cleanup interval 30s. This avoids Waitress default 4-thread starvation when long-poll requests are open.
- Restart server button triggers restart flow.
- `restart_waitress.cmd` launches `start_waitress.cmd` after a short delay.
- Earlier restart issues included bad cwd/path and Windows access prompts; keep restart commands simple and run from `D:\TrainControl`.

Training/queue:
- Queue session recovery supports continuing last session after restart.
- A fix was added so if the script restarts while a train is active/done, status recovery should not leave the project stuck as `running`.
- 2026-09-20 follow-up: external train launcher no longer pauses on failure. PID checks for completion/restart recovery now distinguish the actual Python train process from an idle `.cmd` launcher. If monitor never starts and no Python train process exists after 20 seconds, the wait returns failure rather than hanging forever; if the monitor disappears after training was observed, it returns failure (not an unverified success). Restart the web server to load this backend fix.
- Restart recovery now starts a watcher for the restored `current` project. The watcher uses `wait_train_completion_from_monitor(..., assume_started=True)`, finalizes the recovered project, clears `STATE["current"]`, syncs `queue_session.json`, and starts the worker so queued projects continue.
- `wait_train_completion_from_monitor()` now treats `assume_started=True` as already having seen training, and it can finish when monitor reports idle even if the project name is missing/not matching, as long as no train PID remains for that project.
- `start_worker_if_needed()` also checks whether the worker thread is actually alive, not only `STATE["worker_running"]`, to avoid stale worker flags after errors/restarts.
- Monitor loop starts the worker if it clears stale `current` while queue items remain.
- Training order supports priority changes in queue.

Dataset/project:
- Clear dataset handles Windows access-denied cases more robustly. Dataset folders are first renamed into `.tc_delete_pending` inside the project, then deleted. If Windows still holds file handles, the visible `train/test/valid/runs` folders disappear immediately and pending cleanup is retried on later clear operations.
- Project/data upload supports zip and image imports.
- Quick Predict predicts unlabeled images using the latest `best.pt`.
- MSA Sample Test ZIP no longer requires a project `data.yaml`; it uses class names embedded in the selected weights, falling back to the project class list. Re-validation remains dataset-based and still requires `data.yaml`. MSA prediction labels/images are read from the actual Ultralytics `msa_predict` output directory (the previous path had an extra nested `msa_predict`).
- New MSA workflow: Model Testing tab opens `/msa_testing?project=...` in a separate page. Opening the page creates `<project>/MSA/images`, `labels`, and `results`. The page imports a ZIP of paired images/YOLO labels, lists `best.pt`/`last.pt` across project output runs, starts a background MSA test for the selected weight, and compares GT/prediction images side by side. Each run is stored separately under `MSA/results/<unique-id>` with `results.json`, GT overlays, and prediction images/labels. Comparison uses IoU >= 0.5 with class matching; summary lists wrong images/total, percentage, and per-class missed/extra boxes. New APIs: `/api/project/msa`, `/api/project/msa/upload`, `/api/project/msa/run`, `/api/project/msa/status`, `/api/project/msa/result`, `/api/project/msa/file`. Existing ZIP-modal MSA endpoint remains for compatibility but is no longer linked from the main UI.
- 2026-09-20 MSA upload fix: Flask/Werkzeug's uploaded `SpooledTemporaryFile` is not compatible with `zipfile.ZipFile(upload_file.stream)` in this environment. Save the upload to a temporary ZIP path first, then open `ZipFile(path)`. A real Flask test-client upload regression test was added to `test_msa.py`. The MSA page now reports HTTP errors instead of showing `Unexpected token '<'` if the server returns HTML.
- 2026-09-20 MSA progress: ZIP upload now shows actual browser byte progress, then imports/extracts asynchronously with server-reported percent and file/sample counts. MSA prediction streams Ultralytics results to report per-image progress, followed by label comparison progress. `/api/project/msa/status` returns both run and upload task state; the page resumes progress display after refresh. Background MSA workers receive captured username/session/IP for audit logging rather than trying to access Flask request/session outside a request context.
- 2026-09-20 MSA Label Editor integration: the left comparison pane now embeds `UI/project_editor.html` in MSA mode (`msa=1&embed=1`), reusing its canvas, bbox move/resize/add/delete/duplicate, class selection, zoom, and save/autosave-on-image-switch behavior. Embedded styling hides dataset-only controls/sidebar. New isolated APIs `/api/project/msa/images` and `/api/project/msa/label` read/write `<project>/MSA/images` and `<project>/MSA/labels` only; image bytes use `/api/project/msa/file`. The MSA page switches editor images via same-origin `postMessage`, and warns that a saved label requires rerunning MSA to refresh comparison statistics. `test_msa.py` covers the isolated label API and path traversal rejection.
- 2026-09-20 MSA predict visualization: the right pane now draws original images plus prediction labels on a canvas. A persistent 10-30 px font-size slider redraws labels immediately. Per-image `/api/project/msa/sample` classifies prediction boxes with the same IoU>=0.5 matching as the MSA error summary: green = correct, red = wrong class/extra, dashed amber = missed GT box. Label save in the left embedded editor refreshes the right overlay for the selected image; aggregate results still require rerunning MSA. New predictions save confidence in YOLO `.txt` so the overlay can show percentages; old runs still render without confidence. `test_msa.py` covers the sample endpoint and correct/wrong matching.
- 2026-09-20 MSA auto-labeling: ZIP import now accepts image-only archives and preserves existing labels when a ZIP supplies only images. The MSA page has `Predict label gốc`, using the selected project weight to create YOLO `.txt` only for images missing labels; it never overwrites an existing label, including labels created concurrently during prediction. An empty prediction creates an empty label file. The task reports per-image progress through `/api/project/msa/status` (`label_task`), refreshes the embedded editor after completion, and warns that model-generated labels need human review. `Chạy MSA` is disabled/rejected until every MSA image has a label, avoiding partial evaluation. `test_msa.py` covers image-only import, no-overwrite behavior, MSA guard, and async API progress.
- 2026-09-20 MSA auto-label name fix: Ultralytics renames images supplied as a list of file paths to `image0.jpg`, etc., causing `Unexpected prediction image: image0.jpg`. `predict_missing_msa_labels()` now stages only unlabeled images in a temporary directory inside the project MSA folder (hard links with copy fallback) and predicts from the directory, preserving original filenames. The temporary directory is removed after prediction. The fake model in `test_msa.py` reproduces the list-input rename behavior, and all tests pass with the directory-input fix.
- 2026-09-20 Model Testing tab cleanup: removed legacy per-run `Model Testing | Model_Train...` result cards and Run Testing buttons from project details. The tab now only opens the dedicated MSA Model Testing page. Legacy APIs and saved results remain untouched.
- 2026-09-20 MSA page redesign: `UI/msa_testing.html` now has a compact project header, grouped model/result controls and actions, a horizontal metric strip, a searchable image list with color-coded correct/wrong/missing-label states, larger side-by-side comparison panes, a clear empty prediction state, and responsive stacking on narrower screens. Existing MSA APIs and workflows are unchanged.
- 2026-09-23 false training failure fix: training history showed each affected project recorded as `success`/RC 0 and then overwritten 3-6 seconds later as `failed`/RC 1. `wait_train_completion_from_monitor()` now ignores a stale failure message from the previous run until the current run has actually been observed training, resets stale failure state on that transition, and treats an explicit `completed`/`done`/`finished` message as success. The background stale-project reconciler also skips the project currently owned by `run_project` or the recovered watcher, preventing two threads from finalizing the same run. `test_train_completion.py` covers stale failure followed by success and a genuine current-run failure.
- 2026-10-02 HTTPS startup fix: `start_waitress.cmd`, `start_https.cmd`, and the Restart Server flow now consistently start HTTPS on port 820 instead of restarting into HTTP Waitress. When no explicit cert/key is configured, `build_https_ssl_context()` creates a persistent internal CA plus a one-year server certificate signed by that CA. The server SAN covers localhost, local IPv4 addresses, machine hostname, and `TRAIN_CONTROL_HTTPS_HOSTNAMES` (default `aisystemmyhome.ddns.net`). `install_https_ca.cmd` trusts only `traincontrol-ca.cer` for the current Windows user; `remove_https_ca.cmd` removes that trust. Generated certificates and private keys are git-ignored. A trusted public DDNS connection still requires TCP 820 router/firewall forwarding and trusting this CA on each client, or configuring a publicly issued cert/key through the environment variables.
- 2026-10-02 external HTTPS diagnosis: the server listens on `0.0.0.0:820` and DDNS resolves to `116.105.23.102`, but Windows Firewall has no `TrainControl HTTPS 820` inbound rule. The host also has two static LAN addresses (`192.168.1.19` and `192.168.1.20`) on separate adapters in the same subnet with equal default-route metrics. Router TCP 820 forwarding must target the intended stable address (currently use `192.168.1.20`) and the narrowly scoped Private-profile firewall rule still needs explicit user approval before installation.
- 2026-10-02 Caddy public HTTPS trial: installed checksum-verified Caddy v2.11.6 under `tools/caddy`, added `Caddyfile`, opened a Private-profile Windows Firewall rule scoped to `caddy.exe` on TCP 80/443, and moved Waitress to the loopback-only `127.0.0.1:821` backend with `ProxyFix`/secure proxy cookies enabled. Caddy storage is kept in `.caddy-data` and proxy environment variables are cleared before startup so ACME can connect directly. Let's Encrypt account registration succeeded, but both HTTP-01 on port 80 and TLS-ALPN-01 on port 443 timed out against public IP `116.105.23.102`; the router must forward WAN TCP 80 -> `192.168.1.20:80` and WAN TCP 443 -> `192.168.1.20:443`. Automatic public certificate requests are temporarily disabled with `auto_https disable_certs` to avoid failed-validation rate limits; after forwarding is confirmed, change this to `auto_https ignore_loaded_certs` and reload Caddy. A fallback Caddy listener on `https://aisystemmyhome.ddns.net:820` uses the existing internal CA certificate and proxies to the same backend, preserving the current route locally; adding TCP 820 to the Caddy firewall rule still requires explicit approval if that fallback must remain reachable externally.
- 2026-10-02 HTTP rollback: HTTPS/Caddy startup is disabled at the user's request. `start_waitress.cmd` now stops any running project Caddy instance and serves TrainControl directly on `0.0.0.0:820` over HTTP; `start_https.cmd` is retained only as a compatibility launcher and delegates to `start_waitress.cmd`. The active URLs are `http://127.0.0.1:820` locally and `http://<LAN-IP>:820` on the LAN. Existing certificate/Caddy files remain dormant so no unrelated setup is destructively removed.
- Quick Train is currently frontend orchestration in `UI/app.js` using existing APIs: `/api/project/clear_dataset`, `/api/project/create_dataset`, and `/api/queue`. The helper `runQuickTrainSteps(project)` is shared by single-project and selected-project Quick Train.
- Selected best.pt download bundles latest `best.pt` for selected projects.

Project notes:
- Backend endpoint: `POST /api/project/note`.
- Notes are saved in `project_notes.json`.
- A dedicated `project_notes_lock` avoids holding global `state_lock` during note file IO.
- Note file writes are atomic via tmp file + `os.replace`.
- `/api/state`, `/api/snapshot`, and `/api/log` now build payloads under lock but call `jsonify` after releasing the lock.

## Important Implementation Details

- `STATE["projects"]` is runtime state. Use `ensure_project_state()` to initialize per-project fields.
- `project_state_public()` controls fields sent to frontend.
- `scan_projects()` should not overwrite in-memory notes for already-known projects; it only loads note from file for newly discovered projects.
- `build_state_payload_locked()` creates `/api/state` payload.
- Long-poll endpoint `/api/state/changes` drives live updates. Keep it lightweight.
- Frontend `refreshData()` is the main table render path.
- Frontend `shouldUseLightweightProjectRefresh()` decides whether rows can be patched without full table render.
- If adding/removing a project table column, update:
  - `UI/index.html` sticky header
  - `UI/index.html` table `<colgroup>` and `<thead>`
  - `UI/app.js` full row render
  - `UI/app.js` lightweight cell index updates
  - `UI/style.css` column width variables and grid template
  - any `colspan`

## Validation

Latest successful check:

```powershell
python -m py_compile TrainControl.py tc_config.py tc_auth.py tc_persistence.py tc_routes_basic.py tc_dataset.py
```

Result: passed.

Latest restart fix validation: same `py_compile` command passed after updating recovered-current watcher and worker restart logic.

Latest project table validation: same `py_compile` command passed after moving `Action` before `Note`, making same-row refresh lightweight so note editing does not lose focus, narrowing the popup-only `Action` column, and changing note fields to auto-grow for long text.

Latest activity-log validation: same `py_compile` command passed after adding `label_editor_*` audit entries for Label Editor page/API actions.

Latest clear-dataset validation: same `py_compile` command passed after changing dataset deletion to best-effort rename/delete with `.tc_delete_pending`; a temporary local test folder was successfully cleared.

Latest expand-performance validation: same `py_compile` command passed after changing project expand to immediate row insertion and making state long-poll lightweight.

Latest polling UX validation: same `py_compile` command passed after adding frontend pause/resume state polling during user actions with a 20-second timeout.

Latest project-detail tab validation: same `py_compile` command passed after fixing detail tabs and metrics/testing controls to update the existing detail row instead of calling full table refresh.

Latest data-refresh validation: same `py_compile` command passed after making clear dataset and clear selected dataset refresh project detail/counts automatically without manual page refresh.

Latest Quick Train validation: same `py_compile` command passed after adding per-project Quick Train and toolbar Quick Train Selected Project. Node.js is not installed, so JS syntax check with `node --check UI\app.js` was skipped and the changed JS was inspected manually.

Latest performance validation: `python -m py_compile TrainControl.py tc_config.py tc_auth.py tc_persistence.py tc_routes_basic.py tc_dataset.py` passed, `node --check UI\app.js` passed, and `waitress-serve --help` confirmed the tuning flags used by `start_waitress.cmd`.

Node.js is installed at `C:\Program Files\nodejs\node.exe` (`v24.19.0`) with npm `11.17.0`. Existing terminals may need to be reopened before plain `node` is on PATH.

## Current Worktree Notes

The worktree may contain many modified runtime files and generated files, including `__pycache__`, `audit_log.jsonl`, `queue_session.json`, `train_history.jsonl`, and config/user data. Do not revert user/runtime changes unless explicitly requested.

Use `git status --short` before edits. Use `rg` for search. Use `apply_patch` for manual edits.

## Common User Preferences

- User prefers Vietnamese responses.
- User wants compact, direct UI with minimal clutter.
- Avoid big panels/cards when a dropdown or floating compact control works.
- Keep Label Editor fast and simple for repetitive labeling work.
- Prefer direct implementation over long proposals.
- When updating UI, verify no flicker, no overlapping text, and no unnecessary refresh.

## Resume Checklist For New Chat

1. Confirm current folder is `D:\TrainControl`.
2. Read this checkpoint first.
3. Inspect the relevant files before editing; do not assume current code from memory.
4. Check `git status --short` and avoid reverting unrelated changes.
5. For backend changes, run `python -m py_compile TrainControl.py tc_config.py tc_auth.py tc_persistence.py tc_routes_basic.py tc_dataset.py`.
6. For frontend changes, Node may be unavailable, so inspect changed JS carefully.
7. Update this checkpoint after meaningful changes.
