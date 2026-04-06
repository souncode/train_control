# -*- coding: utf-8 -*-
import json
import yaml
import cv2
import time
import queue
import threading
import traceback
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Union

import torch
from ultralytics import YOLO, __version__ as ULTRA_VERSION

# ---------- Web server (FastAPI + SSE) ----------
# pip install fastapi uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import uvicorn

LOG_QUEUE: "queue.Queue[dict]" = queue.Queue(maxsize=10000)
LOG_HISTORY: list[dict] = []
LOG_HISTORY_MAX = 5000
LOG_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()

TRAIN_STATE: Dict[str, Any] = {
    "is_training": False,
    "epoch": 0,
    "epochs": 0,
    "progress": 0.0,
    "project_name": None,
    "project_dir": None,

    # loss/opt/speed/time
    "lr": None,
    "loss": None,
    "loss_box": None,
    "loss_cls": None,
    "loss_dfl": None,
    "ips": None,
    "epoch_time": None,
    "avg_epoch_time": None,
    "elapsed_sec": 0.0,
    "eta_sec": None,

    # metrics
    "precision": None,
    "recall": None,
    "map50": None,
    "map5095": None,
    "best_map5095": None,

    # run & data
    "save_dir": None,
    "weights": None,
    "run_name": None,
    "started_at": None,
    "batch_size": None,
    "img_size": None,

    # environment & dataset & hparams
    "env": {},
    "dataset": {},
    "hparams": {},
    "msg": "idle",
}


def _append_history(rec: dict):
    with LOG_LOCK:
        LOG_HISTORY.append(rec)
        if len(LOG_HISTORY) > LOG_HISTORY_MAX:
            del LOG_HISTORY[: len(LOG_HISTORY) - LOG_HISTORY_MAX]


def _jsonable(x):
    if isinstance(x, dict):
        return {str(k): _jsonable(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_jsonable(v) for v in x]
    if isinstance(x, tuple):
        return [_jsonable(v) for v in x]
    if isinstance(x, np.generic):
        return x.item()
    if isinstance(x, Path):
        return str(x)
    return x


def get_state_snapshot() -> Dict[str, Any]:
    with STATE_LOCK:
        return _jsonable(dict(TRAIN_STATE))


def update_state(**kwargs):
    with STATE_LOCK:
        TRAIN_STATE.update(kwargs)


def push_log(msg: str, kind: str = "info", extra: Dict[str, Any] | None = None):
    rec = {"t": time.strftime("%Y-%m-%d %H:%M:%S"), "kind": kind, "msg": msg}
    if extra:
        rec.update(extra)
    try:
        LOG_QUEUE.put_nowait(rec)
    except queue.Full:
        pass
    _append_history(rec)
    print(f"[{rec['t']}] {msg}")


def start_web_server(host: str = "0.0.0.0", port: int = 8008):
    app = FastAPI()

    @app.get("/", response_class=HTMLResponse)
    def home():
        return """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>AI Training Console</title>
<style>
:root{--bg:#0b0f16;--panel:#111827;--muted:#9ca3af;--ok:#34d399;--warn:#f59e0b;--err:#f87171;--chip:#1f2937}
*{box-sizing:border-box} body{font-family:ui-monospace,Menlo,Consolas,monospace;background:var(--bg);color:#e5e7eb;margin:0}
header{padding:12px 16px;background:var(--panel);position:sticky;top:0;border-bottom:1px solid #1f2937;display:flex;align-items:center;gap:10px;z-index:1}
.badge{display:inline-block;background:var(--chip);color:#93c5fd;padding:4px 8px;border-radius:999px;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;padding:12px 16px}
.card{background:var(--panel);padding:10px;border-radius:12px}
.k{color:var(--muted);font-size:12px}
.v{font-size:18px;line-height:1.2;margin-top:2px}
.row{display:flex;gap:8px;align-items:center}
#bar{height:8px;background:#1f2937;border-radius:999px;overflow:hidden;margin-left:auto;min-width:180px}
#bar>div{height:100%;background:#3b82f6;width:0%}
#log{padding:12px 16px;white-space:pre-wrap;line-height:1.35}
.line{margin:0 0 6px 0}
.dim{color:var(--muted)} .ok{color:var(--ok)} .warn{color:var(--warn)} .err{color:var(--err)}
@media(max-width:1200px){.grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:700px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
small{color:var(--muted)}
</style>
</head>
<body>
<header class="row">
  <strong>AI Training Console</strong>
  <span id="epoch" class="badge">epoch 0/0 â€¢ 0%</span>
  <div id="bar"><div></div></div>
  <span id="eta" class="badge">ETA â€“</span>
  <span id="elapsed" class="badge">elapsed â€“</span>
</header>

<section class="grid">
  <div class="card"><div class="k">Project</div><div id="proj" class="v">â€“</div></div>
  <div class="card"><div class="k">Project dir</div><div id="pdir" class="v" style="font-size:12px;word-break:break-all">â€“</div></div>
  <div class="card"><div class="k">LR</div><div id="lr" class="v">â€“</div></div>
  <div class="card"><div class="k">Loss (total)</div><div id="loss" class="v">â€“</div></div>
  <div class="card"><div class="k">Loss box / cls / dfl</div><div id="lossx" class="v">â€“ / â€“ / â€“</div></div>
  <div class="card"><div class="k">Images / sec</div><div id="ips" class="v">â€“</div></div>

  <div class="card"><div class="k">Epoch time / Avg (s)</div><div id="etime" class="v">â€“ / â€“</div></div>
  <div class="card"><div class="k">Batch / ImgSize</div><div id="bi" class="v">â€“ / â€“</div></div>
  <div class="card"><div class="k">Precision (B)</div><div id="prec" class="v">â€“</div></div>
  <div class="card"><div class="k">Recall (B)</div><div id="rec" class="v">â€“</div></div>
  <div class="card"><div class="k">mAP50 (B)</div><div id="m50" class="v">â€“</div></div>
  <div class="card"><div class="k">mAP50-95 (B)</div><div id="m5095" class="v">â€“</div></div>

  <div class="card"><div class="k">Best mAP50-95</div><div id="best" class="v">â€“</div></div>
  <div class="card"><div class="k">Run dir</div><div id="sdir" class="v" style="font-size:12px;word-break:break-all">â€“</div></div>
  <div class="card"><div class="k">Device</div><div id="dev" class="v">â€“</div><small id="ver">â€“</small></div>
  <div class="card"><div class="k">GPU Memory</div><div id="mem" class="v">â€“</div></div>
  <div class="card"><div class="k">Dataset (train / val)</div><div id="ds" class="v">â€“</div></div>
  <div class="card"><div class="k">Optimizer</div><div id="opt" class="v">â€“</div></div>

  <div class="card"><div class="k">Start time</div><div id="start" class="v">â€“</div></div>
  <div class="card"><div class="k">Weights</div><div id="wts" class="v" style="font-size:12px;word-break:break-all">â€“</div></div>
</section>

<main id="log"></main>

<script>
  const el = (id)=>document.getElementById(id);
  const epoch = el('epoch'), bar = el('bar').firstElementChild;
  const f = (x, n=4)=> (x===null||x===undefined||isNaN(x)) ? 'â€“' : (+x).toFixed(n);
  const fmtT = (s)=>{ if(!s&&s!==0) return 'â€“'; s=Math.max(0,Math.floor(s)); const h=String(Math.floor(s/3600)).padStart(2,'0'); const m=String(Math.floor((s%3600)/60)).padStart(2,'0'); const ss=String(s%60).padStart(2,'0'); return `${h}:${m}:${ss}`; }
  const add = (rec)=>{
    const div = document.createElement('div');
    const cls = rec.kind === 'warn' ? 'warn' : rec.kind === 'error' ? 'err' : rec.kind === 'ok' ? 'ok' : 'dim';
    div.className = 'line ' + cls;
    div.textContent = `[${rec.t}] ${rec.msg}`;
    document.getElementById('log').appendChild(div);
    window.scrollTo(0, document.body.scrollHeight);
  };

  fetch('/history').then(r=>r.json()).then(h=>{
    (h.logs||[]).forEach(add);
    updateState(h.state||{});
  });

  function updateState(s){
    epoch.textContent = `epoch ${s.epoch}/${s.epochs} â€¢ ${f(s.progress,1)}%`;
    bar.style.width = (s.progress||0) + '%';
    el('eta').textContent = 'ETA ' + fmtT(s.eta_sec);
    el('elapsed').textContent = 'elapsed ' + fmtT(s.elapsed_sec);
    el('proj').textContent = s.project_name || 'â€“';
    el('pdir').textContent = s.project_dir || 'â€“';
    el('lr').textContent = f(s.lr,3);
    el('loss').textContent = f(s.loss,4);
    el('lossx').textContent = `${f(s.loss_box,4)} / ${f(s.loss_cls,4)} / ${f(s.loss_dfl,4)}`;
    el('ips').textContent = f(s.ips,2);
    el('etime').textContent = `${f(s.epoch_time,2)} / ${f(s.avg_epoch_time,2)}`;
    el('bi').textContent = `${s.batch_size ?? 'â€“'} / ${s.img_size ?? 'â€“'}`;
    el('prec').textContent = f(s.precision,4);
    el('rec').textContent = f(s.recall,4);
    el('m50').textContent = f(s.map50,4);
    el('m5095').textContent = f(s.map5095,4);
    el('best').textContent = f(s.best_map5095,4);
    el('sdir').textContent = s.save_dir || 'â€“';
    el('wts').textContent = s.weights || 'â€“';
    el('start').textContent = s.started_at || 'â€“';

    if(s.env){
      el('dev').textContent = s.env.device || 'â€“';
      el('ver').textContent = `torch ${s.env.torch} â€¢ cuda ${s.env.cuda} â€¢ ultralytics ${s.env.ultra}`;
      el('mem').textContent = s.env.vram_used && s.env.vram_total ? `${s.env.vram_used} / ${s.env.vram_total} GB` : 'â€“';
    }
    if(s.dataset){
      el('ds').textContent = `${s.dataset.train_images ?? 'â€“'} / ${s.dataset.val_images ?? 'â€“'}`;
    }
    if(s.hparams){
      const hp = s.hparams;
      el('opt').textContent = `${hp.optimizer || 'â€”'} â€¢ lr0 ${hp.lr0 ?? 'â€”'} â€¢ mom ${hp.momentum ?? 'â€”'} â€¢ wd ${hp.weight_decay ?? 'â€”'}`;
    }
  }

  const es = new EventSource('/stream');
  es.onmessage = (e)=>{
    const rec = JSON.parse(e.data);
    if(rec.type==='state'){ updateState(rec); } else { add(rec); }
  };
  es.onerror = ()=>{ epoch.textContent = 'disconnected'; };
</script>
</body>
</html>"""

    @app.get("/stream")
    def stream():
        def gen():
            last_push = 0.0
            while True:
                try:
                    rec = LOG_QUEUE.get(timeout=1.0)
                    yield f"data: {json.dumps(rec, ensure_ascii=False)}\n\n"
                except queue.Empty:
                    pass

                now = time.time()
                if now - last_push >= 1.0:
                    st = get_state_snapshot()
                    st["type"] = "state"
                    yield f"data: {json.dumps(st, ensure_ascii=False)}\n\n"
                    last_push = now
        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.get("/history")
    def history():
        with LOG_LOCK:
            logs = list(LOG_HISTORY)
        return JSONResponse({"state": get_state_snapshot(), "logs": logs})

    @app.get("/status")
    def status():
        return JSONResponse(get_state_snapshot())

    threading.Thread(
        target=lambda: uvicorn.run(app, host=host, port=port, log_level="warning"),
        daemon=True
    ).start()

    display_host = "127.0.0.1" if host == "0.0.0.0" else host
    push_log(f"Web console started at http://{display_host}:{port}", "ok")


def _load_class_names(data_yaml: Path) -> Dict[int, str]:
    with open(data_yaml, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    names = data.get("names", {})
    if isinstance(names, list):
        return {i: n for i, n in enumerate(names)}
    if isinstance(names, dict):
        return {int(k): v for k, v in names.items()}
    return {}


def _load_data_yaml_dict(data_yaml: Path) -> Dict[str, Any]:
    with open(data_yaml, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def _resolve_data_split_dir(data_yaml: Path, key: str = "val") -> Path | None:
    data = _load_data_yaml_dict(data_yaml)
    value = data.get(key)
    if isinstance(value, list):
        value = value[0] if value else None
    if not value:
        return None

    p = Path(str(value))
    if not p.is_absolute():
        p = (data_yaml.parent / p).resolve()
    if p.exists() and p.is_dir():
        return p
    return None


def _resolve_label_dir_from_image_dir(image_dir: Path) -> Path | None:
    if image_dir.name.lower() == "images":
        candidate = image_dir.parent / "labels"
        if candidate.exists() and candidate.is_dir():
            return candidate
    candidate = image_dir / "labels"
    if candidate.exists() and candidate.is_dir():
        return candidate
    return None


def _read_label_class_ids(label_file: Path | None) -> List[int]:
    if not label_file or not label_file.exists() or not label_file.is_file():
        return []
    ids: List[int] = []
    try:
        with open(label_file, "r", encoding="utf-8") as f:
            for line in f:
                parts = str(line or "").strip().split()
                if not parts:
                    continue
                try:
                    ids.append(int(float(parts[0])))
                except Exception:
                    continue
    except Exception:
        return []
    return sorted(set(ids))


def _count_images(path_or_list) -> int:
    try:
        if isinstance(path_or_list, (list, tuple)):
            return sum(_count_images(p) for p in path_or_list)
        p = Path(path_or_list)
        if p.is_dir():
            return len([x for x in p.rglob("*") if x.suffix.lower() in {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"}])
        elif p.is_file():
            with open(p, "r", encoding="utf-8") as f:
                return sum(1 for _ in f)
    except Exception:
        pass
    return 0


def _collect_dataset_stats(data_yaml: Path) -> Dict[str, int]:
    try:
        with open(data_yaml, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return {
            "train_images": _count_images(data.get("train")),
            "val_images": _count_images(data.get("val")),
        }
    except Exception:
        return {}


def _collect_env_info() -> Dict[str, Any]:
    device_name = "CPU"
    vram_used = vram_total = None
    cuda_ver = torch.version.cuda or "none"
    if torch.cuda.is_available():
        idx = torch.cuda.current_device()
        device_name = torch.cuda.get_device_name(idx)
        vram_total = round(torch.cuda.get_device_properties(idx).total_memory / 1024**3, 2)
        try:
            torch.cuda.synchronize()
            vram_used = round(torch.cuda.memory_allocated(idx) / 1024**3, 2)
        except Exception:
            vram_used = None
    return {
        "device": device_name,
        "vram_used": vram_used,
        "vram_total": vram_total,
        "torch": torch.__version__,
        "cuda": cuda_ver,
        "ultra": ULTRA_VERSION,
    }


def export_misclassified_samples(
    data_yaml_path: Path,
    prediction_labels_dir: Path,
    save_dir: Path,
    class_names: Dict[int, str]
) -> str | None:
    val_image_dir = _resolve_data_split_dir(data_yaml_path, "val")
    if not val_image_dir:
        push_log("[WARN] cannot resolve val image dir for misclassified samples", "warn")
        return None

    val_label_dir = _resolve_label_dir_from_image_dir(val_image_dir)
    if not val_label_dir:
        push_log(f"[WARN] cannot resolve val label dir from: {val_image_dir}", "warn")
        return None

    image_by_stem: Dict[str, Path] = {}
    for img_file in val_image_dir.rglob("*"):
        if not img_file.is_file():
            continue
        if img_file.suffix.lower() not in {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tif", ".tiff"}:
            continue
        image_by_stem.setdefault(img_file.stem, img_file)

    gt_labels_by_stem: Dict[str, Path] = {}
    for gt_file in val_label_dir.rglob("*.txt"):
        if gt_file.is_file():
            gt_labels_by_stem.setdefault(gt_file.stem, gt_file)

    pred_labels_by_stem: Dict[str, Path] = {}
    for pred_file in prediction_labels_dir.glob("*.txt"):
        if pred_file.is_file():
            pred_labels_by_stem.setdefault(pred_file.stem, pred_file)

    sample_root = save_dir / "misclassified_samples"
    sample_root.mkdir(parents=True, exist_ok=True)

    copied_rel_by_stem: Dict[str, str] = {}
    sample_rows: List[Dict[str, Any]] = []

    for stem in sorted(set(gt_labels_by_stem) | set(pred_labels_by_stem)):
        gt_ids = set(_read_label_class_ids(gt_labels_by_stem.get(stem)))
        pred_ids = set(_read_label_class_ids(pred_labels_by_stem.get(stem)))
        if gt_ids == pred_ids:
            continue

        image_file = image_by_stem.get(stem)
        if not image_file or not image_file.exists():
            continue

        if stem not in copied_rel_by_stem:
            dest_file = sample_root / image_file.name
            if dest_file.exists():
                dest_file = sample_root / f"{image_file.stem}_{abs(hash(str(image_file))) % 100000}{image_file.suffix.lower()}"
            try:
                dest_file.write_bytes(image_file.read_bytes())
                copied_rel_by_stem[stem] = dest_file.relative_to(save_dir).as_posix()
            except Exception:
                continue

        sample_rel = copied_rel_by_stem.get(stem)
        if not sample_rel:
            continue

        missing_gt = sorted(gt_ids - pred_ids)
        extra_pred = sorted(pred_ids - gt_ids)

        if missing_gt and extra_pred:
            for gt_id in missing_gt:
                for pred_id in extra_pred[:5]:
                    sample_rows.append({
                        "image_name": image_file.name,
                        "sample_rel_path": sample_rel,
                        "gt_class_id": gt_id,
                        "gt_class_name": class_names.get(gt_id, str(gt_id)),
                        "pred_class_id": pred_id,
                        "pred_class_name": class_names.get(pred_id, str(pred_id)),
                        "issue_type": "class_confusion",
                    })
        elif missing_gt:
            for gt_id in missing_gt:
                sample_rows.append({
                    "image_name": image_file.name,
                    "sample_rel_path": sample_rel,
                    "gt_class_id": gt_id,
                    "gt_class_name": class_names.get(gt_id, str(gt_id)),
                    "pred_class_id": None,
                    "pred_class_name": "background",
                    "issue_type": "missed_detection",
                })
        elif extra_pred:
            for pred_id in extra_pred[:5]:
                sample_rows.append({
                    "image_name": image_file.name,
                    "sample_rel_path": sample_rel,
                    "gt_class_id": None,
                    "gt_class_name": "background",
                    "pred_class_id": pred_id,
                    "pred_class_name": class_names.get(pred_id, str(pred_id)),
                    "issue_type": "false_positive",
                })

    if not sample_rows:
        push_log("[WARN] no misclassified samples exported", "warn")
        return None

    sample_csv = save_dir / "misclassified_samples.csv"
    pd.DataFrame(sample_rows).to_csv(sample_csv, index=False, encoding="utf-8-sig")
    push_log(f"[VAL] misclassified_samples.csv saved ({len(sample_rows)} rows)")
    return str(sample_csv)


def validate_and_export(
    weights: str,
    data_yaml: str,
    img_size: int = 640,
    device: Union[int, str] = 0,
    iou: float = 0.65,
    conf: float = 0.50,
    output_dir: str | None = None
) -> Dict[str, Any]:
    data_yaml_path = Path(data_yaml).resolve()
    class_names = _load_class_names(data_yaml_path)

    push_log(f"[VAL] Using weights: {weights}")
    model = YOLO(str(weights))
    export_dir = Path(output_dir).resolve() if output_dir else None
    val_kwargs = {}
    if export_dir is not None:
        val_kwargs.update({
            "project": str(export_dir.parent),
            "name": export_dir.name,
            "exist_ok": True,
        })
        push_log(f"[VAL] Target export dir: {export_dir}")

    val_results = model.val(
        data=str(data_yaml_path),
        imgsz=img_size,
        batch=64,
        device=device,
        iou=iou,
        conf=conf,
        save_txt=True,
        save_conf=True,
        plots=True,
        **val_kwargs
    )

    save_dir = export_dir or Path(val_results.save_dir)
    labels_dir = save_dir / "labels"
    predictions_csv = save_dir / "predictions_val.csv"
    metrics_json = save_dir / "metrics_summary.json"
    per_class_csv = save_dir / "per_class_metrics.csv"

    try:
        metrics_dict = val_results.results_dict if hasattr(val_results, "results_dict") else {}
        metrics_dict = {k: float(v) for k, v in metrics_dict.items() if isinstance(v, (int, float))}
        with open(metrics_json, "w", encoding="utf-8") as f:
            json.dump(metrics_dict, f, ensure_ascii=False, indent=2)
        push_log("[VAL] metrics_summary.json saved")
        TRAIN_STATE["best_map5095"] = max(TRAIN_STATE.get("best_map5095") or 0.0, metrics_dict.get("metrics/mAP50-95(B)", 0.0))
        TRAIN_STATE["precision"] = metrics_dict.get("metrics/precision(B)")
        TRAIN_STATE["recall"] = metrics_dict.get("metrics/recall(B)")
        TRAIN_STATE["map50"] = metrics_dict.get("metrics/mAP50(B)")
        TRAIN_STATE["map5095"] = metrics_dict.get("metrics/mAP50-95(B)")
    except Exception as e:
        push_log(f"[WARN] save metrics_summary.json failed: {e}", "warn")

    try:
        maps = None
        if hasattr(val_results, "box") and hasattr(val_results.box, "maps"):
            maps = list(val_results.box.maps)
        elif hasattr(val_results, "maps"):
            maps = list(getattr(val_results, "maps"))
        if maps is not None:
            rows = [{"class_id": i, "class_name": class_names.get(i, str(i)), "AP50-95": float(ap) if ap is not None else None}
                    for i, ap in enumerate(maps)]
            pd.DataFrame(rows).to_csv(per_class_csv, index=False, encoding="utf-8-sig")
            push_log("[VAL] per_class_metrics.csv saved")
    except Exception as e:
        push_log(f"[WARN] save per_class_metrics.csv failed: {e}", "warn")

    mis_counts_csv = None
    mis_pairs_csv = None
    mis_samples_csv = None
    try:
        cm = getattr(val_results, "confusion_matrix", None)
        if cm is not None:
            df_cm_abs = pd.DataFrame(cm.summary(normalize=False, decimals=6))
            cm_abs_csv = save_dir / "confusion_matrix.csv"
            df_cm_abs.to_csv(cm_abs_csv, index=False, encoding="utf-8-sig")

            df_cm_norm = pd.DataFrame(cm.summary(normalize=True, decimals=6))
            cm_norm_csv = save_dir / "confusion_matrix_normalized.csv"
            df_cm_norm.to_csv(cm_norm_csv, index=False, encoding="utf-8-sig")

            raw_npy = save_dir / "confusion_matrix_raw.npy"
            np.save(raw_npy, cm.matrix)
            push_log("[VAL] confusion matrices saved (csv+npy)")

            m = cm.matrix.astype(int)
            nc = len(class_names)
            m = m[:nc + 1, :nc + 1]
            idx_to_name = [class_names.get(i, str(i)) for i in range(nc)] + ["background"]
            rows_sum = m.sum(axis=1)

            out_rows = []
            for gt in range(nc):
                gt_total = int(rows_sum[gt])
                correct = int(m[gt, gt])
                missed_bg = int(m[gt, nc])
                mis_other = int(m[gt, :nc].sum() - m[gt, gt])
                total_errors = mis_other + missed_bg
                err_rate = (total_errors / gt_total) if gt_total > 0 else 0.0
                out_rows.append({
                    "gt_class_id": gt,
                    "gt_class_name": idx_to_name[gt],
                    "gt_total": gt_total,
                    "correct": correct,
                    "mis_as_other_classes": mis_other,
                    "missed_as_background": missed_bg,
                    "total_errors": total_errors,
                    "error_rate": round(err_rate, 6)
                })
            mis_counts_csv = save_dir / "misclassified_counts.csv"
            pd.DataFrame(out_rows).to_csv(mis_counts_csv, index=False, encoding="utf-8-sig")

            pair_rows = []
            for gt in range(nc):
                gt_total = int(rows_sum[gt])
                if gt_total == 0:
                    continue
                for pred in range(nc + 1):
                    if pred == gt:
                        continue
                    cnt = int(m[gt, pred])
                    if cnt <= 0:
                        continue
                    pair_rows.append({
                        "gt_class_id": gt,
                        "gt_class_name": idx_to_name[gt],
                        "pred_class_id": pred if pred < nc else None,
                        "pred_class_name": idx_to_name[pred],
                        "count": cnt,
                        "rate_over_gt": round(cnt / gt_total, 6)
                    })
            if pair_rows:
                df_pairs = pd.DataFrame(pair_rows).sort_values(["count", "rate_over_gt"], ascending=False)
                mis_pairs_csv = save_dir / "misclassified_pairs.csv"
                df_pairs.to_csv(mis_pairs_csv, index=False, encoding="utf-8-sig")
            push_log("[VAL] misclassified_* saved")
        else:
            push_log("[WARN] no confusion_matrix in val results", "warn")
    except Exception as e:
        push_log(f"[WARN] confusion export failed: {e}", "warn")

    try:
        rows: List[Dict[str, Any]] = []
        if labels_dir.exists():
            for txt_path in sorted(labels_dir.glob("*.txt")):
                stem = txt_path.stem
                with open(txt_path, "r", encoding="utf-8") as f:
                    for line in f:
                        parts = line.strip().split()
                        if len(parts) < 6:
                            continue
                        cls_id = int(float(parts[0]))
                        cx, cy, w, h = map(float, parts[1:5])
                        conf_score = float(parts[5])
                        rows.append({
                            "image_name": stem,
                            "class_id": cls_id,
                            "class_name": class_names.get(cls_id, str(cls_id)),
                            "conf": conf_score,
                            "cx_norm": cx, "cy_norm": cy, "w_norm": w, "h_norm": h
                        })
            if rows:
                pd.DataFrame(rows).to_csv(predictions_csv, index=False, encoding="utf-8-sig")
                push_log("[VAL] predictions_val.csv saved")
        else:
            push_log(f"[WARN] val labels dir not found: {labels_dir}", "warn")
    except Exception as e:
        push_log(f"[WARN] export predictions failed: {e}", "warn")

    try:
        if labels_dir.exists():
            mis_samples_csv = export_misclassified_samples(
                data_yaml_path=data_yaml_path,
                prediction_labels_dir=labels_dir,
                save_dir=save_dir,
                class_names=class_names,
            )
    except Exception as e:
        push_log(f"[WARN] export misclassified samples failed: {e}", "warn")

    return {
        "val_dir": str(save_dir),
        "metrics_json": str(metrics_json),
        "per_class_csv": str(per_class_csv),
        "predictions_csv": str(predictions_csv),
        "misclassified_counts_csv": str(mis_counts_csv) if mis_counts_csv else None,
        "misclassified_pairs_csv": str(mis_pairs_csv) if mis_pairs_csv else None,
        "misclassified_samples_csv": str(mis_samples_csv) if mis_samples_csv else None,
    }


def main():
    current_project_dir = str(Path.cwd().resolve())
    current_project_name = Path.cwd().resolve().name

    update_state(project_dir=current_project_dir, project_name=current_project_name, msg="starting")

    start_web_server(host="0.0.0.0", port=8008)

    device = 0
    push_log(f"Training on device: {device}")
    push_log(f"Current project: {current_project_name}")
    push_log(f"Current dir: {current_project_dir}")

    model = YOLO('yolo11m.pt')

    data_yaml = r"data.yaml"
    epochs = 200
    batch_size = 26
    img_size = 640
    hparams = dict(
        optimizer='SGD',
        lr0=0.01, lrf=0.25, cos_lr=True, momentum=0.937, weight_decay=0.0005,
        warmup_epochs=10,
    )

    update_state(**{
        "env": _collect_env_info(),
        "dataset": _collect_dataset_stats(Path(data_yaml)),
        "hparams": dict(hparams, batch=batch_size, imgsz=img_size),
        "is_training": True,
        "epoch": 0,
        "epochs": epochs,
        "progress": 0.0,
        "batch_size": batch_size,
        "img_size": img_size,
        "elapsed_sec": 0.0,
        "eta_sec": None,
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "project_name": current_project_name,
        "project_dir": current_project_dir,
        "msg": "training",
    })

    _fit_start = time.time()
    _epoch_start = {"t": None}
    _img_seen_this_epoch = {"n": 0}
    _epoch_times = []

    def on_train_start(trainer):
        update_state(msg="training")
        push_log("[TRAIN] start")

    def on_train_epoch_start(trainer):
        _epoch_start["t"] = time.time()
        _img_seen_this_epoch["n"] = 0

    def on_train_batch_end(trainer):
        try:
            bs = int(getattr(trainer, "batch_size", TRAIN_STATE.get("batch_size") or 0))
            _img_seen_this_epoch["n"] += bs
            dur = max(1e-6, time.time() - (_epoch_start["t"] or time.time()))
            update_state(ips=_img_seen_this_epoch["n"] / dur)
        except Exception:
            pass

    def on_fit_epoch_end(trainer):
        try:
            ep = int(trainer.epoch) + 1
            tot = int(trainer.epochs)
            TRAIN_STATE["epoch"] = ep
            TRAIN_STATE["progress"] = ep / tot * 100.0 if tot else 0.0

            try:
                TRAIN_STATE["lr"] = float(trainer.optimizer.param_groups[0]["lr"])
            except Exception:
                pass

            lbox = lcls = dfl = loss_total = None
            try:
                li = getattr(trainer, "loss_items", None)
                if li is not None:
                    vals = [float(x) for x in (li.tolist() if hasattr(li, "tolist") else li)]
                    if len(vals) >= 1:
                        lbox = vals[0]
                    if len(vals) >= 2:
                        lcls = vals[1]
                    if len(vals) >= 3:
                        dfl = vals[2]
                    if len(vals) >= 4:
                        loss_total = vals[3]
            except Exception:
                pass
            if loss_total is None:
                try:
                    loss_total = float(getattr(trainer, "loss", None))
                except Exception:
                    pass

            TRAIN_STATE["loss_box"] = lbox
            TRAIN_STATE["loss_cls"] = lcls
            TRAIN_STATE["loss_dfl"] = dfl
            TRAIN_STATE["loss"] = loss_total

            try:
                et = time.time() - (_epoch_start["t"] or time.time())
                TRAIN_STATE["epoch_time"] = et
                _epoch_times.append(et)
                if len(_epoch_times) > 20:
                    _epoch_times.pop(0)
                avg_et = sum(_epoch_times) / len(_epoch_times)
                TRAIN_STATE["avg_epoch_time"] = avg_et
                TRAIN_STATE["elapsed_sec"] = time.time() - _fit_start
                rem = max(0, tot - ep)
                TRAIN_STATE["eta_sec"] = rem * avg_et
            except Exception:
                pass

            prec = rec = m50 = m5095 = None
            for obj in (
                getattr(trainer, "validator", None),
                getattr(getattr(trainer, "validator", None), "metrics", None),
                getattr(trainer, "metrics", None),
            ):
                try:
                    d = getattr(obj, "results_dict", None)
                    if isinstance(d, dict) and d:
                        prec = prec or d.get("metrics/precision(B)")
                        rec = rec or d.get("metrics/recall(B)")
                        m50 = m50 or d.get("metrics/mAP50(B)")
                        m5095 = m5095 or d.get("metrics/mAP50-95(B)")
                except Exception:
                    pass

            TRAIN_STATE["precision"] = float(prec) if prec is not None else TRAIN_STATE["precision"]
            TRAIN_STATE["recall"] = float(rec) if rec is not None else TRAIN_STATE["recall"]
            TRAIN_STATE["map50"] = float(m50) if m50 is not None else TRAIN_STATE["map50"]
            TRAIN_STATE["map5095"] = float(m5095) if m5095 is not None else TRAIN_STATE["map5095"]

            if TRAIN_STATE["map5095"] is not None:
                bm = TRAIN_STATE.get("best_map5095")
                TRAIN_STATE["best_map5095"] = max(bm, TRAIN_STATE["map5095"]) if bm is not None else TRAIN_STATE["map5095"]

            push_log(
                f"[TRAIN] Epoch {ep}/{tot} "
                f"lr={TRAIN_STATE['lr']:.3e} "
                f"loss={TRAIN_STATE['loss']} "
                f"ips={TRAIN_STATE.get('ips'):.2f} "
                f"avg_et={TRAIN_STATE.get('avg_epoch_time'):.2f}s "
                f"ETA~{TRAIN_STATE.get('eta_sec'):.0f}s"
            )
        except Exception as e:
            push_log(f"[WARN] on_fit_epoch_end metric parse failed: {e}", "warn")

    def on_train_end(trainer):
        update_state(msg="train_end")
        push_log("[TRAIN] end")

    model.add_callback("on_train_start", on_train_start)
    model.add_callback("on_train_epoch_start", on_train_epoch_start)
    model.add_callback("on_train_batch_end", on_train_batch_end)
    model.add_callback("on_fit_epoch_end", on_fit_epoch_end)
    model.add_callback("on_train_end", on_train_end)

    try:
        results = model.train(
            data=data_yaml,
            epochs=epochs,
            imgsz=img_size,
            batch=batch_size,
            optimizer=hparams["optimizer"],
            lr0=hparams["lr0"],
            lrf=hparams["lrf"],
            cos_lr=hparams["cos_lr"],
            momentum=hparams["momentum"],
            weight_decay=hparams["weight_decay"],
            warmup_epochs=hparams["warmup_epochs"],
            augment=True,
            workers=10,
            hsv_h=0.015, hsv_s=0.2, hsv_v=0.2,
            flipud=0.0, fliplr=0.0,
            mosaic=0.0, mixup=0.0,
            degrees=0.5, translate=0.0, scale=0.05,
            shear=0.0, perspective=0.0, copy_paste=0.0,
            device=device,
            project=r"Output",
            name="Model_Train",
            resume=False,
            plots=True,
            patience=0,
            close_mosaic=30,
            save_conf=True,
            save_txt=False
        )
        push_log("Done training", "ok")

        save_dir = Path(results.save_dir)
        best_path = save_dir / "weights" / "best.pt"
        last_path = save_dir / "weights" / "last.pt"
        weights_path = best_path if best_path.exists() else last_path
        if not weights_path.exists():
            push_log(f"Khong tim thay weights trong: {save_dir / 'weights'}", "error")
            raise FileNotFoundError(f"Khong tim thay weights trong: {save_dir / 'weights'}")

        update_state(save_dir=str(save_dir), run_name=save_dir.name, weights=str(weights_path), msg="validating")

        info = validate_and_export(
            weights=str(weights_path),
            data_yaml=data_yaml,
            img_size=img_size,
            device=device,
            iou=0.65,
            conf=0.50,
            output_dir=str(save_dir)
        )
        update_state(is_training=False, progress=100.0, msg="completed")
        push_log("Validation done", "ok")
        push_log(f"Artifacts dir: {info['val_dir']}", "ok")

    except Exception as e:
        update_state(is_training=False, msg=f"failed: {e}")
        push_log(f"[ERROR] Training failed: {e}", "error")
        push_log(traceback.format_exc(), "error")
        raise

    finally:
        st = get_state_snapshot()
        if st.get("progress", 0.0) >= 100.0 and str(st.get("msg", "")).startswith("completed"):
            update_state(is_training=False, progress=100.0)
        else:
            update_state(is_training=False)


if __name__ == '__main__':
    main()
