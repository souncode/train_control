import os
import shutil
import threading
import time
from pathlib import Path


BACKUP_TASK = {
    "id": None,
    "project": "",
    "target_path": "",
    "status": "idle",
    "progress": 0.0,
    "eta_sec": None,
    "copied_bytes": 0,
    "total_bytes": 0,
    "started_at": 0.0,
    "ended_at": 0.0,
    "message": "",
}

backup_lock = threading.Lock()


def build_backup_status_payload():
    with backup_lock:
        payload = dict(BACKUP_TASK)
    payload["ok"] = True
    return payload


def reset_backup_task_locked():
    BACKUP_TASK.update({
        "id": None,
        "project": "",
        "target_path": "",
        "status": "idle",
        "progress": 0.0,
        "eta_sec": None,
        "copied_bytes": 0,
        "total_bytes": 0,
        "started_at": 0.0,
        "ended_at": 0.0,
        "message": "",
    })


def safe_backup_target_path(project_name: str, backup_root: Path) -> Path:
    target_path = (backup_root / str(project_name or "").strip()).resolve()
    backup_root_resolved = backup_root.resolve()
    if target_path == backup_root_resolved or backup_root_resolved not in target_path.parents:
        raise ValueError("Invalid backup target path")
    return target_path


def estimate_backup_eta(total_bytes: int, copied_bytes: int, started_at: float) -> int | None:
    if total_bytes <= 0 or copied_bytes <= 0 or started_at <= 0:
        return None
    elapsed = max(0.001, time.time() - started_at)
    speed = copied_bytes / elapsed
    if speed <= 0:
        return None
    remaining = max(0, total_bytes - copied_bytes)
    return max(0, int(remaining / speed))


def calculate_directory_size(source_dir: Path) -> int:
    total = 0
    for root, _, files in os.walk(source_dir):
        root_path = Path(root)
        for name in files:
            try:
                total += (root_path / name).stat().st_size
            except Exception:
                pass
    return max(1, total)


def update_backup_progress(copied_bytes: int, total_bytes: int):
    with backup_lock:
        if BACKUP_TASK.get("status") != "running":
            return
        BACKUP_TASK["copied_bytes"] = int(copied_bytes)
        BACKUP_TASK["total_bytes"] = int(total_bytes)
        BACKUP_TASK["progress"] = max(0.0, min(100.0, (copied_bytes / max(1, total_bytes)) * 100.0))
        BACKUP_TASK["eta_sec"] = estimate_backup_eta(total_bytes, copied_bytes, float(BACKUP_TASK.get("started_at") or 0.0))


def copy_project_with_progress(source_dir: Path, target_dir: Path, total_bytes: int, chunk_size: int):
    copied_bytes = 0
    for root, dirs, files in os.walk(source_dir):
        root_path = Path(root)
        rel_root = root_path.relative_to(source_dir)
        dst_root = target_dir / rel_root
        dst_root.mkdir(parents=True, exist_ok=True)

        for dir_name in dirs:
            (dst_root / dir_name).mkdir(parents=True, exist_ok=True)

        for file_name in files:
            src_file = root_path / file_name
            dst_file = dst_root / file_name
            if dst_file.exists():
                try:
                    shutil.copystat(src_file, dst_file, follow_symlinks=False)
                except Exception:
                    pass
            with src_file.open("rb") as sf, dst_file.open("wb") as df:
                while True:
                    chunk = sf.read(chunk_size)
                    if not chunk:
                        break
                    df.write(chunk)
                    copied_bytes += len(chunk)
                    update_backup_progress(copied_bytes, total_bytes)
            try:
                shutil.copystat(src_file, dst_file, follow_symlinks=False)
            except Exception:
                pass
    update_backup_progress(total_bytes, total_bytes)


def run_project_backup(task_id: str, project_name: str, source_dir: Path, target_dir: Path, chunk_size: int):
    try:
        total_bytes = calculate_directory_size(source_dir)
        with backup_lock:
            BACKUP_TASK["total_bytes"] = total_bytes
            BACKUP_TASK["message"] = f"Backing up {project_name}"

        if target_dir.exists():
            raise FileExistsError(f"Backup target already exists: {target_dir}")

        target_dir.parent.mkdir(parents=True, exist_ok=True)
        copy_project_with_progress(source_dir, target_dir, total_bytes, chunk_size)

        with backup_lock:
            if BACKUP_TASK.get("id") == task_id:
                BACKUP_TASK["status"] = "success"
                BACKUP_TASK["progress"] = 100.0
                BACKUP_TASK["eta_sec"] = 0
                BACKUP_TASK["ended_at"] = time.time()
                BACKUP_TASK["message"] = f"Backup completed: {target_dir}"
    except Exception as e:
        try:
            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
        except Exception:
            pass
        with backup_lock:
            if BACKUP_TASK.get("id") == task_id:
                BACKUP_TASK["status"] = "failed"
                BACKUP_TASK["ended_at"] = time.time()
                BACKUP_TASK["message"] = str(e)


def start_project_backup(project_name: str, backup_root: Path, chunk_size: int, resolve_project_path):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Không tìm thấy project", None

    try:
        target_path = safe_backup_target_path(project_name, backup_root)
    except Exception as e:
        return False, str(e), None

    with backup_lock:
        if BACKUP_TASK.get("status") == "running":
            return False, "Đang có backup khác chạy", None
        reset_backup_task_locked()
        task_id = f"backup-{int(time.time() * 1000)}"
        BACKUP_TASK.update({
            "id": task_id,
            "project": project_name,
            "target_path": str(target_path),
            "status": "running",
            "progress": 0.0,
            "eta_sec": None,
            "copied_bytes": 0,
            "total_bytes": 0,
            "started_at": time.time(),
            "ended_at": 0.0,
            "message": f"Starting backup to {target_path}",
        })

    threading.Thread(
        target=run_project_backup,
        args=(task_id, project_name, project_path, target_path, chunk_size),
        daemon=True
    ).start()
    return True, f"Đã bắt đầu backup {project_name}", task_id
