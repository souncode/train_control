import json
import re
import shutil
import tempfile
import zipfile
from pathlib import Path

from tc_config import ROOT_DIR


def is_valid_project_name(name: str) -> bool:
    text = str(name or "").strip()
    if not text:
        return False
    if text in {".", ".."}:
        return False
    if re.search(r'[\\/:*?"<>|]', text):
        return False
    return True


def get_available_duplicate_name(source_name: str) -> str:
    base = f"{str(source_name or '').strip()} - Copy".strip()
    candidate = base
    idx = 2
    while (ROOT_DIR / candidate).exists():
        candidate = f"{base} {idx}"
        idx += 1
    return candidate


def is_path_inside(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def clear_project_dataset_dirs(project_path: Path, progress_cb=None):
    removed = []
    folders = ("runs", "test", "train", "valid")
    total = len(folders)
    for idx, folder_name in enumerate(folders, start=1):
        target = (project_path / folder_name).resolve()
        if not is_path_inside(target, project_path):
            continue
        if callable(progress_cb):
            try:
                progress_cb(idx - 1, total, folder_name, "scan")
            except Exception:
                pass
        if target.exists() and target.is_dir():
            shutil.rmtree(target)
            removed.append(folder_name)
        if callable(progress_cb):
            try:
                progress_cb(idx, total, folder_name, "done")
            except Exception:
                pass
    return removed


def dataset_config_file_path(base_dir: Path) -> Path:
    return base_dir / "dataset_config.json"


def read_shared_dataset_config(base_dir: Path):
    p = dataset_config_file_path(base_dir)
    if not p.exists() or not p.is_file():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_shared_dataset_config(base_dir: Path, data: dict):
    p = dataset_config_file_path(base_dir)
    p.write_text(json.dumps(data or {}, ensure_ascii=False, indent=2), encoding="utf-8")


def load_dataset_config(base_dir: Path, project_path: Path | None = None):
    defaults = {
        "train_percent": 80,
        "valid_percent": 20,
        "test_percent": 0,
        "shuffle": True,
        "seed": 42,
        "split_by_class": False,
        "train_all_data": False,
    }

    data = read_shared_dataset_config(base_dir)
    if not isinstance(data, dict):
        return dict(defaults)

    try:
        cfg = dict(defaults)
        for key in ("train_percent", "valid_percent", "test_percent"):
            try:
                cfg[key] = int(data.get(key, cfg[key]))
            except Exception:
                pass
        cfg["shuffle"] = bool(data.get("shuffle", cfg["shuffle"]))
        try:
            cfg["seed"] = int(data.get("seed", cfg["seed"]))
        except Exception:
            pass
        cfg["split_by_class"] = bool(data.get("split_by_class", cfg["split_by_class"]))
        cfg["train_all_data"] = bool(data.get("train_all_data", cfg["train_all_data"]))
        return cfg
    except Exception:
        return dict(defaults)


def validate_dataset_config(cfg: dict):
    try:
        train_pct = int(cfg.get("train_percent", 0))
        valid_pct = int(cfg.get("valid_percent", 0))
        test_pct = int(cfg.get("test_percent", 0))
        seed = int(cfg.get("seed", 42))
    except Exception:
        return False, "Tỷ lệ dataset không hợp lệ", None

    shuffle_enabled = bool(cfg.get("shuffle", True))
    split_by_class = bool(cfg.get("split_by_class", False))
    train_all_data = bool(cfg.get("train_all_data", False))

    values = [train_pct, valid_pct, test_pct]
    if any(x < 0 or x > 100 for x in values):
        return False, "Tỷ lệ dataset phải nằm trong khoảng 0-100", None
    if (train_pct + valid_pct + test_pct) != 100:
        return False, "Tổng Train/Validation/Test phải bằng 100", None

    return True, None, {
        "train_percent": train_pct,
        "valid_percent": valid_pct,
        "test_percent": test_pct,
        "shuffle": shuffle_enabled,
        "seed": seed,
        "split_by_class": split_by_class,
        "train_all_data": train_all_data,
    }


def save_dataset_config(base_dir: Path, project_path: Path | None, cfg: dict):
    ok, err, clean_cfg = validate_dataset_config(cfg)
    if not ok:
        return False, err, None
    try:
        write_shared_dataset_config(base_dir, clean_cfg)
        return True, None, clean_cfg
    except Exception as e:
        return False, f"Không lưu được dataset config: {e}", None


def resolve_uploaded_project_dir(extract_root: Path):
    try:
        items = list(extract_root.iterdir())
    except Exception:
        return None, "cannot_scan_extracted_content"

    if not items:
        return None, "project_not_found"

    top_level_dirs = [item for item in items if item.is_dir()]
    top_level_files = [item for item in items if item.is_file()]

    if len(top_level_dirs) == 1 and not top_level_files:
        return top_level_dirs[0], None

    return extract_root, None


def save_uploaded_project_zip(upload_file, base_dir: Path, sanitize_name):
    if not upload_file:
        return False, "missing_file", None

    filename = str(upload_file.filename or "").strip()
    if not filename.lower().endswith(".zip"):
        return False, "only_zip_supported", None

    with tempfile.TemporaryDirectory(prefix="traincontrol_upload_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        zip_path = tmp_dir_path / "upload.zip"
        extract_root = tmp_dir_path / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)

        upload_file.save(str(zip_path))

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                infos = zf.infolist()
                if not infos:
                    return False, "zip_empty", None

                for info in infos:
                    name = str(info.filename or "").replace("\\", "/")
                    parts = Path(name).parts
                    if name.startswith("/") or ".." in parts:
                        return False, "invalid_zip_path", None

                zf.extractall(path=str(extract_root))
        except zipfile.BadZipFile:
            return False, "bad_zip_file", None
        except Exception:
            return False, "extract_failed", None

        candidate, err = resolve_uploaded_project_dir(extract_root)
        if err or not candidate:
            return False, err or "project_not_found", None

        suggested_name = candidate.name
        if candidate == extract_root:
            suggested_name = Path(filename).stem

        target_name = sanitize_name(suggested_name)
        target_dir = ROOT_DIR / target_name

        if target_dir.exists():
            return False, "project_already_exists", target_name

        try:
            shutil.copytree(candidate, target_dir)
        except Exception:
            return False, "copy_failed", target_name

    return True, "uploaded", target_name


def collect_importable_data_files(extract_root: Path, image_exts: set[str]):
    image_rows = []
    label_rows = []
    file_items = []

    for src in extract_root.rglob("*"):
        if src.is_file():
            rel = src.relative_to(extract_root)
            file_items.append((src, rel))

    if not file_items:
        return image_rows, label_rows

    top_level_parts = {rel.parts[0] for _, rel in file_items if rel.parts}
    strip_first_part = len(top_level_parts) == 1

    for src, rel in file_items:
        rel_parts = list(rel.parts)
        if strip_first_part and len(rel_parts) > 1:
            rel_parts = rel_parts[1:]
        elif strip_first_part and len(rel_parts) == 1:
            rel_parts = rel.parts[:]

        lower_parts = [part.lower() for part in rel_parts]
        suffix = src.suffix.lower()

        if suffix in image_exts:
            trimmed_parts = rel_parts[:]
            if lower_parts and lower_parts[0] in ("data", "dataset", "image", "images"):
                trimmed_parts = rel_parts[1:]
            rel_target = Path(*trimmed_parts) if trimmed_parts else Path(src.name)
            image_rows.append((src, rel_target))
            continue

        if suffix != ".txt":
            continue

        trimmed_parts = rel_parts[:]
        if lower_parts and lower_parts[0] in ("data", "dataset", "labels", "label"):
            trimmed_parts = rel_parts[1:]
        elif lower_parts and lower_parts[0] in ("image", "images"):
            trimmed_parts = rel_parts[1:]

        rel_target = Path(*trimmed_parts) if trimmed_parts else Path(src.stem).with_suffix(".txt")
        label_rows.append((src, rel_target.with_suffix(".txt")))

    return image_rows, label_rows
