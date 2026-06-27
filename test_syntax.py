from pathlib import Path

def _read_yolo_label_rows(text: str):
    rows = []
    for line in str(text or "").replace("\r\n", "\n").split("\n"):
        s = line.strip()
        if not s:
            continue
        parts = s.split()
        if len(parts) < 5:
            continue
        try:
            cls = int(float(parts[0]))
            cx = float(parts[1])
            cy = float(parts[2])
            w = float(parts[3])
            h = float(parts[4])
        except Exception:
            continue
        rows.append({
            "cls": cls,
            "cx": cx,
            "cy": cy,
            "w": w,
            "h": h,
        })
    return rows

print("OK")
