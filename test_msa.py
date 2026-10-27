import io
import json
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import TrainControl as tc


class UploadedZip:
    def __init__(self, data):
        self.stream = io.BytesIO(data)

    def save(self, path):
        Path(path).write_bytes(self.stream.getvalue())


class FakeModel:
    names = {0: "OK", 1: "NG"}

    def __init__(self, weights):
        self.weights = weights

    def predict(self, **kwargs):
        if isinstance(kwargs["source"], list):
            for index, _ in enumerate(kwargs["source"]):
                yield FakePrediction(f"image{index}.jpg")
            return
        if kwargs.get("save") is False:
            for image_path in sorted(Path(kwargs["source"]).iterdir()):
                yield FakePrediction(image_path)
            return
        output = Path(kwargs["project"]) / kwargs["name"]
        (output / "labels").mkdir(parents=True)
        for image in Path(kwargs["source"]).iterdir():
            (output / image.name).write_bytes(image.read_bytes())
            (output / "labels" / f"{image.stem}.txt").write_text(
                "1 0.5 0.5 0.2 0.2\n", encoding="utf-8"
            )
            yield SimpleNamespace()


class FakePrediction:
    def __init__(self, path):
        self.path = path

    def save_txt(self, path, save_conf=False):
        Path(path).write_text("1 0.5 0.5 0.2 0.2\n", encoding="utf-8")


class MsaTests(unittest.TestCase):
    def test_image_only_zip_and_predict_missing_labels(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp)
            msa = project / "MSA"
            run = project / "Output" / "run1"
            (run / "weights").mkdir(parents=True)
            (run / "weights" / "best.pt").write_bytes(b"fake")
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("images/a.jpg", b"image a")
                archive.writestr("images/b.jpg", b"image b")
            with patch.object(tc, "msa_root_for_project", return_value=msa):
                ok, _, imported = tc.import_msa_zip("sample", UploadedZip(buffer.getvalue()))
                self.assertTrue(ok)
                self.assertEqual(imported["unlabeled"], 2)
                (msa / "labels" / "a.txt").write_text("0 0.5 0.5 0.1 0.1\n", encoding="utf-8")
                with patch.object(tc, "resolve_run_dir", return_value=(run, None)), \
                     patch.object(tc, "load_train_module", return_value=SimpleNamespace(YOLO=FakeModel)), \
                     patch.object(tc, "_read_run_args", return_value={}):
                    test_ok, test_message, _ = tc.run_project_msa_test("sample", "run1", "best.pt")
                    self.assertFalse(test_ok)
                    self.assertIn("no label", test_message)
                    progress = []
                    ok, _, payload = tc.predict_missing_msa_labels(
                        "sample", "run1", "best.pt", progress_cb=lambda *args: progress.append(args)
                    )
                self.assertTrue(ok)
                self.assertEqual(payload["generated"], 1)
                self.assertEqual(payload["total"], 1)
                self.assertEqual(progress[-1][0], 100)
                self.assertEqual((msa / "labels" / "a.txt").read_text(encoding="utf-8"), "0 0.5 0.5 0.1 0.1\n")
                self.assertEqual((msa / "labels" / "b.txt").read_text(encoding="utf-8"), "1 0.5 0.5 0.2 0.2\n")

    def test_predict_labels_api_reports_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp)
            msa = project / "MSA"
            (msa / "images").mkdir(parents=True)
            (msa / "images" / "a.jpg").write_bytes(b"image")
            run = project / "Output" / "run1"
            (run / "weights").mkdir(parents=True)
            (run / "weights" / "best.pt").write_bytes(b"fake")
            with patch.object(tc, "is_authenticated", return_value=True), \
                 patch.object(tc, "msa_root_for_project", return_value=msa), \
                 patch.object(tc, "resolve_run_dir", return_value=(run, None)), \
                 patch.object(tc, "load_train_module", return_value=SimpleNamespace(YOLO=FakeModel)), \
                 patch.object(tc, "_read_run_args", return_value={}), \
                 patch.object(tc, "write_msa_task_audit"):
                client = tc.app.test_client()
                response = client.post("/api/project/msa/predict_labels", json={
                    "project": "sample", "run_folder": "run1", "weight": "best.pt",
                })
                self.assertEqual(response.status_code, 200)
                for _ in range(100):
                    status = client.get("/api/project/msa/status").json["label_task"]
                    if status["status"] != "running":
                        break
                    time.sleep(0.01)
                self.assertEqual(status["status"], "success")
                self.assertEqual(status["progress"], 100)
                self.assertEqual(status["generated"], 1)

    def test_prediction_sample_marks_wrong_and_missing_boxes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "MSA"
            result_id = "20260920_120000_000000_abcdef"
            (root / "images").mkdir(parents=True)
            (root / "labels").mkdir()
            prediction_labels = root / "results" / result_id / "prediction" / "labels"
            prediction_labels.mkdir(parents=True)
            (root / "images" / "a.jpg").write_bytes(b"image")
            (root / "labels" / "a.txt").write_text("0 0.5 0.5 0.2 0.2\n", encoding="utf-8")
            (prediction_labels / "a.txt").write_text("1 0.5 0.5 0.2 0.2 0.93\n", encoding="utf-8")
            (prediction_labels.parent.parent / "results.json").write_text(
                json.dumps({"sample_items": [{"image_name": "a.jpg"}], "class_names": {"0": "OK", "1": "NG"}}), encoding="utf-8"
            )
            with patch.object(tc, "is_authenticated", return_value=True), \
                 patch.object(tc, "msa_root_for_project", return_value=root):
                client = tc.app.test_client()
                url = f"/api/project/msa/sample?project=sample&id={result_id}&image=a.jpg"
                response = client.get(url)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json["pred_statuses"], ["wrong_class"])
                self.assertEqual(response.json["missed_gt_indices"], [0])
                self.assertAlmostEqual(response.json["pred_boxes"][0]["confidence"], 0.93)
                (root / "labels" / "a.txt").write_text("1 0.5 0.5 0.2 0.2\n", encoding="utf-8")
                updated = client.get(url)
                self.assertEqual(updated.json["pred_statuses"], ["correct"])
                self.assertEqual(updated.json["missed_gt_indices"], [])

    def test_embedded_editor_uses_only_msa_labels(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "MSA"
            (root / "images").mkdir(parents=True)
            (root / "labels").mkdir()
            (root / "images" / "a.jpg").write_bytes(b"image")
            (root / "labels" / "a.txt").write_text("0 0.5 0.5 0.2 0.2\n", encoding="utf-8")
            with patch.object(tc, "is_authenticated", return_value=True), \
                 patch.object(tc, "msa_root_for_project", return_value=root), \
                 patch.object(tc, "write_audit_log"):
                client = tc.app.test_client()
                images = client.get("/api/project/msa/images?project=sample")
                self.assertEqual(images.json["images"], ["a.jpg"])
                label = client.get("/api/project/msa/label?project=sample&rel=a.jpg")
                self.assertEqual(label.json["bbox_count"], 1)
                saved = client.post("/api/project/msa/label", json={"project": "sample", "rel": "a.jpg", "text": "1 0.5 0.5 0.1 0.1\n"})
                self.assertEqual(saved.status_code, 200)
                self.assertEqual((root / "labels" / "a.txt").read_text(encoding="utf-8"), "1 0.5 0.5 0.1 0.1\n")
                invalid = client.get("/api/project/msa/label?project=sample&rel=../a.jpg")
                self.assertEqual(invalid.status_code, 404)

    def test_upload_api_returns_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("images/a.jpg", b"image")
                archive.writestr("labels/a.txt", "0 0.5 0.5 0.2 0.2\n")
            buffer.seek(0)
            with patch.object(tc, "is_authenticated", return_value=True), \
                 patch.object(tc, "msa_root_for_project", return_value=Path(tmp) / "MSA"), \
                 patch.object(tc, "write_msa_task_audit"):
                response = tc.app.test_client().post(
                    "/api/project/msa/upload",
                    data={"project": "sample", "file": (buffer, "sample.zip")},
                    content_type="multipart/form-data",
                )
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.is_json)
                self.assertTrue(response.json["ok"])
                for _ in range(100):
                    with tc.msa_task_lock:
                        task = dict(tc.MSA_UPLOAD_TASK)
                    if task["status"] != "running":
                        break
                    time.sleep(0.01)
                self.assertEqual(task["status"], "success")
                self.assertEqual(task["progress"], 100)
                self.assertEqual(task["count"], 1)
                status_response = tc.app.test_client().get("/api/project/msa/status")
                self.assertEqual(status_response.status_code, 200)
                self.assertEqual(status_response.json["upload_task"]["progress"], 100)

    def test_reject_zip_path_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("../outside.jpg", b"image")
                archive.writestr("outside.txt", "0 0.5 0.5 0.2 0.2")
            with patch.object(tc, "msa_root_for_project", return_value=Path(tmp) / "MSA"):
                ok, _, _ = tc.import_msa_zip("sample", UploadedZip(buffer.getvalue()))
            self.assertFalse(ok)
            self.assertFalse((Path(tmp) / "outside.jpg").exists())

    def test_same_class_wrong_position_is_error(self):
        gt = [{"cls": 0, "cx": 0.2, "cy": 0.2, "w": 0.1, "h": 0.1}]
        pred = [{"cls": 0, "cx": 0.8, "cy": 0.8, "w": 0.1, "h": 0.1}]
        issues = tc.compare_msa_boxes(gt, pred, {0: "OK"})
        self.assertEqual(issues[0]["missed"], 1)
        self.assertEqual(issues[0]["extra"], 1)

    def test_import_and_compare(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp)
            msa = project / "MSA"
            run = project / "Output" / "run1"
            (run / "weights").mkdir(parents=True)
            (run / "weights" / "best.pt").write_bytes(b"fake")
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("sample/images/a.jpg", b"image")
                archive.writestr("sample/labels/a.txt", "0 0.5 0.5 0.2 0.2\n")
            with patch.object(tc, "msa_root_for_project", return_value=msa):
                ok, _, payload = tc.import_msa_zip("sample", UploadedZip(buffer.getvalue()))
                self.assertTrue(ok)
                self.assertEqual(payload["count"], 1)
                self.assertTrue((msa / "images" / "a.jpg").is_file())
                self.assertTrue((msa / "labels" / "a.txt").is_file())
                with patch.object(tc, "resolve_run_dir", return_value=(run, None)), \
                     patch.object(tc, "load_train_module", return_value=SimpleNamespace(YOLO=FakeModel, cv2=None)), \
                     patch.object(tc, "_read_run_args", return_value={}):
                    progress = []
                    ok, _, result = tc.run_project_msa_test("sample", "run1", "best.pt", progress_cb=lambda *args: progress.append(args))
            self.assertTrue(ok)
            self.assertEqual(progress[-1][0], 100)
            self.assertEqual(result["total_images"], 1)
            self.assertEqual(result["wrong_images"], 1)
            self.assertEqual(result["error_rate"], 100.0)
            self.assertEqual(result["class_errors"]["0"]["missed"], 1)
            self.assertEqual(result["class_errors"]["1"]["extra"], 1)
            self.assertTrue((msa / result["sample_items"][0]["gt_image"]).is_file())
            self.assertTrue((msa / result["sample_items"][0]["pred_image"]).is_file())
            self.assertEqual(json.loads((msa / "results" / result["id"] / "results.json").read_text())["wrong_images"], 1)


if __name__ == "__main__":
    unittest.main()
