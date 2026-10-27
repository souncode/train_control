import ast
import unittest
from pathlib import Path


def load_wait_function(statuses):
    source = Path("TrainControl.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "wait_train_completion_from_monitor"
    )

    class Clock:
        value = 1000.0

        @classmethod
        def time(cls):
            return cls.value

        @classmethod
        def sleep(cls, seconds):
            cls.value += seconds

    responses = iter((True, status) for status in statuses)
    namespace = {
        "Path": Path,
        "time": Clock,
        "socket": None,
        "fetch_train_monitor_json": lambda _path: next(responses),
        "is_current_train_stop_requested": lambda _project: False,
        "find_project_train_pids": lambda _path, include_launcher=False: set(),
        "TRAIN_MONITOR_HOST": "127.0.0.1",
        "TRAIN_MONITOR_PORT": 8008,
    }
    module = ast.Module(body=[function], type_ignores=[])
    exec(compile(module, "TrainControl.py", "exec"), namespace)
    return namespace["wait_train_completion_from_monitor"]


class TrainCompletionTests(unittest.TestCase):
    def setUp(self):
        self.project = "Example project"
        self.project_path = Path("test-project").resolve()

    def status(self, is_training, message):
        return {
            "project_name": self.project,
            "project_dir": str(self.project_path),
            "is_training": is_training,
            "msg": message,
        }

    def test_stale_failure_does_not_override_new_completed_run(self):
        wait = load_wait_function([
            self.status(False, "failed: previous run"),
            self.status(True, "training"),
            self.status(False, "completed"),
        ])

        self.assertEqual(wait(self.project, self.project_path), 0)

    def test_failure_after_training_started_returns_nonzero(self):
        wait = load_wait_function([
            self.status(True, "training"),
            self.status(False, "failed: validation error"),
        ])

        self.assertEqual(wait(self.project, self.project_path), 1)


if __name__ == "__main__":
    unittest.main()
