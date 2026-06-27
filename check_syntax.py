import ast
import sys

try:
    with open("TrainControl.py", "r", encoding="utf-8") as f:
        code = f.read()
    ast.parse(code)
    print("Syntax OK")
except SyntaxError as e:
    print(f"SyntaxError on line {e.lineno}: {e.msg}")
    print(f"Text: {e.text}")
    print(f"Offset: {e.offset}")
    sys.exit(1)
