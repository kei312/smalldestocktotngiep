import ast, sys
with open("/home/naeouad/deproject/dags/dag_daily.py") as f:
    ast.parse(f.read())
print("[OK] dag_daily.py syntax valid")
