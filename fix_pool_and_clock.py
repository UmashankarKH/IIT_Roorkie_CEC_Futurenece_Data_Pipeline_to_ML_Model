import re
from pathlib import Path

POOL = 'pool="duckdb_writer"'
for name in ("dags/nammamart_etl_anikesh.py", "dags/nammamart_elt_anikesh.py"):
    p = Path(name)
    lines = p.read_text().split("\n")
    out, changed = [], 0
    for i, line in enumerate(lines):
        s = line.strip()
        indent = line[: len(line) - len(line.lstrip())]
        if s == "@task":
            nxt = next((l for l in lines[i + 1:] if l.strip()), "")
            m = re.match(r"\s*def (\w+)\(", nxt)
            if m and m.group(1) != "extract":          # extract only reads CSVs: stays parallel
                line, changed = f"{indent}@task({POOL})", changed + 1
        elif s.startswith('@task(trigger_rule="all_done")'):
            line, changed = line.replace('@task(trigger_rule="all_done")', f'@task({POOL}, trigger_rule="all_done")'), changed + 1
        out.append(line)
    p.write_text("\n".join(out))
    print(f"{name}: {changed} tasks now use the duckdb_writer pool")

q = Path("include/elt_dq.py")
t = q.read_text()
n = t.count("pd.Timestamp.now()")
t = t.replace("pd.Timestamp.now()", 'pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)')
q.write_text(t)
print(f"include/elt_dq.py: {n} timestamps now in IST")
