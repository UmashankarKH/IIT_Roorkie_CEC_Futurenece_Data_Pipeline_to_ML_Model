from pathlib import Path

p = Path("exp_report.py")
t = p.read_text()
old = """        rows.append(dict(file=f, rows_in=n_in, rows_set_aside=bad,
                         rows_passed=(n_in - bad) if (n_in is not None and bad is not None) else None))"""
new = """        stg_total = int(one(f"select count(*) from stg_{f}")[0]) if (dag == ELT and table_rows(f"stg_{f}") != "missing") else None
        total = stg_total if dag == ELT else n_in
        row = dict(file=f, rows_in=n_in, rows_set_aside=bad,
                   rows_passed=(total - bad) if (total is not None and bad is not None) else None)
        if dag == ELT:
            row["rows_in_staging"] = stg_total
        rows.append(row)"""
if "rows_in_staging" in t:
    print("report already patched")
else:
    assert old in t, "pattern not found"
    t = t.replace(old, new)
    t = t.replace("""    print(pd.DataFrame(rows).to_string(index=False))""",
                  """    print(pd.DataFrame(rows).to_string(index=False))
    if dag == ELT:
        print("  note: ELT rows_passed = rows in staging minus rejected; staging holds the deduplicated base file plus the increment")""")
    p.write_text(t)
    print("report patched")
