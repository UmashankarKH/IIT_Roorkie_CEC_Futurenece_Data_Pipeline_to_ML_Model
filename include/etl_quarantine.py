"""Writes rejected rows to include/quarantine/<name>/<file>_failed_<run_id>.csv"""
from include.masking import mask_email, mask_phone
from include.nammamart_config import QUARANTINE_DIR


def write_quarantine(failed, run_id):
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    written = {}
    for name, df in failed.items():
        if df.empty:
            continue
        out = df.copy()
        if name == "customers":          # never write raw personal data outside include/data
            out["full_name"] = "***"
            out["email"] = out["email"].map(mask_email)
            out["phone"] = out["phone"].map(mask_phone)
        path = QUARANTINE_DIR / f"{name}_failed_{run_id}.csv"
        out.to_csv(path, index=False)
        written[name] = (path, len(out))
    return written