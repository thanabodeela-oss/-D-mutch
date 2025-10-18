
from datetime import datetime
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
BASELINE_DIR = ROOT / "baseline"
INCOMING_DIR = ROOT / "incoming"
OUT_DIR = ROOT / "out"
DIFFS_DIR = OUT_DIR / "diffs"

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().replace({"nan": "", "None": ""})
    return df

def compare_one(brand_base: Path, brand_new: Path):
    base_df = pd.read_csv(brand_base, dtype=str, keep_default_na=False)
    base_df = _clean_df(base_df)

    new_df = pd.read_excel(brand_new, sheet_name=0, dtype=str)
    new_df = _clean_df(new_df)

    if set(new_df.columns) == set(base_df.columns):
        new_df = new_df[base_df.columns]

    base_rows = set(tuple(r) for r in base_df.to_numpy().tolist())
    new_rows  = set(tuple(r) for r in new_df.to_numpy().tolist())

    added = new_rows - base_rows
    removed = base_rows - new_rows
    unchanged = new_rows & base_rows

    import pandas as pd
    added_df = pd.DataFrame(list(added), columns=base_df.columns)
    removed_df = pd.DataFrame(list(removed), columns=base_df.columns)

    return base_df, new_df, added_df, removed_df, len(unchanged)

def main():
    OUT_DIR.mkdir(exist_ok=True)
    DIFFS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d")
    out_xlsx = OUT_DIR / f"new_changes_{timestamp}.xlsx"

    writer = pd.ExcelWriter(out_xlsx, engine="xlsxwriter")
    summary_rows = []

    xlsx_files = sorted(INCOMING_DIR.glob("*.xlsx"))
    if not xlsx_files:
        print("No .xlsx files found in ./incoming. Nothing to do.")
        return

    for x in xlsx_files:
        brand_name = x.stem
        baseline_path = BASELINE_DIR / f"{brand_name}.csv"
        if not baseline_path.exists():
            print(f"[SKIP] No baseline CSV for {brand_name}: {baseline_path.name}")
            continue

        print(f"[COMPARE] {brand_name}")
        base_df, new_df, added_df, removed_df, unchanged_count = compare_one(baseline_path, x)

        added_df.to_excel(writer, sheet_name=f"{brand_name}__added", index=False)
        removed_df.to_excel(writer, sheet_name=f"{brand_name}__removed", index=False)
        pd.DataFrame([
            {"brand": brand_name, "unchanged_rows": unchanged_count,
             "added_rows": len(added_df), "removed_rows": len(removed_df),
             "incoming_rows": len(new_df), "baseline_rows": len(base_df)}
        ]).to_excel(writer, sheet_name=f"{brand_name}__unchanged_count", index=False)

        brand_dir = DIFFS_DIR / brand_name
        brand_dir.mkdir(parents=True, exist_ok=True)
        added_df.to_csv(brand_dir / "added.csv", index=False)
        removed_df.to_csv(brand_dir / "removed.csv", index=False)

        summary_rows.append({
            "brand": brand_name,
            "added_rows": len(added_df),
            "removed_rows": len(removed_df),
            "unchanged_rows": unchanged_count,
            "incoming_rows": len(new_df),
            "baseline_rows": len(base_df),
        })

    if summary_rows:
        pd.DataFrame(summary_rows).sort_values("brand").to_excel(writer, sheet_name="__SUMMARY__", index=False)

    writer.close()
    print(f"Done. Wrote: {out_xlsx}")
    print(f"Diff CSVs: {DIFFS_DIR}/<BRAND>/added.csv, removed.csv")

if __name__ == "__main__":
    main()
