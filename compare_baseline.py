# compare & (optional) apply-to-baseline
from datetime import datetime
from pathlib import Path
import argparse
import pandas as pd

ROOT = Path(__file__).resolve().parent
BASELINE_DIR = ROOT / "baseline"
INCOMING_DIR = ROOT / "incoming"
OUT_DIR = ROOT / "out"
DIFFS_DIR = OUT_DIR / "diffs"
BACKUP_DIR = BASELINE_DIR / "_backups"  # เก็บแบ็กอัป baseline

def _ts():
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().replace({"nan": "", "None": ""})
    # เอาแถวซ้ำออก (ทั้งแถว)
    df = df.drop_duplicates()
    return df

def _read_baseline_csv(p: Path) -> pd.DataFrame:
    return pd.read_csv(p, dtype=str, keep_default_na=False, encoding="utf-8-sig")

def _write_csv(df: pd.DataFrame, p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False, encoding="utf-8-sig")

def _backup_baseline(brand_name: str, baseline_path: Path):
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = _ts()
    dst = BACKUP_DIR / f"{brand_name}.{stamp}.csv"
    baseline_path.replace(dst)
    print(f"[BACKUP] {baseline_path.name} -> {_rel(dst)}")
    return dst

def _rel(p: Path) -> str:
    try:
        return p.relative_to(ROOT).as_posix()
    except Exception:
        return p.as_posix()

def compare_one(brand_base: Path, brand_new: Path):
    base_df = _read_baseline_csv(brand_base)
    base_df = _clean_df(base_df)

    new_df = pd.read_excel(brand_new, sheet_name=0, dtype=str)
    new_df = _clean_df(new_df)

    # จัดคอลัมน์ให้เทียบกันได้ (ถ้าเหมือนกันพอดีจะเรียงตาม baseline)
    if set(new_df.columns) == set(base_df.columns):
        new_df = new_df[base_df.columns]

    base_rows = set(tuple(r) for r in base_df.to_numpy().tolist())
    new_rows  = set(tuple(r) for r in new_df.to_numpy().tolist())

    added = new_rows - base_rows
    removed = base_rows - new_rows
    unchanged = new_rows & base_rows

    added_df = pd.DataFrame(list(added), columns=base_df.columns)
    removed_df = pd.DataFrame(list(removed), columns=base_df.columns)

    return base_df, new_df, added_df, removed_df, len(unchanged)

def main():
    parser = argparse.ArgumentParser(description="Compare incoming xlsx vs baseline csv per brand (+ optional apply).")
    parser.add_argument("--apply", choices=["none", "replace", "append"], default="none",
                        help="none=ไม่อัปเดต baseline | replace=เขียนทับ baseline ด้วยไฟล์ใหม่ | append=เพิ่มเฉพาะแถวใหม่")
    parser.add_argument("--incoming-glob", default="*.xlsx", help="แพทเทิร์นไฟล์ใน ./incoming (เช่น *.xlsx)")
    args = parser.parse_args()

    OUT_DIR.mkdir(exist_ok=True)
    DIFFS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d")
    out_xlsx = OUT_DIR / f"new_changes_{timestamp}.xlsx"

    writer = pd.ExcelWriter(out_xlsx, engine="xlsxwriter")
    summary_rows = []

    xlsx_files = sorted(INCOMING_DIR.glob(args.incoming_glob))
    if not xlsx_files:
        print("No files matched in ./incoming. Nothing to do.")
        writer.close()
        return

    print(f"[START] apply={args.apply}")
    for x in xlsx_files:
        brand_name = x.stem
        baseline_path = BASELINE_DIR / f"{brand_name}.csv"
        if not baseline_path.exists():
            print(f"[SKIP] No baseline CSV for {brand_name}: {baseline_path.name}")
            continue

        print(f"[COMPARE] {brand_name}")
        base_df, new_df, added_df, removed_df, unchanged_count = compare_one(baseline_path, x)

        # ---- เขียนรายงานต่อชีต ----
        added_df.to_excel(writer, sheet_name=f"{brand_name}__added", index=False)
        removed_df.to_excel(writer, sheet_name=f"{brand_name}__removed", index=False)
        pd.DataFrame([{
            "brand": brand_name,
            "unchanged_rows": unchanged_count,
            "added_rows": len(added_df),
            "removed_rows": len(removed_df),
            "incoming_rows": len(new_df),
            "baseline_rows": len(base_df)
        }]).to_excel(writer, sheet_name=f"{brand_name}__unchanged_count", index=False)

        # ---- เขียน CSV diff ต่อแบรนด์ ----
        brand_dir = DIFFS_DIR / brand_name
        brand_dir.mkdir(parents=True, exist_ok=True)
        _write_csv(added_df, brand_dir / "added.csv")
        _write_csv(removed_df, brand_dir / "removed.csv")

        # ---- APPLY (อัปเดต baseline) ----
        applied = None
        if args.apply != "none":
            # ทำแบ็กอัปก่อนเสมอ
            backup_path = _backup_baseline(brand_name, baseline_path)
            if args.apply == "replace":
                # เขียน baseline ใหม่เป็น new_df (ใช้คอลัมน์ตาม new_df)
                _write_csv(new_df, baseline_path)
                applied = f"replace -> {baseline_path.name} (from {_rel(x)})"
            elif args.apply == "append":
                # เพิ่มเฉพาะ added_df ต่อท้าย baseline เดิม
                combined = pd.concat([base_df, added_df], ignore_index=True)
                combined = _clean_df(combined).drop_duplicates()
                _write_csv(combined, baseline_path)
                applied = f"append {len(added_df)} row(s) -> {baseline_path.name}"
            print(f"[APPLY] {brand_name}: {applied}")

        # ---- สรุป ----
        summary_rows.append({
            "brand": brand_name,
            "added_rows": len(added_df),
            "removed_rows": len(removed_df),
            "unchanged_rows": unchanged_count,
            "incoming_rows": len(new_df),
            "baseline_rows": len(base_df),
            "applied": applied or "-"
        })

    if summary_rows:
        pd.DataFrame(summary_rows).sort_values("brand").to_excel(writer, sheet_name="__SUMMARY__", index=False)

    writer.close()
    print(f"Done. Wrote report: {_rel(out_xlsx)}")
    print(f"Diff CSVs: {_rel(DIFFS_DIR)}/<BRAND>/added.csv, removed.csv")
    if args.apply != "none":
        print(f"Backups saved in: {_rel(BACKUP_DIR)}")

if __name__ == "__main__":
    main()
