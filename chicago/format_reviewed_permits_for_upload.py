import argparse
import csv

import openpyxl
import pandas as pd

# Helper function that can be factored out for other scripts
from helper import (
    REQUIRED_COLS,
    filled_columns,
    finalize_columns,
    normalize_pin,
)

FLAG_FILL_COLORS = {
    "FFFFFF00",  # yellow (ARGB)
    "FFFFC000",  # orange (ARGB)
    # For some reason, one color is not recognized with the hex, but
    # only with the theme value.
    ("theme", 7, 0.3999755851924192),
}


def pin_cell_matches_flag(pin_cell) -> bool:
    """Return True if the PIN cell has a background color in FLAG_FILL_COLORS."""
    if pin_cell is None:
        return False

    fg = getattr(pin_cell.fill, "fgColor", None)
    if fg is None:
        return False

    #  RGB Fills
    rgb = getattr(fg, "rgb", None)
    if rgb:
        val = str(rgb).upper().lstrip("#")
        if val in FLAG_FILL_COLORS:
            return True
        if len(val) == 8 and val[2:] in FLAG_FILL_COLORS:
            return True

    # Theme-based fills (there is one color which is theme-based)
    if getattr(fg, "type", None) == "theme":
        theme = getattr(fg, "theme", None)
        tint = getattr(fg, "tint", None)

        # normalize tint to avoid float precision issues
        if isinstance(tint, float):
            tint = round(tint, 6)

        # also normalize the tuples stored in FLAG_FILL_COLORS
        # Tried without these steps and it wasn't caught.
        normalized_flag_colors = {
            (
                t[0],
                t[1],
                round(t[2], 6),
            )
            if isinstance(t, tuple) and len(t) == 3 and isinstance(t[2], float)
            else t
            for t in FLAG_FILL_COLORS
        }

        if ("theme", theme, tint) in normalized_flag_colors:
            return True

    return False


def remove_flagged_rows_from_original_xlsx(file_path: str) -> str:
    """
    Remove any rows in the 'PIN Errors' sheet whose 'PIN* [PARID]' cell fill color
    matches FLAG_FILL_COLORS, and save a copy of the workbook preserving formatting.
    """
    wb = openpyxl.load_workbook(file_path, data_only=False)
    ws = wb["PIN Errors"]

    # Read header row directly from worksheet cells
    header_cells = next(ws.iter_rows(min_row=1, max_row=1, values_only=False))
    original_header = [
        c.value if c.value is not None else "" for c in header_cells
    ]
    header_index = {col: i for i, col in enumerate(original_header)}
    pin_idx = header_index.get("PIN* [PARID]")

    # Determine Excel row numbers to delete
    rows_to_delete = []
    for r in range(2, ws.max_row + 1):
        pin_cell = ws.cell(row=r, column=pin_idx + 1)
        if pin_cell_matches_flag(pin_cell):
            rows_to_delete.append(r)

    # Delete bottom-up so row indices don't shift
    for r in reversed(rows_to_delete):
        ws.delete_rows(r, 1)

    out_path = file_path.replace(".xlsx", "_flagged_rows_removed.xlsx")
    wb.save(out_path)
    return out_path


def format_reviewed_permits_for_upload(file_path: str) -> None:
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb["PIN Errors"]

    rows_values = list(ws.iter_rows(values_only=True))
    rows_cells = list(ws.iter_rows(values_only=False))

    # Normalize header row
    original_header = [c if c is not None else "" for c in rows_values[0]]
    header_index = {col: i for i, col in enumerate(original_header)}
    pin_idx = header_index.get("PIN* [PARID]")

    # Upload batching setup
    batch_size = 250
    batch_number = 1
    rows_in_batch = 0
    current_lline = 1

    def new_upload_batch():
        nonlocal batch_number, current_lline
        upload_path = file_path.replace(".xlsx", f"_upload_{batch_number}.csv")
        f = open(upload_path, "w", newline="", encoding="utf-8")
        w = csv.writer(f)
        w.writerow(REQUIRED_COLS)
        current_lline = 1
        print(f"Created upload batch: {upload_path}")
        return w, f, upload_path

    upload_writer, upload_handle, last_upload_path = new_upload_batch()

    # Only keep rows where the PIN cell background matches the flag colors
    flagged_rows = []

    for row_vals, row_cells in zip(rows_values[1:], rows_cells[1:]):
        row_vals = list(row_vals)

        # Extract PIN background cell
        pin_cell = None
        if pin_idx is not None and pin_idx < len(row_cells):
            pin_cell = row_cells[pin_idx]

        if not pin_cell_matches_flag(pin_cell):
            continue

        # Build row with REQUIRED_COLS only
        new_row = {}
        for col in REQUIRED_COLS:
            if col == "LLINE":
                continue
            idx = header_index.get(col)
            if idx is not None and idx < len(row_vals):
                val = row_vals[idx]
                new_row[col] = "" if val is None else val
            else:
                new_row[col] = ""

        # Normalize PIN for upload processing
        if "PIN* [PARID]" in new_row:
            new_row["PIN* [PARID]"] = normalize_pin(
                str(new_row["PIN* [PARID]"])
            )

        flagged_rows.append(new_row)

    df_flagged_only = pd.DataFrame(flagged_rows)

    out = finalize_columns(df_flagged_only, filled_columns)
    upload_df = out["upload"].copy()
    need_review_df = out["need_review"].copy()

    # Write need_review as a single CSV
    need_review_path = file_path.replace(".xlsx", "_need_review.csv")
    need_review_df = need_review_df.reindex(columns=REQUIRED_COLS)
    need_review_df["LLINE"] = range(1, len(need_review_df) + 1)
    need_review_df.to_csv(need_review_path, index=False, encoding="utf-8")
    print(f"Need-review CSV saved to: {need_review_path}")
    print(f"Total need-review rows written: {len(need_review_df)}")

    # Write upload rows in batches, with LLINE reset per batch
    for start in range(0, len(upload_df), batch_size):
        batch = upload_df.iloc[start : start + batch_size].copy()
        batch["LLINE"] = range(1, len(batch) + 1)

        # Ensure column order
        batch = batch.reindex(columns=REQUIRED_COLS)

        for row in batch.itertuples(index=False, name=None):
            upload_writer.writerow(list(row))

        rows_in_batch += len(batch)

        if start + batch_size < len(upload_df):
            upload_handle.close()
            batch_number += 1
            rows_in_batch = 0
            upload_writer, upload_handle, last_upload_path = new_upload_batch()

    upload_handle.close()

    # Remove flagged rows from original XLSX and save copy for re-review
    removed_path = remove_flagged_rows_from_original_xlsx(file_path)
    print(f"Workbook with flagged rows removed saved to: {removed_path}")

    print("\nProcessing complete.")
    print(f"Upload batches created. Last batch: {last_upload_path}")
    print(f"Total upload rows written: {len(upload_df)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export flagged PIN Errors rows (only) into upload CSV batches, and save an XLSX copy with flagged rows removed."
    )
    parser.add_argument("file_path", help="Path to the Excel file")
    args = parser.parse_args()
    format_reviewed_permits_for_upload(args.file_path)
