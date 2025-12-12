import argparse
import csv

import openpyxl

# Ordered column output for final CSV upload
REQUIRED_COLS = [
    "LLINE",
    "PIN* [PARID]",
    "Local Permit No.* [USER28]",
    "Issue Date* [PERMDT]",
    "Desc 1* [DESC1]",
    "Desc 2 Code 1 [USER6]",
    "Desc 2 Code 2 [USER7]",
    "Desc 2 Code 3 [USER8]",
    "Amount* [AMOUNT]",
    "Assessable [IS_ASSESS]",
    "Applicant Street Address* [ADDR1]",
    "Applicant Address 2 [ADDR2]",
    "Applicant City, State, Zip* [ADDR3]",
    "Contact Phone* [PHONE]",
    "Applicant* [USER21]",
    "Notes [NOTE1]",
    "Occupy Dt [UDATE1]",
    "Submit Dt* [CERTDATE]",
    "Est Comp Dt [UDATE2]",
]

FLAG_FILL_COLORS = {
    "FFFFFF00",  # yellow (ARGB)
    "FFFFC000",  # orange (ARGB)
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

    # 2) Theme-based fills (there is one color which is theme-based)
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
            if isinstance(t, tuple)
            and len(t) == 3
            and isinstance(t[2], float)
            else t
            for t in FLAG_FILL_COLORS
        }

        if ("theme", theme, tint) in normalized_flag_colors:
            return True

    return False


def format_reviewed_permits_for_upload(file_path: str) -> None:
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb["PIN Errors"]

    rows_values = list(ws.iter_rows(values_only=True))
    rows_cells = list(ws.iter_rows(values_only=False))

    # Normalize header row
    original_header = [c if c is not None else "" for c in rows_values[0]]
    header_index = {col: i for i, col in enumerate(original_header)}
    pin_idx = header_index.get("PIN* [PARID]")

    # Store file which doesn't match flag colors
    no_upload_path = file_path.replace(".xlsx", "_no_upload.csv")
    f_no = open(no_upload_path, "w", newline="", encoding="utf-8")
    no_writer = csv.writer(f_no)
    no_writer.writerow(REQUIRED_COLS)

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

    lline_idx = REQUIRED_COLS.index("LLINE")

    # Process each row
    for row_vals, row_cells in zip(rows_values[1:], rows_cells[1:]):
        row_vals = list(row_vals)

        # Build row with REQUIRED_COLS only
        new_row = []
        for col in REQUIRED_COLS:
            idx = header_index.get(col)
            if idx is not None and idx < len(row_vals):
                val = row_vals[idx]
                new_row.append("" if val is None else val)
            else:
                new_row.append("")

        # Extract PIN background cell
        pin_cell = None
        if pin_idx is not None and pin_idx < len(row_cells):
            pin_cell = row_cells[pin_idx]

        # UPLOAD if background matches; NO_UPLOAD if it doesn't
        if pin_cell_matches_flag(pin_cell):
            new_row[lline_idx] = current_lline
            upload_writer.writerow(new_row)
            current_lline += 1
            rows_in_batch += 1

            if rows_in_batch >= batch_size:
                upload_handle.close()
                batch_number += 1
                rows_in_batch = 0
                upload_writer, upload_handle, last_upload_path = (
                    new_upload_batch()
                )
        else:
            no_writer.writerow(new_row)

    upload_handle.close()
    f_no.close()

    print("\nProcessing complete.")
    print(f"Upload batches created. Last batch: {last_upload_path}")
    print(f"No-upload CSV saved to: {no_upload_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export PIN Errors sheet into upload and no-upload CSVs."
    )
    parser.add_argument("file_path", help="Path to the Excel file")
    args = parser.parse_args()
    format_reviewed_permits_for_upload(args.file_path)
