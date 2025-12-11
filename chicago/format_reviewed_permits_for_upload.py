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


def row_has_red_text(cells) -> bool:
    """Return True if any cell in the row contains red font color."""
    for cell in cells:
        if cell is None:
            continue
        color = cell.font.color
        if color is None:
            continue
        rgb = getattr(color, "rgb", None)
        if rgb is None:
            continue
        if str(rgb).upper().endswith("FF0000"):
            return True
    return False


def format_reviewed_permits_for_upload(file_path: str) -> None:
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb["PIN Errors"]

    rows_values = list(ws.iter_rows(values_only=True))
    rows_cells = list(ws.iter_rows(values_only=False))

    original_header = [c if c is not None else "" for c in rows_values[0]]
    header_index = {col: i for i, col in enumerate(original_header)}

    no_upload_path = file_path.replace(".xlsx", "_no_upload.csv")
    red_writer = csv.writer(
        open(no_upload_path, "w", newline="", encoding="utf-8")
    )
    red_writer.writerow(REQUIRED_COLS)

    # Upload batching setup
    batch_size = 250
    batch_number = 1
    rows_in_current_batch = 0

    # Produce file names like:  myfile_upload_1.csv
    def open_new_batch_writer():
        nonlocal batch_number
        batch_path = file_path.replace(".xlsx", f"_upload_{batch_number}.csv")
        f = open(batch_path, "w", newline="", encoding="utf-8")
        writer = csv.writer(f)
        writer.writerow(REQUIRED_COLS)
        print(f"Created upload batch: {batch_path}")
        return writer, f, batch_path

    upload_writer, upload_handle, last_path = open_new_batch_writer()

    lline_idx = REQUIRED_COLS.index("LLINE")
    current_lline = 1

    # Process rows
    for row_vals, row_cells in zip(rows_values[1:], rows_cells[1:]):
        row_vals = list(row_vals)

        new_row = []
        required_cells = []

        for col in REQUIRED_COLS:
            idx = header_index.get(col)
            if idx is not None and idx < len(row_vals):
                val = row_vals[idx]
                new_row.append("" if val is None else val)
                required_cells.append(row_cells[idx])
            else:
                new_row.append("")
                required_cells.append(None)

        # Red rows → no_upload.csv
        if row_has_red_text(required_cells):
            red_writer.writerow(new_row)
            continue

        # Valid rows → upload batch
        new_row[lline_idx] = current_lline
        upload_writer.writerow(new_row)
        current_lline += 1
        rows_in_current_batch += 1

        # Start new batch if needed
        if rows_in_current_batch >= batch_size:
            upload_handle.close()
            batch_number += 1
            rows_in_current_batch = 0
            upload_writer, upload_handle, last_path = open_new_batch_writer()

    upload_handle.close()

    print("\nProcessing complete.")
    print(f"Last upload batch saved to: {last_path}")
    print(f"No-upload CSV saved to: {no_upload_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export PIN Errors sheet into upload and no-upload CSVs."
    )
    parser.add_argument("file_path", help="Path to the Excel file")
    args = parser.parse_args()
    format_reviewed_permits_for_upload(args.file_path)
