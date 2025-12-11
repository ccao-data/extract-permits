import argparse
import openpyxl
import csv

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

        rgb_string = str(rgb).upper()

        # Standard Excel red is ARGB FFFF0000
        if rgb_string.endswith("FF0000"):
            return True

    return False


def format_reviewed_permits_for_upload(file_path: str) -> None:
    wb = openpyxl.load_workbook(file_path, data_only=True)

    # Use only the sheet "PIN Errors"
    ws = wb["PIN Errors"]

    rows_values = list(ws.iter_rows(values_only=True))
    rows_cells = list(ws.iter_rows(values_only=False))

    # Use header exactly as-is
    original_header = [c if c is not None else "" for c in rows_values[0]]

    # Build header index directly
    header_index = {col: i for i, col in enumerate(original_header)}

    # Output file names
    output_file_path = file_path.replace(".xlsx", "_upload.csv")
    no_upload_xlsx_path = file_path.replace(".xlsx", "_no_upload.csv")

    # Precompute index of LLINE within REQUIRED_COLS
    lline_idx = REQUIRED_COLS.index("LLINE")

    # Counter for LLINE values in the upload file
    current_lline = 1

    with open(output_file_path, "w", newline="", encoding="utf-8") as f_out, \
         open(no_upload_xlsx_path, "w", newline="", encoding="utf-8") as f_red:

        writer = csv.writer(f_out)
        red_writer = csv.writer(f_red)

        # Write header to both files
        writer.writerow(REQUIRED_COLS)
        red_writer.writerow(REQUIRED_COLS)

        # Process all data rows
        for row_vals, row_cells in zip(rows_values[1:], rows_cells[1:]):
            row_vals = list(row_vals)

            # Keep only REQUIRED_COLS (values and cells) as the first step
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

            # Route into correct output file
            if row_has_red_text(required_cells):
                # For rows with red text, write as-is (LLINE from sheet or blank)
                red_writer.writerow(new_row)
            else:
                # For valid rows, overwrite LLINE with ascending sequence
                new_row[lline_idx] = current_lline
                writer.writerow(new_row)
                current_lline += 1

    print(f"Created upload CSV (valid rows): {output_file_path}")
    print(f"Created NO-UPLOAD CSV (rows with red text): {no_upload_xlsx_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export PIN Errors sheet into upload and no-upload CSVs."
    )
    parser.add_argument("file_path", help="Path to the Excel file")

    args = parser.parse_args()
    format_reviewed_permits_for_upload(args.file_path)
