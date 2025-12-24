from io import BytesIO
import pandas as pd
from fastapi.responses import StreamingResponse

def download_data_as_excel(
    sheets: dict[str, pd.DataFrame],
    filename: str
) -> StreamingResponse:
    """
    sheets: {sheet_name: DataFrame}
    filename: name of the downloaded Excel file
    """

    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)

            sheet = writer.sheets[sheet_name]

            # Auto-adjust column widths
            for column_cells in sheet.columns:
                length = max(len(str(cell.value)) if cell.value else 0 for cell in column_cells)
                sheet.column_dimensions[column_cells[0].column_letter].width = length + 3

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )