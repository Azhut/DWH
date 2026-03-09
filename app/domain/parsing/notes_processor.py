import pandas as pd
import numpy as np

# Служебное значение для пустых ячеек
_SERVICE_EMPTY = "__EMPTY__"
ROWS_QUANTITY = 0


class NotesProcessor:
    @staticmethod
    def process_notes(sheet: pd.DataFrame, raw_quantity: int) -> pd.DataFrame:

        if not isinstance(sheet, pd.DataFrame):
            return sheet

        if sheet.shape[0] <= raw_quantity:
            return sheet

        header_df = sheet.iloc[:raw_quantity].copy()

        if "Справочно" not in header_df.columns:
            header_df["Справочно"] = "Справочно"
        else:
            header_df["Справочно"] = "Справочно"

        # исправлено: раньше было sheet.iloc[7:]
        body_df = sheet.iloc[raw_quantity:].copy().reset_index(drop=True)

        if "Справочно" not in body_df.columns:
            body_df["Справочно"] = _SERVICE_EMPTY

        # переводим dataframe в numpy для ускорения
        data = body_df.to_numpy(dtype=object)

        # быстрый поиск координат "Справочно:"
        coords = np.argwhere(data == "Справочно:")

        if len(coords) == 0:
            return sheet

        new_rows = []

        max_row, max_col = data.shape

        first_col = body_df.columns[0]
        code_col = body_df.columns[1] if body_df.shape[1] > 1 else None

        def is_code(e):
            return isinstance(e, str) and e.startswith("(") and e.endswith(")") and e[1:-1].isdigit()

        def is_number(e):
            if not isinstance(e, str):
                return False
            e = e.replace(",", ".")
            return any(ch.isdigit() for ch in e) and e.replace(".", "", 1).isdigit()

        for row, col in coords:

            prev_label = None

            for dr in range(0, max_row - row):

                r = row + dr

                for c in range(col, max_col):

                    val = data[r, c]

                    if pd.isna(val) or val == _SERVICE_EMPTY or str(val).strip() == "Справочно:":
                        continue

                    entries = [
                        str(x).strip()
                        for x in data[r, c:]
                        if x is not None and x != _SERVICE_EMPTY and not pd.isna(x)
                    ]

                    entries = [e for e in entries if e != "Справочно:"]

                    if not entries:
                        continue

                    # строка только с текстом → это label
                    if all(not (is_code(e) or is_number(e)) for e in entries):
                        prev_label = entries[0]

                    else:

                        code = None
                        value = None
                        unit = None

                        if entries and not (is_code(entries[0]) or is_number(entries[0])):
                            row_label = entries[0]
                        else:
                            row_label = prev_label

                        for e in entries:

                            if is_code(e):
                                code = e

                            elif is_number(e):
                                value = float(e.replace(",", "."))

                            else:
                                if e != row_label:
                                    unit = e

                        if not row_label:
                            continue

                        # исправление: если значение отсутствует
                        if value is None:
                            value = 0.0

                        row_label = f"{row_label} ({unit})" if unit else row_label

                        row_dict = {col_name: _SERVICE_EMPTY for col_name in body_df.columns}

                        row_dict[first_col] = row_label

                        if code_col:
                            row_dict[code_col] = code or _SERVICE_EMPTY

                        row_dict["Справочно"] = str(value)

                        new_rows.append(row_dict)

                    break

        if new_rows:

            new_df = pd.DataFrame(new_rows, columns=body_df.columns)

            combined_body = pd.concat([body_df, new_df], ignore_index=True)

            # ускоренная фильтрация
            mask_not_marker = ~combined_body.eq("Справочно:").any(axis=1)

            mask_label = combined_body[first_col].notna() & (~combined_body[first_col].eq(_SERVICE_EMPTY))

            mask_value = combined_body["Справочно"].notna() & (~combined_body["Справочно"].eq(_SERVICE_EMPTY))

            filtered_body = combined_body.loc[
                mask_not_marker & (mask_label | mask_value)
            ].reset_index(drop=True)

            sheet = pd.concat([header_df, filtered_body], ignore_index=True)

        return sheet