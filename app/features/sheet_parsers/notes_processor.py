import pandas as pd

# служебное значение для пустых ячеек
_SERVICE_EMPTY = '__EMPTY__'
ROWS_QUANTITY= 0

class NotesProcessor:
    @staticmethod
    def process_notes(sheet: pd.DataFrame, raw_quantity: int) -> pd.DataFrame:
        # Проверка типа
        if not isinstance(sheet, pd.DataFrame):
            return sheet

        # Если меньше 7 строк - ничего не трогаем
        if sheet.shape[0] <= raw_quantity:
            return sheet

        # Разделяем первые 7 строк (заголовки) и тело
        header_df = sheet.iloc[:raw_quantity].copy()
        # Заполняем в заголовках столбец 'Справочно' значением 'Справочно'
        if 'Справочно' not in header_df.columns:
            header_df['Справочно'] = 'Справочно'
        else:
            header_df['Справочно'] = 'Справочно'

        body_df = sheet.iloc[7:].copy().reset_index(drop=True)

        # Инициализация столбца 'Справочно' служебным значением в теле
        if 'Справочно' not in body_df.columns:
            body_df['Справочно'] = _SERVICE_EMPTY

        # Находим координаты 'Справочно:' только в теле
        coords = [
            (r, c)
            for r in range(body_df.shape[0])
            for c in range(body_df.shape[1])
            if str(body_df.iat[r, c]).strip() == 'Справочно:'
        ]
        if not coords:
            return sheet

        new_rows = []
        max_row, max_col = body_df.shape
        first_col = body_df.columns[0]
        code_col = body_df.columns[1] if body_df.shape[1] > 1 else None

        is_code = lambda e: isinstance(e, str) and e.startswith('(') and e.endswith(')') and e[1:-1].isdigit()
        is_number = lambda e: isinstance(e, str) and any(ch.isdigit() for ch in e.replace(',', '.')) and e.replace(',', '.').replace('.', '', 1).isdigit()

        for row, col in coords:
            prev_label = None
            for dr in range(0, max_row - row):
                r = row + dr
                for c in range(col, max_col):
                    val = body_df.iat[r, c]
                    if pd.isna(val) or val == _SERVICE_EMPTY or str(val).strip() == 'Справочно:':
                        continue
                    entries = [str(x).strip() for x in body_df.iloc[r, c:].tolist() if pd.notna(x) and x != _SERVICE_EMPTY]
                    entries = [e for e in entries if e != 'Справочно:']
                    if not entries:
                        continue
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
                                value = float(e.replace(',', '.'))
                            else:
                                if e != row_label:
                                    unit = e
                        if not row_label or value is None:
                            break
                        row_label = f"{row_label} ({unit})" if unit else row_label
                        row_dict = {col_name: _SERVICE_EMPTY for col_name in body_df.columns}
                        row_dict[first_col] = row_label
                        if code_col:
                            row_dict[code_col] = code or _SERVICE_EMPTY
                        row_dict['Справочно'] = str(value)
                        new_rows.append(row_dict)
                    break

        if new_rows:
            new_df = pd.DataFrame(new_rows, columns=body_df.columns)
            combined_body = pd.concat([body_df, new_df], ignore_index=True)
            # Удаляем строки-метки и пустые
            mask_not_marker = ~combined_body.apply(lambda row: row.astype(str).eq('Справочно:').any(), axis=1)
            mask_label = combined_body[first_col].notna() & (~combined_body[first_col].eq(_SERVICE_EMPTY))
            mask_value = combined_body['Справочно'].notna() & (~combined_body['Справочно'].eq(_SERVICE_EMPTY))
            filtered_body = combined_body.loc[mask_not_marker & (mask_label | mask_value)].reset_index(drop=True)
            # Объединяем заголовки и тело
            sheet = pd.concat([header_df, filtered_body], ignore_index=True)
        return sheet
