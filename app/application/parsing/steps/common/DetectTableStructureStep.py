"""Step: detect table structure and normalize DataFrame bounds for header parsing."""

import logging
import re
from typing import Callable, Optional, Tuple

import pandas as pd

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.domain.parsing import (
    AutoDetectStructureStrategy,
    FixedStructureStrategy,
    TableStructure,
    detect_table_structure,
)

logger = logging.getLogger(__name__)


class DetectTableStructureStep(BaseParsingStep):
    """
    Detects table boundaries: header rows, data start row, vertical header column.

    Modes:
    - auto_detect=True: AutoDetectStructureStrategy (auto forms, e.g. 5FK)
    - fixed params: FixedStructureStrategy (manual forms, e.g. 1FK)
    """

    def __init__(
        self,
        auto_detect: bool = False,
        fixed_header_range: Optional[Tuple[int, int]] = None,
        fixed_vertical_col: Optional[int] = None,
        fixed_data_start_row: Optional[int] = None,
        normalize_fn: Optional[Callable[[str], str]] = None,
    ) -> None:
        self._auto_detect = auto_detect
        self._fixed_header_range = fixed_header_range
        self._fixed_vertical_col = fixed_vertical_col
        self._fixed_data_start_row = fixed_data_start_row
        self._normalize_fn = normalize_fn

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        raw_name = ctx.sheet_model.sheet_fullname
        ctx.sheet_model.sheet_name = (
            self._normalize_fn(raw_name) if self._normalize_fn is not None else raw_name
        )

        if self._auto_detect:
            strategy = AutoDetectStructureStrategy()
        else:
            if self._fixed_header_range is None or self._fixed_data_start_row is None:
                raise CriticalParsingError(
                    "DetectTableStructureStep: fixed structure params are missing. "
                    "Provide fixed_header_range and fixed_data_start_row.",
                    domain="parsing.steps.detect_structure",
                    meta={"sheet_name": ctx.sheet_name},
                )
            strategy = FixedStructureStrategy(
                header_start_row=self._fixed_header_range[0],
                header_end_row=self._fixed_header_range[1],
                data_start_row=self._fixed_data_start_row,
                vertical_header_column=self._fixed_vertical_col or 0,
            )

        try:
            structure = detect_table_structure(
                ctx.raw_dataframe,
                strategy,
                ctx.sheet_name,
            )
        except Exception as e:
            raise CriticalParsingError(
                f"Failed to detect table structure for sheet '{ctx.sheet_name}': {e}",
                domain="parsing.steps.detect_structure",
                meta={"sheet_name": ctx.sheet_name, "error": str(e)},
                show_traceback=True,
            ) from e

        df = ctx.processed_dataframe if ctx.processed_dataframe is not None else ctx.raw_dataframe

        if self._auto_detect:
            df, structure = self._trim_dataframe_by_numbering_row(df, structure, ctx.sheet_name)
        else:
            df = self._trim_dataframe_by_header_zone(df, structure, ctx.sheet_name)

        ctx.processed_dataframe = df
        ctx.table_structure = structure

        logger.debug(
            "Structure for '%s': headers [%d:%d], data from %d, vertical_col=%d, cols_after_trim=%d",
            ctx.sheet_name,
            structure.header_start_row,
            structure.header_end_row,
            structure.data_start_row,
            structure.vertical_header_column,
            df.shape[1],
        )

    @staticmethod
    def _to_positive_int(value: object) -> Optional[int]:
        if pd.isna(value) or value is None or isinstance(value, bool):
            return None

        if isinstance(value, int):
            return value if value > 0 else None

        if isinstance(value, float):
            if value > 0 and value.is_integer():
                return int(value)
            return None

        s = str(value).strip().lower().replace("\xa0", " ")
        if not s:
            return None

        s_compact = s.replace(" ", "")
        if s_compact.isdigit():
            parsed = int(s_compact)
            return parsed if parsed > 0 else None

        if re.fullmatch(r"\d+[\.,]0+", s_compact):
            parsed = int(float(s_compact.replace(",", ".")))
            return parsed if parsed > 0 else None

        return None

    @classmethod
    def _find_1_to_n_run(cls, row: pd.Series) -> Optional[Tuple[int, int, int]]:
        best_start = -1
        best_end = -1
        best_len = 0

        run_start = -1
        expected = 1

        for col_idx, raw_value in enumerate(row.tolist()):
            parsed = cls._to_positive_int(raw_value)

            if parsed == expected:
                if expected == 1:
                    run_start = col_idx
                expected += 1
                continue

            run_len = expected - 1
            if run_len > best_len:
                best_len = run_len
                best_start = run_start
                best_end = col_idx - 1

            if parsed == 1:
                run_start = col_idx
                expected = 2
            else:
                run_start = -1
                expected = 1

        tail_len = expected - 1
        if tail_len > best_len:
            best_len = tail_len
            best_start = run_start
            best_end = len(row) - 1

        if best_len <= 0 or best_start < 0 or best_end < best_start:
            return None

        return best_start, best_end, best_len

    @classmethod
    def _find_numbering_row_and_bounds(
        cls,
        df: pd.DataFrame,
        max_rows_to_check: int = 80,
        min_sequence_len: int = 8,
    ) -> Optional[Tuple[int, int, int, int]]:
        if df.empty:
            return None

        search_rows = min(max_rows_to_check, len(df))
        best: Optional[Tuple[int, int, int, int]] = None

        for row_idx in range(search_rows):
            run = cls._find_1_to_n_run(df.iloc[row_idx])
            if run is None:
                continue

            start_col, end_col, seq_len = run
            if seq_len < min_sequence_len:
                continue

            if best is None:
                best = (row_idx, start_col, end_col, seq_len)
                continue

            _, best_start, _, best_len = best
            if seq_len > best_len or (seq_len == best_len and start_col < best_start):
                best = (row_idx, start_col, end_col, seq_len)

        return best

    @staticmethod
    def _find_header_start_row(
        df: pd.DataFrame,
        header_end_row: int,
        first_col: int,
        last_col: int,
    ) -> int:
        if header_end_row < 0:
            return 0

        best_any: Optional[int] = None
        for row_idx in range(header_end_row + 1):
            row = df.iloc[row_idx, first_col : last_col + 1]
            non_empty_mask = row.notna()
            non_empty_count = int(non_empty_mask.sum())

            if non_empty_count >= 2:
                return row_idx
            if non_empty_count >= 1 and best_any is None:
                best_any = row_idx

        return best_any if best_any is not None else 0

    def _trim_dataframe_by_numbering_row(
        self,
        df: pd.DataFrame,
        structure: TableStructure,
        sheet_name: str,
    ) -> Tuple[pd.DataFrame, TableStructure]:
        numbering = self._find_numbering_row_and_bounds(df)

        if numbering is None:
            trimmed = self._trim_dataframe_by_header_zone(df, structure, sheet_name)
            return trimmed, structure

        numbering_row, first_col, last_col, seq_len = numbering

        if numbering_row <= 0:
            # If numbering is unexpectedly in row 0, keep old row bounds and trim only columns.
            header_start_row = structure.header_start_row
            header_end_row = structure.header_end_row
            data_start_row = structure.data_start_row
        else:
            header_end_row = numbering_row - 1
            header_start_row = self._find_header_start_row(df, header_end_row, first_col, last_col)
            data_start_row = numbering_row + 1

        data_start_row = min(data_start_row, len(df))
        trimmed_df = df.iloc[:, first_col : last_col + 1].copy()

        # After trimming to numbering bounds, column "1" is always index 0.
        vertical_col = 0

        refined_structure = TableStructure(
            header_start_row=header_start_row,
            header_end_row=header_end_row,
            data_start_row=data_start_row,
            vertical_header_column=vertical_col,
        )

        logger.info(
            "Trim by numbering row for '%s': row=%d, sequence=1..%d, columns [%d:%d], new_headers=[%d:%d], new_data_start=%d",
            sheet_name,
            numbering_row,
            seq_len,
            first_col,
            last_col,
            refined_structure.header_start_row,
            refined_structure.header_end_row,
            refined_structure.data_start_row,
        )

        return trimmed_df, refined_structure

    def _trim_dataframe_by_header_zone(
        self,
        df: pd.DataFrame,
        structure: TableStructure,
        sheet_name: str,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        header_start = structure.header_start_row
        header_end = structure.header_end_row
        header_zone = df.iloc[header_start : header_end + 1]
        mask_list = header_zone.notna().any(axis=0).tolist()

        first_valid = -1
        last_valid = -1
        for i, v in enumerate(mask_list):
            if v and first_valid == -1:
                first_valid = i
            if v:
                last_valid = i

        if last_valid == -1:
            logger.warning("No header columns found in sheet '%s'", sheet_name)
            return df

        seen_empty = False
        first_empty = None

        for i in range(first_valid, last_valid + 1):
            v = mask_list[i]
            if not v:
                if not seen_empty:
                    first_empty = i
                seen_empty = True
            if v and seen_empty:
                logger.warning(
                    "Header mask has gaps in '%s' (first_empty=%s, next_non_empty=%d). Continue with best-effort trim.",
                    sheet_name,
                    first_empty,
                    i,
                )
                break

        trimmed_cols = last_valid + 1

        if trimmed_cols < df.shape[1]:
            logger.info(
                "Header-zone trim for '%s': %d -> %d columns",
                sheet_name,
                df.shape[1],
                trimmed_cols,
            )
            return df.iloc[:, :trimmed_cols].copy()

        return df
