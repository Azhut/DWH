"""Tests for filename parsing: extension, unique 4-digit year, subject (reporter)."""

import pytest

from app.core.exceptions import FileValidationError
from app.domain.file.service import (
    FileService,
    validate_and_extract_metadata_from_filename,
)


def test_valid_standard_shape_subject_space_year_and_suffix():
    info = validate_and_extract_metadata_from_filename("АЛАПАЕВСК 2020 REP.xls")
    assert info.reporter == "АЛАПАЕВСК REP"
    assert info.year == 2020
    assert info.extension == "xls"


def test_valid_year_without_space_before_suffix():
    info = validate_and_extract_metadata_from_filename("SUBJ2020.xlsx")
    assert info.reporter == "SUBJ"
    assert info.year == 2020
    assert info.extension == "xlsx"


def test_valid_multiple_dots_in_stem():
    info = validate_and_extract_metadata_from_filename("my.file 2020 name.xlsm")
    assert info.reporter == "MY.FILE NAME"
    assert info.year == 2020
    assert info.extension == "xlsm"


def test_extension_is_after_last_dot_uppercase_normalized():
    info = validate_and_extract_metadata_from_filename("TOWN 2025.XLSX")
    assert info.reporter == "TOWN"
    assert info.year == 2025
    assert info.extension == "xlsx"


def test_whitespace_normalized_in_reporter():
    info = validate_and_extract_metadata_from_filename("A   B  1999 C.xlsx")
    assert info.reporter == "A B C"
    assert info.year == 1999


def test_no_dot_raises():
    with pytest.raises(FileValidationError) as exc:
        validate_and_extract_metadata_from_filename("NOEXTENSION")
    assert "extension" in exc.value.message.lower() or "." in exc.value.message


def test_empty_extension_raises():
    with pytest.raises(FileValidationError) as exc:
        validate_and_extract_metadata_from_filename("file.")
    assert "empty extension" in exc.value.message.lower()


def test_disallowed_extension_raises():
    with pytest.raises(FileValidationError) as exc:
        validate_and_extract_metadata_from_filename("X 2020.txt")
    assert "extension" in exc.value.message.lower()


def test_no_four_digit_year_raises():
    with pytest.raises(FileValidationError) as exc:
        validate_and_extract_metadata_from_filename("ONLYNAME.xlsx")
    assert "year" in exc.value.message.lower()


def test_multiple_four_digit_runs_raises():
    with pytest.raises(FileValidationError) as exc:
        validate_and_extract_metadata_from_filename("CITY 2020 OLD 1999.xlsx")
    assert "only one" in exc.value.message.lower()


def test_year_duplicated_non_overlapping_raises():
    with pytest.raises(FileValidationError):
        validate_and_extract_metadata_from_filename("X 20202020 Y.xlsx")


def test_only_year_in_stem_empty_subject_raises():
    with pytest.raises(FileValidationError) as exc:
        validate_and_extract_metadata_from_filename("2020.xlsx")
    assert "empty" in exc.value.message.lower() or "cannot be empty" in exc.value.message.lower()


def test_year_out_of_range_raises():
    with pytest.raises(FileValidationError) as exc:
        validate_and_extract_metadata_from_filename("T 1899.xlsx")
    assert "year" in exc.value.message.lower()


def test_empty_filename_raises():
    with pytest.raises(FileValidationError):
        validate_and_extract_metadata_from_filename("")


def test_file_service_delegates_same_as_module_function():
    class _Repo:
        pass

    svc = FileService(_Repo())
    a = validate_and_extract_metadata_from_filename("Q 2001.xlsx")
    b = svc.validate_and_extract_metadata_from_filename("Q 2001.xlsx")
    assert a == b


def test_parsing_does_not_depend_on_form_no_extra_parameters():
    """There is no form_id branch in filename parsing; same rules always apply."""
    import inspect

    sig = inspect.signature(validate_and_extract_metadata_from_filename)
    assert list(sig.parameters) == ["filename"]
