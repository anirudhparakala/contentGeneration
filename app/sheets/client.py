from __future__ import annotations

import os
from typing import Any, Protocol, Sequence


SHEETS_SCOPES: tuple[str, ...] = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)


class SheetsClientError(RuntimeError):
    pass


class WorksheetClient(Protocol):
    def fetch_all_values(self) -> list[list[str]]:
        ...

    def update_row(self, *, row_number: int, values: Sequence[str]) -> None:
        ...

    def append_row(self, *, values: Sequence[str]) -> None:
        ...


class SheetsClient(Protocol):
    def open_worksheet(self, *, spreadsheet_id: str, worksheet_name: str) -> WorksheetClient:
        ...


class _GSpreadWorksheet:
    def __init__(self, worksheet: Any) -> None:
        self._worksheet = worksheet

    def fetch_all_values(self) -> list[list[str]]:
        try:
            rows = self._worksheet.get_all_values()
        except Exception as exc:  # pragma: no cover - depends on external SDK exceptions
            raise SheetsClientError("failed reading worksheet rows") from exc
        normalized: list[list[str]] = []
        for row in rows:
            normalized.append([str(cell) if cell is not None else "" for cell in row])
        return normalized

    def update_row(self, *, row_number: int, values: Sequence[str]) -> None:
        try:
            self._worksheet.update(
                f"A{row_number}",
                [list(values)],
                value_input_option="RAW",
            )
        except Exception as exc:  # pragma: no cover - depends on external SDK exceptions
            raise SheetsClientError(f"failed updating worksheet row {row_number}") from exc

    def append_row(self, *, values: Sequence[str]) -> None:
        try:
            self._worksheet.append_row(list(values), value_input_option="RAW")
        except Exception as exc:  # pragma: no cover - depends on external SDK exceptions
            raise SheetsClientError("failed appending worksheet row") from exc


class GoogleSheetsClient:
    def __init__(self, raw_client: Any) -> None:
        self._raw_client = raw_client

    @classmethod
    def from_env(cls) -> "GoogleSheetsClient":
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not isinstance(creds_path, str) or not creds_path.strip():
            raise SheetsClientError("missing GOOGLE_APPLICATION_CREDENTIALS")
        normalized_path = creds_path.strip()

        try:
            import gspread
        except ImportError as exc:
            raise SheetsClientError("missing dependency: gspread") from exc

        try:
            from google.oauth2.service_account import Credentials
        except ImportError as exc:
            raise SheetsClientError("missing dependency: google-auth") from exc

        try:
            credentials = Credentials.from_service_account_file(
                normalized_path,
                scopes=list(SHEETS_SCOPES),
            )
        except Exception as exc:  # pragma: no cover - external credential parser exceptions
            raise SheetsClientError("failed loading service account credentials") from exc

        try:
            client = gspread.authorize(credentials)
        except Exception as exc:  # pragma: no cover - external auth exceptions
            raise SheetsClientError("failed authorizing Google Sheets client") from exc
        return cls(client)

    def open_worksheet(self, *, spreadsheet_id: str, worksheet_name: str) -> WorksheetClient:
        if not isinstance(spreadsheet_id, str) or not spreadsheet_id.strip():
            raise SheetsClientError("spreadsheet_id must be non-empty")
        if not isinstance(worksheet_name, str) or not worksheet_name.strip():
            raise SheetsClientError("worksheet_name must be non-empty")

        try:
            spreadsheet = self._raw_client.open_by_key(spreadsheet_id.strip())
        except Exception as exc:  # pragma: no cover - depends on external SDK exceptions
            raise SheetsClientError("failed opening spreadsheet") from exc
        try:
            worksheet = spreadsheet.worksheet(worksheet_name.strip())
        except Exception as exc:  # pragma: no cover - depends on external SDK exceptions
            raise SheetsClientError("failed opening worksheet") from exc
        return _GSpreadWorksheet(worksheet)

