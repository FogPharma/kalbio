"""Service class for handling registration workflows in Kaleidoscope.

This module provides methods for submitting results as part of the
registration process. The typical workflow is:

1. Receive a ``registration_submitted`` webhook event containing a ``file_url``
   and ``record_ids``.
2. Download the file using :meth:`FilesService.download_file`.
3. Process the file externally.
4. Submit results back using :meth:`RegistrationService.submit_results`.

Classes:
    RegistrationService: Service for submitting results in registration
        workflows.

Example:
    ```python
    # Download the file from the webhook event
    path = client.files.download_file(
        file_id="file-uuid",
        filename="original_filename.csv",
        destination="/tmp/downloads",
    )

    # After processing, submit results back
    client.registration.submit_results(
        operation_id="operation-uuid",
        file_id="file-uuid",
        status="success",
        key_field_names=["id", "name"],
        records=[
            {
                "record_id": "record-uuid",
                "status": "matched",
                "data": {"id": "123", "name": "Example"},
            }
        ],
    )
    ```
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Optional

from kalbio.client import KaleidoscopeClient

_logger = logging.getLogger(__name__)


class RegistrationService:
    """Service class for registration workflows.

    Provides methods to submit processing results, supporting the
    registration webhook lifecycle.
    """

    def __init__(self, client: "KaleidoscopeClient"):
        self._client = client

    def submit_results(
        self,
        operation_id: str,
        file_id: str,
        status: Literal["success", "error"],
        error_message: Optional[str] = None,
        key_field_names: Optional[list[str]] = None,
        records: Optional[list[dict[str, Any]]] = None,
    ) -> bool:
        """Submit registration results for a processed file.

        Called after downloading and processing a file from a
        ``registration_submitted`` webhook event.

        Args:
            operation_id (str): The operation ID from the webhook event.
            file_id (str): The file ID from the webhook event.
            status (Literal["success", "error"]): Overall result status.
            error_message (str, optional): Error description when status
                is ``"error"``.
            key_field_names (list[str], optional): Names of key fields in the
                result data (e.g. ``["id", "name"]``).
            records (list[dict[str, Any]], optional): Per-record results. Each
                dict should contain ``record_id`` (str, required),
                ``status`` (str, required), and optionally ``data``
                (dict[str, Any]) and ``error_message`` (str).

        Returns:
            bool: True if the results were submitted successfully (204),
            False on failure.

        Example:
            ```python
            success = client.registration.submit_results(
                operation_id="op-uuid",
                file_id="file-uuid",
                status="success",
                key_field_names=["id"],
                records=[
                    {
                        "record_id": "rec-uuid",
                        "status": "matched",
                        "data": {"id": "123"},
                    }
                ],
            )
            ```
        """
        try:
            payload: dict[str, Any] = {"status": status}
            if error_message is not None:
                payload["error_message"] = error_message
            if key_field_names is not None:
                payload["key_field_names"] = key_field_names
            if records is not None:
                payload["records"] = records

            url = f"/operations/{operation_id}/register/{file_id}/results"
            return self._client._post_no_content(url, payload)
        except Exception as e:
            _logger.error(f"Error submitting results for file {file_id}: {e}")
            return False
