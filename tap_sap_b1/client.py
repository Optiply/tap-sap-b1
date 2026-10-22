"""SAP Business One Service Layer client."""

from __future__ import annotations

import time
from collections.abc import Iterable
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk import RESTStream
from hotglue_singer_sdk.authenticators import APIAuthenticatorBase
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError

#: Records per request when nothing is configured.
DEFAULT_PAGE_SIZE = 500
#: Seconds before a Service Layer request is abandoned.
DEFAULT_REQUEST_TIMEOUT = 300
#: Appended to api_url when it carries no Service Layer path.
DEFAULT_SERVICE_LAYER_PATH = "/b1s/v1"

def page_offset(value: Any | None) -> int:
    """Validate and normalize an OData offset token."""
    try:
        offset = int(value or 0)
    except (TypeError, ValueError) as exc:
        raise FatalAPIError(f"Invalid SAP Business One pagination token: {value!r}") from exc
    if offset < 0:
        raise FatalAPIError(f"Invalid SAP Business One pagination token: {value!r}")
    return offset


def response_records(response: requests.Response) -> list[dict]:
    """Return and validate an SAP OData collection."""
    body = response.json()
    records = body.get("value") if isinstance(body, dict) else None
    if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
        raise FatalAPIError("SAP Business One returned a malformed OData collection")
    return records


def retry_after_seconds(value: str | None, *, maximum: int = 60) -> int:
    """Return a bounded Retry-After delay for seconds or HTTP-date values."""
    if not value:
        return 0
    try:
        delay = int(value)
    except ValueError:
        try:
            delay = int(parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return 0
    return max(0, min(delay, maximum))


class SAPSessionAuthenticator(APIAuthenticatorBase):
    """Authenticate once per stream with SAP's Login endpoint."""

    def __init__(self, stream: SAPB1Stream) -> None:
        super().__init__(stream)
        missing = [
            key
            for key in ("company_db", "username", "password")
            if not self.config.get(key)
        ]
        if missing:
            raise FatalAPIError(f"Missing SAP Business One config: {', '.join(missing)}")
        self._stream = stream
        self._auth_headers = {"Cookie": self._login()}

    def _login(self) -> str:
        login_url = f"{self._stream.url_base}/Login"
        for attempt in range(5):
            try:
                response = self._stream.requests_session.post(
                    login_url,
                    json={
                        "CompanyDB": self.config["company_db"],
                        "UserName": self.config["username"],
                        "Password": self.config["password"],
                    },
                    headers={"Accept": "application/json"},
                    timeout=self._stream.timeout,
                    verify=self._stream.verify,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 4:
                    raise
                time.sleep(2**attempt)
                continue

            if response.status_code in (401, 403):
                raise InvalidCredentialsError("SAP Business One rejected the credentials")
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt == 4:
                    raise RetriableAPIError("SAP Business One login retries exhausted", response)
                delay = retry_after_seconds(response.headers.get("Retry-After")) or 2**attempt
                time.sleep(delay)
                continue
            if response.status_code == 404:
                raise FatalAPIError(
                    f"SAP Business One login failed with HTTP 404 at {login_url}. "
                    "api_url must point at the Service Layer root, for example "
                    "https://host:50000/b1s/v1"
                )
            if response.status_code >= 400:
                raise FatalAPIError(
                    f"SAP Business One login failed with HTTP {response.status_code}"
                )

            try:
                session_id = response.json().get("SessionId")
            except requests.JSONDecodeError as exc:
                raise FatalAPIError("SAP Business One login returned malformed JSON") from exc
            session_id = response.cookies.get("B1SESSION") or session_id
            if not session_id:
                raise FatalAPIError("SAP Business One login returned no session ID")
            cookies = [f"B1SESSION={session_id}"]
            if route_id := response.cookies.get("ROUTEID"):
                cookies.append(f"ROUTEID={route_id}")
            return "; ".join(cookies)

        raise AssertionError("unreachable")


class SAPB1Stream(RESTStream):
    """Base stream for SAP Business One OData collections."""

    records_jsonpath = "$.value[*]"
    default_replication_key: str | None = None
    default_schema: dict[str, Any] = {}
    default_page_size = DEFAULT_PAGE_SIZE
    select_fields: tuple[str, ...] = ()
    fixed_filter: str | None = None
    default_primary_keys: tuple[str, ...] = ()
    _sap_authenticator: SAPSessionAuthenticator | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("schema", self.default_schema)
        super().__init__(*args, **kwargs)
        self.primary_keys = list(self.default_primary_keys)
        if self.default_replication_key is not None:
            self.replication_key = self.default_replication_key

    @property
    def url_base(self) -> str:
        value = self.config.get("api_url", "")
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise FatalAPIError("api_url must be an absolute HTTP(S) Service Layer URL")
        base = value.rstrip("/")
        if "/b1s/" not in f"{urlsplit(base).path}/":
            self.logger.warning(
                "api_url has no Service Layer path; assuming %s. Set api_url to the "
                "Service Layer root (for example https://host:50000/b1s/v1) to pin "
                "the version.",
                DEFAULT_SERVICE_LAYER_PATH,
            )
            base = f"{base}{DEFAULT_SERVICE_LAYER_PATH}"
        return base

    @property
    def timeout(self) -> int:
        value = self.config.get("request_timeout", DEFAULT_REQUEST_TIMEOUT)
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise FatalAPIError(f"Invalid request_timeout: {value!r}") from exc

    @property
    def page_size(self) -> int:
        """Records per request: per-stream config, then global, then the default."""
        per_stream = self.config.get("page_sizes") or {}
        value = None
        if isinstance(per_stream, dict):
            value = per_stream.get(self.name)
        if value is None:
            value = self.config.get("page_size")
        if value is None:
            return self.default_page_size
        try:
            page_size = int(value)
        except (TypeError, ValueError) as exc:
            raise FatalAPIError(f"Invalid page size for {self.name}: {value!r}") from exc
        if page_size < 1:
            raise FatalAPIError(f"Invalid page size for {self.name}: {value!r}")
        return page_size

    @property
    def verify(self) -> bool | str:
        return self.config.get("ca_bundle") or self.config.get("verify_ssl", True)

    @property
    def authenticator(self) -> SAPSessionAuthenticator:
        authenticator = self._sap_authenticator
        if authenticator is None:
            authenticator = SAPSessionAuthenticator(self)
            self._sap_authenticator = authenticator
        return authenticator

    @property
    def http_headers(self) -> dict[str, str]:
        return {"Accept": "application/json", "Prefer": f"odata.maxpagesize={self.page_size}"}

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code in (401, 403):
            raise InvalidCredentialsError("SAP Business One session is unauthorized")
        if response.status_code == 429 or 500 <= response.status_code < 600:
            raise RetriableAPIError(self.response_error_message(response), response)
        super().validate_response(response)

    def get_url_params(
        self, context: dict | None, next_page_token: Any | None
    ) -> dict[str, Any]:
        filters = [self.fixed_filter] if self.fixed_filter else []
        if self.replication_key:
            starting_time = self.get_starting_time(context)
            if starting_time:
                date = starting_time.strftime("%Y-%m-%d")
                filters.append(f"{self.replication_key} ge '{date}'")

        params: dict[str, Any] = {
            "$top": self.page_size,
            "$skip": page_offset(next_page_token) if not isinstance(next_page_token, dict) else 0,
        }
        # U_last_changed is an unindexed UDF; sorting on it makes SAP sort the
        # whole table before returning a page. The primary key is unique, so
        # paging stays stable without it.
        if orderby := ",".join(self.primary_keys or ()):
            params["$orderby"] = orderby
        if self.select_fields:
            params["$select"] = ",".join(self.select_fields)
        if filters:
            params["$filter"] = " and ".join(filters)
        if isinstance(next_page_token, dict):
            params.pop("$skip")
            params.update(next_page_token)
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Any | None
    ) -> int | dict[str, str] | None:
        records = response_records(response)
        body = response.json()
        next_link = body.get("@odata.nextLink") or body.get("odata.nextLink")
        if next_link:
            if not isinstance(next_link, str):
                raise FatalAPIError("SAP Business One returned an invalid OData next link")
            token = dict(parse_qsl(urlsplit(next_link).query, keep_blank_values=True))
            if not token or token == previous_token:
                raise FatalAPIError("SAP Business One pagination did not progress")
            return token
        if len(records) < self.page_size:
            return None
        previous_offset = page_offset(previous_token)
        next_token = previous_offset + len(records)
        if next_token <= previous_offset:
            raise FatalAPIError("SAP Business One pagination did not progress")
        return next_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from response_records(response)

    def _request(
        self, prepared_request: requests.PreparedRequest, context: dict | None
    ) -> requests.Response:
        response = self.requests_session.send(
            prepared_request,
            timeout=self.timeout,
            verify=self.verify,
        )
        if response.status_code == 401:
            self._sap_authenticator = None
            prepared_request.headers.update(self.authenticator.auth_headers)
            response = self.requests_session.send(
                prepared_request,
                timeout=self.timeout,
                verify=self.verify,
            )
        self._write_request_duration_log(
            endpoint=self.path,
            response=response,
            context=context,
            extra_tags=(
                {"url": prepared_request.path_url} if self._LOG_REQUEST_METRIC_URLS else None
            ),
        )
        self.validate_response(response)
        return response
