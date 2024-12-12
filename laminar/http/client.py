import asyncio
import time
import aiohttp
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, Literal, TypedDict, Unpack, Callable, Union, TYPE_CHECKING

from ..core.exceptions import ErrorThresholdExceeded
from ..core.models import Log

if TYPE_CHECKING:
    from ..core.run import FlowRun, Config

METHOD = Literal["GET", "PUT", "POST", "PATCH", "DELETE"]
RESPONSE_TYPE = Literal["json", "text", "file"]

class RequestKwargs(TypedDict, total=False):
    response_type: RESPONSE_TYPE
    params: Optional[Union[Dict[str, str], Callable]]
    data: Optional[Any]
    json: Optional[Union[Dict[str, Any], Callable]]
    headers: Optional[Union[Dict[str, str], Callable]]
    cookies: Optional[Union[Dict[str, str], Callable]]
    to_file: Optional[str]
    inject: Optional[Dict[str, Any]]
    allow_redirects: Optional[bool]


class HTTPRun(Log):
    """
    A class to log HTTP request details.

    Attributes:
        run_id (str): The ID of the run.
        log_type (str): The type of log, default is "HTTP".
        method (Optional[str]): The HTTP method used.
        status_code (int): The HTTP status code.
        url (Optional[str]): The URL of the request.
    """

    __tablename__ = "logs.http_request"
    retries: int = 0
    run_id: str
    log_type: str = "HTTP"
    method: Optional[METHOD] = None
    status_code: int = None
    task: Optional[str] = None
    url: Optional[str] = None

    def finish(self, status_code: int = None, error_message: str = None):
        """
        Finish the log entry with a status code and optional error message.

        Args:
            status_code (int, optional): The HTTP status code.
            error_message (str, optional): The error message if any.
        """
        self.status_code = status_code
        if status_code is None:
            status = "UNKNOWN"
        elif 200 <= status_code < 300:
            status = "COMPLETED"
        else:
            status = "FAILED"
        return super().finish(status, error_message)

def evaluate_kwargs(run: "FlowRun", **kwargs: Unpack[RequestKwargs]) -> RequestKwargs:
    for key, value in kwargs.items():
        if callable(value):
            kwargs[key] = value(run)
    return kwargs

class HTTPClient:
    """
    A class to handle HTTP requests with rate limiting and logging.

    Attributes:
        log_request_starts (bool): Whether to log the start of requests.
        config (Config): The configuration object.
    """

    log_request_starts: bool = False
    config: "Config"

    def __init__(self, run: "FlowRun"):
        """
        Initialize the HTTPClient.

        Args:
            run (Run): The run object containing configuration and state.
        """
        self.run = run
        self.config = run.config
        self.logger = run.logger
        self._semaphore = asyncio.Semaphore(self.config.http_concurrency_limit)
        self._last_request_time: Optional[float] = None
        self._rate_limit_lock = asyncio.Lock()
        self._session = None

    @asynccontextmanager
    async def get_session(self):
        """
        Get an aiohttp session, creating one if it doesn't exist.
        """
        if self._session is None:
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(verify_ssl=False)
            ) as session:
                self._session = session
                try:
                    yield self._session
                finally:
                    self._session = None
        else:
            yield self._session

    async def _rate_limit(self):
        """
        Ensure that requests respect the rate limit.
        """
        if self.config.http_per_second is not None:
            async with self._rate_limit_lock:
                if self._last_request_time is not None:
                    min_interval = 1 / self.config.http_per_second
                    elapsed = time.monotonic() - self._last_request_time
                    if elapsed < min_interval:
                        wait_time = min_interval - elapsed
                        await asyncio.sleep(wait_time)
                self._last_request_time = time.monotonic()

    async def request(
        self,
        method: METHOD,
        url: str,
        **kwargs: Unpack[RequestKwargs],
    ) -> Any:
        """
        Make an HTTP request.

        Args:
            method (METHOD): The HTTP method to use (e.g., "GET", "POST").
            url (str): The URL to request.
            response_type (str, optional): The expected response type. Defaults to "json".
            params (Optional[Dict[str, str]], optional): The query parameters to include in the request. Defaults to None.
            data (Optional[Any], optional): The data to include in the request body. Defaults to None.
            json (Optional[Dict[str, Any]], optional): The JSON data to include in the request body. Defaults to None.
            headers (Optional[Dict[str, str]], optional): The headers to include in the request. Defaults to None.
            cookies (Optional[Dict[str, str]], optional): The cookies to include in the request. Defaults to None.
            auth (Optional[aiohttp.BasicAuth], optional): The authentication information. Defaults to None.
            allow_redirects (bool, optional): Whether to allow redirects. Defaults to True.
            timeout (Optional[aiohttp.ClientTimeout], optional): The request timeout. Defaults to None.
            **kwargs (Any): Additional arguments to pass to the request method.

        Returns:
            Any: The response data, type depends on response_type.
        """
        async with self._semaphore:
            await self._rate_limit()
            http_log = HTTPRun(
                run_id=self.run.id,
                flow=self.run.flow,
                logger=self.logger,
                method=method,
                url=url,
                write_db=self.run.config.log_db,
                log_init=self.log_request_starts,
            )
            response = None
            retries = self.config.http_retries
            for attempt in range(retries + 1):
                try:
                    async with self.get_session() as session:
                        kwargs = evaluate_kwargs(self.run, **kwargs)
                        response = await session.request(
                            method,
                            url,
                            params=kwargs.get("params"),
                            data=kwargs.get("data"),
                            json=kwargs.get("json"),
                            headers=kwargs.get("headers"),
                            cookies=kwargs.get("cookies"),
                            auth=kwargs.get("auth"),
                            allow_redirects=kwargs.get("allow_redirects", True),
                            timeout=kwargs.get("timeout")
                        )
                    response.raise_for_status()
                    http_log.url = str(response.real_url)
                    http_log.finish(response.status)
                    return await self._handle_response(
                        response,
                        response_type=kwargs.get("response_type", "json"),
                        inject=kwargs.get("inject"),
                        to_file=kwargs.get("to_file")
                    )
                except aiohttp.ClientResponseError as e:
                    status_code = e.status
                    http_log.finish(status_code, error_message=e)
                    self.run.error_increment()
                    if attempt < retries:
                        http_log.retries += 1
                        await asyncio.sleep(
                            self.config.http_retry_backoff * (2**attempt)
                        )
                    else:
                        if self.config.raise_all:
                            raise e
                except ErrorThresholdExceeded as e:
                    http_log.finish(status_code=response.status, error_message=e)
                    raise e
                finally:
                    if not http_log.finished:
                        http_log.finish()
                    await self.run.upsert_log(http_log)

    async def get_offset_paginated(
        self,
        url: str,
        page_size: int = 1000,
        batch_size: int = 5,
        data_path: str = "records",
        limit_param: str = "limit",
        offset_param: str = "offset",
        **kwargs: Unpack[RequestKwargs],
    ) -> List[Any]:
        """
        Get data from a paginated endpoint using offset pagination.

        Args:
            url (str): The URL to request.
            page_size (int, optional): The number of records per page. Defaults to 1000.
            data_path (str, optional): The path to the data in the response. Defaults to "records".
            batch_size (int, optional): The number of requests to send concurrently. Defaults to 5.

        Returns:
            List[Dict[str, Any]]: The aggregated data from all pages.
        """
        all_data = []
        offset = 0
        params = kwargs.get("params", {}).copy()
        params[limit_param] = page_size
        while True:
            offsets = [offset + i * page_size for i in range(batch_size)]
            tasks = [
                self.get(url, params={**params, offset_param: offset_value}, **kwargs)
                for offset_value in offsets
            ]
            responses = await self.run.gather(*tasks)
            batch_data = []
            for response in responses:
                data = response[data_path]
                batch_data.extend(data)
                if len(data) < page_size:
                    return all_data + batch_data
            all_data.extend(batch_data)
            offset += page_size * batch_size

    async def _handle_response(
        self,
        response: aiohttp.ClientResponse,
        response_type: str,
        chunk_size: int = 1024,
        to_file: str = None,
        inject: dict = None,
    ):
        """
        Handle the HTTP response based on the expected response type.

        Args:
            response (aiohttp.ClientResponse): The HTTP response.
            response_type (str): The expected response type.
            chunk_size (int, optional): The chunk size for file downloads. Defaults to 1024.
            to_file (str, optional): The file path to save the response to. Defaults to None.

        Returns:
            Any: The processed response data.
        """
        if response_type == "json":
            data = await response.json()
            if inject:
                if isinstance(data, dict):
                    data.update(inject)
                elif isinstance(data, list):
                    for item in data:
                        item.update(inject)
            return data
        elif response_type == "text":
            return await response.text()
        elif response_type == "file":
            with open(to_file, "wb") as f:
                async for chunk in response.content.iter_chunked(chunk_size):
                    f.write(chunk)
            return to_file
        else:
            raise ValueError(f"Unsupported response type: {response_type}")

    async def get(self, url: str, response_type: str = "json", **kwargs: Unpack[RequestKwargs]) -> Any:
        """
        Send a GET request.

        Args:
            url (str): The URL to request.
            response_type (str, optional): The expected response type. Defaults to "json".
            headers (Optional[Dict[str, str]], optional): The headers to include in the request.
            params (Optional[Dict[str, str]], optional): The query parameters to include in the request.
            cookies (Optional[Dict[str, str]], optional): The cookies to include in the request.
            auth (Optional[aiohttp.BasicAuth], optional): The authentication information.
            allow_redirects (bool, optional): Whether to allow redirects. Defaults to True.
            timeout (Optional[aiohttp.ClientTimeout], optional): The request timeout.
            **kwargs: Additional arguments to pass to the request method.

        Returns:
            Any: The response data, type depends on response_type.
        """
        return await self.request("GET", url, response_type=response_type, **kwargs)

    async def post(self, url: str, response_type: str = "json", **kwargs: Unpack[RequestKwargs]) -> Any:
        """
        Send a POST request.

        Args:
            url (str): The URL to request.
            response_type (str, optional): The expected response type. Defaults to "json".
            headers (Optional[Dict[str, str]], optional): The headers to include in the request.
            data (Optional[Any], optional): The data to include in the request body.
            json (Optional[Dict[str, Any]], optional): The JSON data to include in the request body.
            cookies (Optional[Dict[str, str]], optional): The cookies to include in the request.
            auth (Optional[aiohttp.BasicAuth], optional): The authentication information.
            allow_redirects (bool, optional): Whether to allow redirects. Defaults to True.
            timeout (Optional[aiohttp.ClientTimeout], optional): The request timeout.
            **kwargs: Additional arguments to pass to the request method.

        Returns:
            Any: The response data, type depends on response_type.
        """
        return await self.request("POST", url, response_type=response_type, **kwargs)

    async def put(self, url: str, response_type: str = "json", **kwargs: Unpack[RequestKwargs]) -> Any:
        """
        Send a PUT request.

        Args:
            url (str): The URL to request.
            response_type (str, optional): The expected response type. Defaults to "json".
            headers (Optional[Dict[str, str]], optional): The headers to include in the request.
            data (Optional[Any], optional): The data to include in the request body.
            json (Optional[Dict[str, Any]], optional): The JSON data to include in the request body.
            cookies (Optional[Dict[str, str]], optional): The cookies to include in the request.
            auth (Optional[aiohttp.BasicAuth], optional): The authentication information.
            allow_redirects (bool, optional): Whether to allow redirects. Defaults to True.
            timeout (Optional[aiohttp.ClientTimeout], optional): The request timeout.
            **kwargs: Additional arguments to pass to the request method.

        Returns:
            Any: The response data, type depends on response_type.
        """
        return await self.request("PUT", url, response_type=response_type, **kwargs)

    async def delete(self, url: str, response_type: str = "json", **kwargs: Unpack[RequestKwargs]) -> Any:
        """
        Send a DELETE request.

        Args:
            url (str): The URL to request.
            response_type (str, optional): The expected response type. Defaults to "json".
            headers (Optional[Dict[str, str]], optional): The headers to include in the request.
            cookies (Optional[Dict[str, str]], optional): The cookies to include in the request.
            auth (Optional[aiohttp.BasicAuth], optional): The authentication information.
            allow_redirects (bool, optional): Whether to allow redirects. Defaults to True.
            timeout (Optional[aiohttp.ClientTimeout], optional): The request timeout.
            **kwargs: Additional arguments to pass to the request method.

        Returns:
            Any: The response data, type depends on response_type.
        """
        return await self.request("DELETE", url, response_type=response_type, **kwargs)
    

