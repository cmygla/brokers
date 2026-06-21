import time
from json import loads
from typing import (Any)
import httpx
from pydantic import BaseModel
from retrying import retry

from framework.internal.http.mail import MailApi


class RestResponse(httpx.Response):
    def __init__(self, response: httpx.Response, body_type: type[BaseModel] | None = None):
        super().__init__(response.status_code, request=response.request)
        self.__dict__.update(response.__dict__)
        self._body_type = body_type
        self.body_as_object = self._json_as_object(response)

    def _json_as_object(self, resp: httpx.Response) -> Any:
        try:
            if self._body_type:
                return self._body_type(**resp.json())
            else:
                return resp.json()
        except Exception:
            return None


def retry_if_result_none(result):
    """Return True if we should retry (in this case when result is None), False otherwise"""
    return result is None


def retrier(func):
    def wraps(*args, **kwargs):
        resp = None
        count = 1
        while resp is None:
            print(f'Retry:{count}')
            resp = func(*args, **kwargs)
            count += 1
            if count == 5:
                raise AssertionError("Превышено время ожидания ответа")
            if resp:
                return resp
            time.sleep(1)
        return None

    return wraps


class MailApiHelper:
    def __init__(self, mailapi_client: MailApi):
        self.mailapi = mailapi_client

    def get_activation_token_by_login(self, login: str, type_: str = "registration"):
        token = self.get_activation_token(login=login, type_=type_)
        return token

    @retry(stop_max_attempt_number=5, wait_fixed=1000, retry_on_result=retry_if_result_none)
    def get_activation_token(self, login: str, type_: str, limit: str = 20):
        token = None
        params = {
            'limit': limit, }
        response = RestResponse(self.mailapi.get_api_v2_messages(params))
        token_name = "ConfirmationLinkUrl" if type_.lower() == "registration" else "ConfirmationLinkUri"

        for item in response.body_as_object["items"]:
            user_data = loads(item.get('Content').get('Body'))
            user_login = user_data.get("Login")
            if user_login == login:
                confirmation_link = user_data.get(token_name)
                if confirmation_link is not None:
                    token = confirmation_link.split('/')[-1]
                    break
        return token

