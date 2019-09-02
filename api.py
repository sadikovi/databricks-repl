#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#
# MIT License
#
# Copyright (c) 2019 sadikovi
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

VERSION = "0.1.0"

import json
import urllib
import urllib2

X_DATABRICKS_ORG_ID = "X-Databricks-Org-Id"
USER_AGENT = "Databricks Unofficial API"

# Performs url encoding of the dictionary of parameters.
def urlencode(params):
    return urllib.urlencode(params)

# No-op redirect handler
class NoRedirect(urllib2.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, hdrs, newurl):
        self.headers = hdrs
        self.code = code
        self.msg = msg
        return None

# Sends https request with provided headers and JSESSIONID.
def send(https_url, headers={}, data=None, session="", timeout=60, follow_redirect=True):
    req = urllib2.Request(https_url, data=data, headers=headers)
    if session:
        req.add_header("Cookie", "JSESSIONID=%s" % session)
    redirect = None if follow_redirect else NoRedirect()
    try:
        opener = urllib2.build_opener() if follow_redirect else urllib2.build_opener(redirect)
        response = opener.open(req, timeout=timeout)
        code = response.getcode()
        headers = dict([[x.strip() for x in header.split(":", 1)] for header in response.info().headers])
        return (code, headers, response.read())
    except urllib2.URLError as err:
        if redirect and redirect.code:
            return (redirect.code, dict(redirect.headers), redirect.msg)
        raise err

class DatabricksApi(object):
    def __init__(self, uri):
        """
        Creates new Databricks API for deployment URI, e.g. https://dbc-123.cloud.databricks.com.
        """
        self._uri = uri

    def login(self, user, password):
        """
        Login with username ans password.
        Returns active session.
        """
        payload = urlencode({"j_username": user, "j_password": password})
        code, headers, data = send(
            "%s/j_security_check" % self._uri,
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "User-Agent": USER_AGENT
            },
            follow_redirect=False
        )
        # Response is supposed to be 303
        if "set-cookie" in headers:
            cookie = [c for c in headers["set-cookie"].split(";") if c.startswith("JSESSIONID")]
            if cookie:
                return Session(self._uri, cookie[-1].split("JSESSIONID=")[-1].strip())
        raise StandardError("Failed to authenticate")

class Session(object):
    def __init__(self, uri, session_id):
        self._uri = uri
        self._session_id = session_id

    # Lists available workspaces.
    def list_workspaces(self):
        """
        Lists workspaces and returns sessions per workspace.
        """
        code, headers, data = send(
            "%s/workspaces" % self._uri,
            session=self._session_id,
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT}
        )
        return [WorkspaceSession(self, w) for w in json.loads(data)]

class WorkspaceSession(object):
    def __init__(self, session, workspace_json):
        self._session = session
        self._owner = str(workspace_json["owner"])
        self._name = str(workspace_json["name"])
        self._deployment_name = str(workspace_json["deploymentName"])
        self._org_id = workspace_json["orgId"]
        self._needs_confirmation = workspace_json["needsConfirmation"]
        # lazily evaluated config, don't use this variable, call get_config instead.
        self.__config = None

    @property
    def org_id(self):
        """
        Returns organisation/workspace id.
        """
        return self._org_id

    @property
    def owner(self):
        """
        Returns workspace owner.
        """
        return self._owner

    @property
    def name(self):
        """
        Returns workspace name.
        """
        return self._name

    # Returns workspace configuration, including CSRF token.
    @property
    def config(self):
        """
        Returns workspace configuration.
        """
        # This call is quite expensive, so we cache the result.
        if not self.__config:
            code, headers, data = send(
                "%s/config" % self._session._uri,
                session=self._session._session_id,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": USER_AGENT,
                    X_DATABRICKS_ORG_ID: self._org_id
                }
            )
            self.__config = json.loads(data)
        return self.__config
