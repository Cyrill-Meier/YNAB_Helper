// fetch wrapper for the JSON API (DESIGN §9.4; F62, F24).
//
// * Sends the session's CSRF token (<meta name="csrf-token">) as X-CSRF-Token on EVERY
//   request: POSTs need it, and so do the GETs that sync YNAB (/api/reconcile/preview,
//   /api/dedupe/scan).
// * Errors become ApiError with a readable message: the server's {error, message}, or a
//   FastAPI-style `detail` (string or validation list), or a default per status — never the
//   empty statusText of HTTP/2.
// * 401 → the session is gone: go to the login page (/?expired=1) once.
//   403 csrf_failed → the page is out of date (signed in again elsewhere): offer a reload.
//   Both are marked `handled` so callers do not toast them again.
// * Requests take an AbortSignal; an aborted request rejects with the AbortError (see
//   isAbort), which callers ignore (js/core/loader.js drops stale responses, F63).

import { toast } from "../components/toast.js";

const CSRF_TOKEN = document.querySelector('meta[name="csrf-token"]')?.getAttribute("content") || "";

const DEFAULT_MESSAGES = {
  0: "Network error — check your connection and try again.",
  400: "The request was not accepted.",
  401: "Your session has expired. Send /login to the bot for a new sign-in link.",
  403: "This action is not allowed.",
  404: "Not found.",
  405: "Not allowed.",
  409: "Another operation is running or the data changed — try again.",
  411: "The upload needs a known size.",
  413: "The request is too large.",
  415: "Unsupported file type.",
  429: "Too many requests — wait a moment and try again.",
  500: "Something went wrong on the server — try again later.",
  502: "YNAB could not be reached — try again later.",
  503: "The service is unavailable — try again later.",
  504: "The server took too long to answer — try again later.",
};

let redirecting = false;

/** An API failure: HTTP `status` (0 = network), server `error` code, readable `message`. */
export class ApiError extends Error {
  constructor({ status, error, message, data = null, retryAfter = null }) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.error = error;
    this.data = data;
    this.retryAfter = retryAfter;
    /** True when the wrapper already told the user (session expired, stale page). */
    this.handled = false;
  }
}

/** True for the rejection of an aborted request. */
export function isAbort(err) {
  return Boolean(err) && err.name === "AbortError";
}

function defaultMessage(status) {
  if (status in DEFAULT_MESSAGES) return DEFAULT_MESSAGES[status];
  if (status >= 500) return DEFAULT_MESSAGES[500];
  return `Request failed (HTTP ${status}).`;
}

/** The message to show for an error response body (FastAPI `detail` normalised). */
export function messageOf(status, data) {
  if (data && typeof data === "object") {
    if (typeof data.message === "string" && data.message.trim()) return data.message.trim();
    const { detail } = data;
    if (typeof detail === "string" && detail.trim()) return detail.trim();
    if (Array.isArray(detail)) {
      const parts = detail
        .map((item) => (item && typeof item.msg === "string" ? item.msg : null))
        .filter(Boolean);
      if (parts.length) return parts.join("; ");
    }
  }
  return defaultMessage(status);
}

function retryAfterOf(response, data) {
  if (data && typeof data.retry_after === "number" && Number.isFinite(data.retry_after)) {
    return Math.max(0, data.retry_after);
  }
  const header = response.headers.get("retry-after");
  if (header && /^\d+$/.test(header.trim())) return Number(header.trim());
  return null;
}

async function readBody(response) {
  const type = response.headers.get("content-type") || "";
  if (type.includes("application/json")) {
    try {
      return await response.json();
    } catch {
      return null;
    }
  }
  try {
    await response.text(); // drain; an HTML error page from a proxy is never shown
  } catch {
    // ignore
  }
  return null;
}

function goToLogin() {
  if (redirecting) return;
  redirecting = true;
  window.location.assign("/?expired=1");
}

function offerReload(message) {
  toast("warn", message || "This page is out of date. Reload it and try again.", {
    action: { label: "Reload", onClick: () => window.location.reload() },
    ttl: 0,
  });
}

/**
 * Send a request to the API. `json` is sent as an application/json body; `body` is sent
 * as is (with your Content-Type header). Resolves with the parsed JSON body.
 */
export async function request(method, url, { json, body, headers = {}, signal } = {}) {
  const init = {
    method,
    credentials: "same-origin",
    cache: "no-store",
    headers: { Accept: "application/json", "X-CSRF-Token": CSRF_TOKEN, ...headers },
    signal,
  };
  if (json !== undefined) {
    init.headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(json);
  } else if (body !== undefined) {
    init.body = body;
  }
  let response;
  try {
    response = await fetch(url, init);
  } catch (err) {
    if (isAbort(err)) throw err;
    throw new ApiError({ status: 0, error: "network", message: DEFAULT_MESSAGES[0] });
  }
  const data = await readBody(response);
  if (response.ok) return data ?? {};

  const error = new ApiError({
    status: response.status,
    error: data && typeof data.error === "string" ? data.error : `http_${response.status}`,
    message: messageOf(response.status, data),
    data,
    retryAfter: retryAfterOf(response, data),
  });
  if (response.status === 401) {
    error.handled = true;
    goToLogin();
  } else if (response.status === 403 && error.error === "csrf_failed") {
    error.handled = true;
    offerReload(error.message);
  }
  throw error;
}

export function get(url, options = {}) {
  return request("GET", url, options);
}

export function post(url, json, options = {}) {
  return request("POST", url, { ...options, json });
}

/** "message (try again in 12 s)" for rate limits, else the message. */
export function describeError(err) {
  if (!err) return "Something went wrong.";
  const message = err.message || "Something went wrong.";
  if (err instanceof ApiError && err.status === 429 && err.retryAfter) {
    const seconds = Math.max(1, Math.ceil(err.retryAfter));
    const wait = seconds < 90 ? `${seconds} s` : `${Math.ceil(seconds / 60)} min`;
    return `${message} (try again in ${wait})`;
  }
  return message;
}

/** Toast an error unless it was aborted or already handled. */
export function reportError(err, prefix = "") {
  if (isAbort(err) || (err && err.handled)) return;
  if (!(err instanceof ApiError)) console.error(err);
  const text = describeError(err);
  toast("error", prefix ? `${prefix}: ${text}` : text);
}
