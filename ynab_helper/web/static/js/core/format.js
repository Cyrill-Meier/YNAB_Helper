// Formatting for display. Money arrives as integer milliunits plus an ISO currency code
// (DESIGN §9.2) and is formatted with Intl.NumberFormat from an exact decimal string.
// Dates (YYYY-MM-DD, calendar days in YNAB) are shown exactly as sent. Nothing is ever
// parsed with new Date()/Date.parse() (F106): a timestamp with an explicit zone ("…Z",
// "…+02:00" — an unambiguous instant) is computed with Date.UTC and shown in the browser's
// time zone, like v1; a naive timestamp is shown as written.

import { html } from "./html.js";

const formatters = new Map();
const CURRENCY_RE = /^[A-Z]{3}$/;

function formatter(key, options) {
  let nf = formatters.get(key);
  if (!nf) {
    try {
      nf = new Intl.NumberFormat(undefined, options);
    } catch {
      nf = new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }
    formatters.set(key, nf);
  }
  return nf;
}

function validCurrency(currency) {
  return typeof currency === "string" && CURRENCY_RE.test(currency) ? currency : null;
}

/** Integer milliunits → exact decimal string ("-1234.560"), or null. */
export function milliToDecimal(milli) {
  if (typeof milli !== "number" || !Number.isFinite(milli)) return null;
  const whole = Math.trunc(milli);
  const negative = whole < 0;
  const abs = Math.abs(whole);
  const units = Math.floor(abs / 1000);
  const fraction = String(abs % 1000).padStart(3, "0");
  return `${negative ? "-" : ""}${units}.${fraction}`;
}

function fractionDigits(currency) {
  const code = validCurrency(currency);
  if (!code) return 2;
  return formatter(`digits:${code}`, { style: "currency", currency: code }).resolvedOptions()
    .maximumFractionDigits;
}

function formatDecimal(nf, milli) {
  const decimal = milliToDecimal(milli);
  if (decimal === null) return "—";
  // Modern engines format the decimal string exactly; older ones convert it to a Number.
  return nf.format(decimal);
}

/** "CHF 1,234.50" — for headline numbers. */
export function money(milli, currency, { signed = false } = {}) {
  const code = validCurrency(currency);
  const sign = signed ? "exceptZero" : "auto";
  if (!code) return amount(milli, null, { signed });
  return formatDecimal(
    formatter(`money:${code}:${sign}`, { style: "currency", currency: code, signDisplay: sign }),
    milli,
  );
}

/** "1,234.50" — the currency's decimals without the symbol (tables, where the column says it). */
export function amount(milli, currency, { signed = false } = {}) {
  const digits = fractionDigits(currency);
  const sign = signed ? "exceptZero" : "auto";
  return formatDecimal(
    formatter(`amount:${digits}:${sign}`, {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
      signDisplay: sign,
    }),
    milli,
  );
}

/** Integer count with grouping. */
export function count(n) {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return formatter("count", { maximumFractionDigits: 0 }).format(n);
}

/** Percentage with one decimal and an explicit sign ("+4.2 %"). */
export function percent(ratio) {
  if (typeof ratio !== "number" || !Number.isFinite(ratio)) return "—";
  return formatter("percent", {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
    signDisplay: "exceptZero",
  }).format(ratio);
}

/** "1 transaction" / "3 transactions". */
export function plural(n, one, many = `${one}s`) {
  return `${count(n)} ${n === 1 ? one : many}`;
}

/** A date string (YYYY-MM-DD) exactly as sent. */
export function date(value) {
  return typeof value === "string" && value !== "" ? value : "—";
}

const TIMESTAMP_RE =
  /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?:(Z)|([+-])(\d{2}):?(\d{2}))?$/;
const localFormatters = new Map();

/**
 * Epoch milliseconds of an ISO timestamp that names its zone ("Z" or "±hh:mm"), or null
 * (naive, malformed or out of range). Arithmetic only — no engine date parsing.
 */
export function instantOf(value) {
  const match = typeof value === "string" ? TIMESTAMP_RE.exec(value) : null;
  if (!match) return null;
  const [, y, mo, d, hh, mm, ss = "00", utc, sign, oh, om] = match;
  if (!utc && !sign) return null;
  const n = [y, mo, d, hh, mm, ss, oh || "0", om || "0"].map(Number);
  const [year, month, day, hour, minute, second, offH, offM] = n;
  if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59) return null;
  if (second > 59 || offH > 23 || offM > 59) return null;
  // Date.UTC would roll 2026-02-31 over into March: reject days the month does not have.
  if (day > daysInMonth(year, month)) return null;
  const offset = (sign === "-" ? -1 : 1) * (offH * 60 + offM);
  const epoch = Date.UTC(year, month - 1, day, hour, minute, second) - offset * 60000;
  return Number.isFinite(epoch) ? epoch : null;
}

function daysInMonth(year, month) {
  if (month === 2) return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0 ? 29 : 28;
  return [4, 6, 9, 11].includes(month) ? 30 : 31;
}

function localParts(epoch, timeZone) {
  const key = timeZone || "";
  let dtf = localFormatters.get(key);
  if (!dtf) {
    dtf = new Intl.DateTimeFormat("en-US", {
      timeZone: timeZone || undefined,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    });
    localFormatters.set(key, dtf);
  }
  const parts = {};
  for (const { type, value } of dtf.formatToParts(epoch)) parts[type] = value;
  return parts;
}

/**
 * A server timestamp for display. "2026-10-16T18:30:00Z" → "2026-10-16 20:30" in the
 * browser's time zone (or `timeZone`, for tests); a naive timestamp keeps its digits
 * ("2026-10-16 08:15"); anything unrecognised is returned verbatim.
 */
export function timestamp(value, { timeZone } = {}) {
  if (typeof value !== "string" || value === "") return "—";
  const match = TIMESTAMP_RE.exec(value);
  if (!match) return value;
  const [, y, mo, d, hh, mm, , utc, sign] = match;
  const asWritten = `${y}-${mo}-${d} ${hh}:${mm}`;
  if (!utc && !sign) return asWritten;
  const epoch = instantOf(value);
  if (epoch === null) return value;
  try {
    const p = localParts(epoch, timeZone);
    return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}`;
  } catch {
    // No time-zone support (or an unknown zone): show the instant in UTC, labelled.
    return utc ? `${asWritten} UTC` : value;
  }
}

/** Text kept on one line (a date such as "2026-10-16" would otherwise wrap at a hyphen). */
export function nowrap(text) {
  return html`<span class="whitespace-nowrap">${text}</span>`;
}

/** "2026-09-05 → 2026-10-17", each date kept on one line. */
export function dateRange(from, to) {
  return html`${nowrap(date(from))} → ${nowrap(date(to))}`;
}

/** Whole seconds as "45 s" / "3 min". */
export function duration(seconds) {
  if (typeof seconds !== "number" || !Number.isFinite(seconds) || seconds < 0) return "";
  if (seconds < 90) return `${Math.max(1, Math.ceil(seconds))} s`;
  return `${Math.ceil(seconds / 60)} min`;
}

/** File size for humans. */
export function bytes(n) {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

/** CSS class for an amount's direction (outflows red, inflows green). */
export function amountClass(milli) {
  if (typeof milli !== "number" || milli === 0) return "";
  return milli < 0 ? "amount-out" : "amount-in";
}
