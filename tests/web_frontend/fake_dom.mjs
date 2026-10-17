// A deliberately small DOM for running the browser modules in Node.js (jsdom is not a
// dependency): just enough of Document/Element/Event for the calls the modules make.
//
// * parseHTML: the well-formed markup that the Jinja templates and html`` produce (quoted
//   attributes, explicit end tags, void and self-closing elements, character references).
// * Selectors: one compound selector — tag, #id, .class, [attr], [attr=value] (lists with
//   commas are supported, combinators are not and throw, so a module using one is noticed).
// * Events bubble from the target to the document; focus() fires focusout/focusin with
//   relatedTarget; <dialog> has showModal()/close() with returnValue and a "close" event.
// * Form controls: input value/checked, select value (from its options).
// Layout does not exist: getBoundingClientRect() returns what a test put in `element.rect`.

const VOID = new Set([
  "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source",
  "track", "wbr",
]);
const RAW_TEXT = new Set(["script", "style"]);
const NAMED = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " " };

export function decodeEntities(text) {
  return text.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (whole, ref) => {
    if (ref[0] === "#") {
      const hex = ref[1] === "x" || ref[1] === "X";
      return String.fromCodePoint(parseInt(ref.slice(hex ? 2 : 1), hex ? 16 : 10));
    }
    return NAMED[ref.toLowerCase()] ?? whole;
  });
}

export class FakeEvent {
  constructor(type, init = {}) {
    this.bubbles = true;
    Object.assign(this, init);
    this.type = type;
    this.target = null;
    this.currentTarget = null;
    this.defaultPrevented = false;
    this.propagationStopped = false;
  }

  preventDefault() {
    this.defaultPrevented = true;
  }

  stopPropagation() {
    this.propagationStopped = true;
  }
}

class Listeners {
  constructor() {
    this.listeners = new Map();
  }

  addEventListener(type, fn, options) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push({ fn, once: Boolean(options && options.once) });
  }

  removeEventListener(type, fn) {
    const list = this.listeners.get(type) || [];
    const index = list.findIndex((entry) => entry.fn === fn);
    if (index >= 0) list.splice(index, 1);
  }

  fire(event) {
    for (const entry of [...(this.listeners.get(event.type) || [])]) {
      if (entry.once) this.removeEventListener(event.type, entry.fn);
      event.currentTarget = this;
      entry.fn.call(this, event);
    }
  }
}

class TextNode {
  constructor(text) {
    this.nodeType = 3;
    this.data = text;
    this.parentNode = null;
  }

  get textContent() {
    return this.data;
  }

  set textContent(value) {
    this.data = String(value);
  }

  cloneNode() {
    return new TextNode(this.data);
  }

  remove() {
    if (this.parentNode) this.parentNode.removeChild(this);
  }

  serialize() {
    return this.data.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }
}

function camelToData(key) {
  return `data-${key.replace(/[A-Z]/g, (c) => `-${c.toLowerCase()}`)}`;
}

export class FakeElement extends Listeners {
  constructor(tagName, ownerDocument) {
    super();
    this.nodeType = 1;
    this.localName = tagName.toLowerCase();
    this.tagName = tagName.toUpperCase();
    this.ownerDocument = ownerDocument;
    this.attributes = new Map();
    this.childNodes = [];
    this.parentNode = null;
    /** Layout stand-in for getBoundingClientRect(): {left, top, width, height}. */
    this.rect = null;
  }

  // ── attributes and reflected properties ────────────────────────────────────
  getAttribute(name) {
    return this.attributes.has(name) ? this.attributes.get(name) : null;
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  removeAttribute(name) {
    this.attributes.delete(name);
  }

  hasAttribute(name) {
    return this.attributes.has(name);
  }

  get id() {
    return this.getAttribute("id") || "";
  }

  set id(value) {
    this.setAttribute("id", value);
  }

  get className() {
    return this.getAttribute("class") || "";
  }

  set className(value) {
    this.setAttribute("class", value);
  }

  get classList() {
    const element = this;
    const tokens = () => element.className.split(/\s+/).filter(Boolean);
    const write = (list) => {
      element.className = list.join(" ");
    };
    return {
      contains: (token) => tokens().includes(token),
      add: (...add) => write([...tokens(), ...add.filter((t) => !tokens().includes(t))]),
      remove: (...remove) => write(tokens().filter((t) => !remove.includes(t))),
      toggle(token, force) {
        const has = tokens().includes(token);
        const want = force === undefined ? !has : Boolean(force);
        if (want && !has) write([...tokens(), token]);
        if (!want && has) write(tokens().filter((t) => t !== token));
        return want;
      },
    };
  }

  get dataset() {
    const element = this;
    return new Proxy(
      {},
      {
        get: (_target, key) =>
          typeof key === "string" ? (element.getAttribute(camelToData(key)) ?? undefined) : undefined,
        set: (_target, key, value) => {
          element.setAttribute(camelToData(String(key)), value);
          return true;
        },
      },
    );
  }

  get hidden() {
    return this.hasAttribute("hidden");
  }

  set hidden(value) {
    if (value) this.setAttribute("hidden", "");
    else this.removeAttribute("hidden");
  }

  get disabled() {
    return this.hasAttribute("disabled");
  }

  set disabled(value) {
    if (value) this.setAttribute("disabled", "");
    else this.removeAttribute("disabled");
  }

  get title() {
    return this.getAttribute("title") || "";
  }

  set title(value) {
    this.setAttribute("title", value);
  }

  get type() {
    return (this.getAttribute("type") || "").toLowerCase();
  }

  set type(value) {
    this.setAttribute("type", value);
  }

  // ── tree ────────────────────────────────────────────────────────────────────
  get children() {
    return this.childNodes.filter((node) => node.nodeType === 1);
  }

  get firstElementChild() {
    return this.children[0] || null;
  }

  get isConnected() {
    let node = this;
    while (node.parentNode) node = node.parentNode;
    return node === this.ownerDocument;
  }

  append(...nodes) {
    for (let node of nodes) {
      if (typeof node === "string") node = new TextNode(node);
      if (node.parentNode) node.parentNode.removeChild(node);
      node.parentNode = this;
      this.childNodes.push(node);
    }
  }

  appendChild(node) {
    this.append(node);
    return node;
  }

  removeChild(node) {
    const index = this.childNodes.indexOf(node);
    if (index >= 0) this.childNodes.splice(index, 1);
    node.parentNode = null;
    return node;
  }

  replaceChildren(...nodes) {
    for (const node of this.childNodes) node.parentNode = null;
    this.childNodes = [];
    this.append(...nodes);
  }

  remove() {
    if (this.parentNode) this.parentNode.removeChild(this);
  }

  contains(node) {
    for (let current = node; current; current = current.parentNode) {
      if (current === this) return true;
    }
    return false;
  }

  cloneNode(deep = false) {
    const copy = this.ownerDocument.createElement(this.localName);
    for (const [name, value] of this.attributes) copy.setAttribute(name, value);
    if (deep) for (const node of this.childNodes) copy.append(node.cloneNode(true));
    return copy;
  }

  get textContent() {
    return this.childNodes.map((node) => node.textContent).join("");
  }

  set textContent(value) {
    const text = value === null || value === undefined ? "" : String(value);
    this.replaceChildren(...(text ? [new TextNode(text)] : []));
  }

  get innerHTML() {
    return this.childNodes.map((node) => node.serialize()).join("");
  }

  set innerHTML(markup) {
    this.replaceChildren(...parseHTML(String(markup), this.ownerDocument));
  }

  serialize() {
    const attrs = [...this.attributes]
      .map(([name, value]) => ` ${name}="${value.replace(/&/g, "&amp;").replace(/"/g, "&quot;")}"`)
      .join("");
    if (VOID.has(this.localName)) return `<${this.localName}${attrs}>`;
    return `<${this.localName}${attrs}>${this.innerHTML}</${this.localName}>`;
  }

  // ── selectors ───────────────────────────────────────────────────────────────
  matches(selector) {
    return compileSelector(selector)(this);
  }

  closest(selector) {
    const test = compileSelector(selector);
    for (let node = this; node && node.nodeType === 1; node = node.parentNode) {
      if (test(node)) return node;
    }
    return null;
  }

  querySelectorAll(selector) {
    const test = compileSelector(selector);
    const out = [];
    walk(this, (element) => {
      if (element !== this && test(element)) out.push(element);
    });
    return out;
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }

  // ── events, focus, layout ───────────────────────────────────────────────────
  dispatchEvent(event) {
    event.target = this;
    const path = [];
    for (let node = this; node; node = node.parentNode) path.push(node);
    for (const node of path) {
      node.fire(event);
      if (event.propagationStopped || !event.bubbles) break;
    }
    return !event.defaultPrevented;
  }

  click() {
    if (this.disabled) return;
    if (this.localName === "input" && (this.type === "checkbox" || this.type === "radio")) {
      this.checked = !this.checked;
      this.dispatchEvent(new FakeEvent("click"));
      this.dispatchEvent(new FakeEvent("input"));
      this.dispatchEvent(new FakeEvent("change"));
      return;
    }
    this.dispatchEvent(new FakeEvent("click"));
  }

  focus() {
    this.ownerDocument.moveFocus(this);
  }

  blur() {
    if (this.ownerDocument.activeElement === this) this.ownerDocument.moveFocus(this.ownerDocument.body);
  }

  getBoundingClientRect() {
    const r = this.rect || { left: 0, top: 0, width: 0, height: 0 };
    return { ...r, x: r.left, y: r.top, right: r.left + r.width, bottom: r.top + r.height };
  }
}

export class FakeInputElement extends FakeElement {
  constructor(tagName, ownerDocument) {
    super(tagName, ownerDocument);
    this.valueSet = null;
    this.checkedSet = null;
    this.indeterminate = false;
    this.files = null;
  }

  get value() {
    if (this.valueSet !== null) return this.valueSet;
    return this.getAttribute("value") ?? (this.type === "checkbox" ? "on" : "");
  }

  set value(value) {
    this.valueSet = String(value);
  }

  get checked() {
    return this.checkedSet ?? this.hasAttribute("checked");
  }

  set checked(value) {
    this.checkedSet = Boolean(value);
  }
}

export class FakeSelectElement extends FakeElement {
  get options() {
    return this.querySelectorAll("option");
  }

  get value() {
    const options = this.options;
    const chosen = options.find((o) => o.selectedSet) || options.find((o) => o.hasAttribute("selected"));
    const option = chosen || options[0];
    return option ? (option.getAttribute("value") ?? option.textContent) : "";
  }

  set value(value) {
    for (const option of this.options) {
      option.selectedSet = (option.getAttribute("value") ?? option.textContent) === String(value);
    }
  }
}

export class FakeDialogElement extends FakeElement {
  constructor(tagName, ownerDocument) {
    super(tagName, ownerDocument);
    this.returnValue = "";
  }

  get open() {
    return this.hasAttribute("open");
  }

  showModal() {
    this.setAttribute("open", "");
  }

  close(returnValue) {
    if (!this.open) return;
    if (returnValue !== undefined) this.returnValue = String(returnValue);
    this.removeAttribute("open");
    this.dispatchEvent(new FakeEvent("close", { bubbles: false }));
  }
}

const CLASSES = {
  input: FakeInputElement,
  textarea: FakeInputElement,
  select: FakeSelectElement,
  dialog: FakeDialogElement,
};

function walk(root, visit) {
  for (const node of root.childNodes) {
    if (node.nodeType !== 1) continue;
    visit(node);
    walk(node, visit);
  }
}

const selectorCache = new Map();

function compileSelector(selector) {
  if (selectorCache.has(selector)) return selectorCache.get(selector);
  const alternatives = selector.split(",").map((part) => {
    const text = part.trim();
    const match = /^([a-zA-Z][\w-]*|\*)?((?:#[\w-]+|\.[\w-]+|\[[^\]]+\])*)$/.exec(text);
    if (!match || text === "") throw new Error(`fake DOM: unsupported selector ${JSON.stringify(selector)}`);
    const tag = match[1] && match[1] !== "*" ? match[1].toLowerCase() : null;
    const tests = [];
    const simple = /#([\w-]+)|\.([\w-]+)|\[\s*([\w:-]+)\s*(?:=\s*(?:"([^"]*)"|'([^']*)'|([^\]\s]+)))?\s*\]/g;
    for (const m of match[2].matchAll(simple)) {
      if (m[1]) tests.push((el) => el.id === m[1]);
      else if (m[2]) tests.push((el) => el.classList.contains(m[2]));
      else {
        const value = m[4] ?? m[5] ?? m[6];
        tests.push(value === undefined ? (el) => el.hasAttribute(m[3]) : (el) => el.getAttribute(m[3]) === value);
      }
    }
    return (el) => (!tag || el.localName === tag) && tests.every((test) => test(el));
  });
  const compiled = (el) => alternatives.some((test) => test(el));
  selectorCache.set(selector, compiled);
  return compiled;
}

const TOKEN_RE =
  /<!--[\s\S]*?-->|<![^>]*>|<\/([a-zA-Z][\w:-]*)\s*>|<([a-zA-Z][\w:-]*)((?:\s+[^\s"'>/=]+(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'=<>`]+))?)*)\s*(\/?)>|([^<]+)|(<)/g;
const ATTR_RE = /([^\s"'>/=]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;

/** Parse markup into nodes owned by `doc` (top-level nodes are returned). */
export function parseHTML(markup, doc) {
  const root = { childNodes: [], append: FakeElement.prototype.append, removeChild: FakeElement.prototype.removeChild };
  const stack = [root];
  let rawUntil = null;
  for (const m of markup.matchAll(TOKEN_RE)) {
    const top = stack[stack.length - 1];
    if (rawUntil) {
      if (m[1] && m[1].toLowerCase() === rawUntil) {
        stack.pop();
        rawUntil = null;
      } else {
        top.append(new TextNode(m[0]));
      }
      continue;
    }
    if (m[1]) {
      const name = m[1].toLowerCase();
      for (let i = stack.length - 1; i > 0; i -= 1) {
        if (stack[i].localName === name) {
          stack.length = i;
          break;
        }
      }
    } else if (m[2]) {
      const element = doc.createElement(m[2]);
      for (const a of (m[3] || "").matchAll(ATTR_RE)) {
        element.setAttribute(a[1].toLowerCase(), decodeEntities(a[2] ?? a[3] ?? a[4] ?? ""));
      }
      top.append(element);
      if (!m[4] && !VOID.has(element.localName)) {
        stack.push(element);
        if (RAW_TEXT.has(element.localName)) rawUntil = element.localName;
      }
    } else if (m[5] !== undefined) {
      top.append(new TextNode(decodeEntities(m[5])));
    } else if (m[6]) {
      top.append(new TextNode("<"));
    }
  }
  const nodes = [...root.childNodes];
  for (const node of nodes) node.parentNode = null;
  return nodes;
}

export class FakeDocument extends Listeners {
  /** `markup`: a whole page (<html>…) or body content. */
  constructor(markup = "") {
    super();
    this.nodeType = 9;
    this.childNodes = [];
    this.parentNode = null;
    const nodes = parseHTML(markup, this);
    let html = nodes.find((n) => n.nodeType === 1 && n.localName === "html");
    if (!html) {
      html = this.createElement("html");
      html.append(this.createElement("head"), this.createElement("body"));
      html.children[1].append(...nodes);
    }
    html.parentNode = this;
    this.childNodes.push(html);
    this.documentElement = html;
    this.head = html.querySelector("head");
    this.body = html.querySelector("body");
    this.activeElement = this.body;
  }

  createElement(tagName) {
    const Kind = CLASSES[tagName.toLowerCase()] || FakeElement;
    return new Kind(tagName, this);
  }

  createTextNode(text) {
    return new TextNode(text);
  }

  getElementById(id) {
    let found = null;
    walk(this, (element) => {
      if (!found && element.id === id) found = element;
    });
    return found;
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }

  querySelectorAll(selector) {
    const test = compileSelector(selector);
    const out = [];
    walk(this, (element) => {
      if (test(element)) out.push(element);
    });
    return out;
  }

  contains(node) {
    return Boolean(node) && node.isConnected;
  }

  moveFocus(element) {
    const previous = this.activeElement;
    if (previous === element) return;
    if (previous && previous !== this.body) {
      previous.dispatchEvent(new FakeEvent("focusout", { relatedTarget: element }));
    }
    this.activeElement = element;
    if (element && element !== this.body) {
      element.dispatchEvent(new FakeEvent("focusin", { relatedTarget: previous }));
    }
  }

  dispatchEvent(event) {
    event.target = this;
    this.fire(event);
    return !event.defaultPrevented;
  }
}

/** A window stand-in whose location/history behave for js/core/urlstate.js and api.js. */
export function makeWindow({ path = "/app/dashboard", search = "", innerWidth = 1024 } = {}) {
  const navigations = [];
  const win = new Listeners();
  Object.assign(win, {
    navigations,
    innerWidth,
    location: {
      pathname: path,
      search,
      hash: "",
      assign: (url) => navigations.push(url),
      reload: () => navigations.push("reload"),
    },
    history: {
      state: null,
      replaceState(_state, _title, url) {
        const [pathname, query = ""] = url.split("#")[0].split("?");
        win.location.pathname = pathname;
        win.location.search = query ? `?${query}` : "";
      },
    },
    matchMedia: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
    confirm: () => false,
  });
  return win;
}

/** Install `doc` and a fresh window as the globals the modules use. */
export function install(doc, windowOptions) {
  const win = makeWindow(windowOptions);
  globalThis.document = doc;
  globalThis.window = win;
  globalThis.HTMLElement = FakeElement;
  globalThis.HTMLInputElement = FakeInputElement;
  globalThis.HTMLSelectElement = FakeSelectElement;
  globalThis.HTMLDialogElement = FakeDialogElement;
  return win;
}

/** Let pending promise callbacks and zero-delay timers run. */
export async function settle(rounds = 25) {
  for (let i = 0; i < rounds; i += 1) await new Promise((resolve) => setImmediate(resolve));
}
