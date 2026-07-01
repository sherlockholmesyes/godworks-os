// Pixel gate for the Godworks agar.io demo.
//
// This intentionally avoids Playwright/Puppeteer dependencies. It launches a
// local Chrome/Edge process, talks to the Chrome DevTools Protocol directly,
// verifies that the visible canvas is drawn from the CLIENT stream, drives the
// mouse path, and saves a screenshot artifact.
const fs = require("fs");
const http = require("http");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");
const { Buffer } = require("buffer");

const BASE = process.env.GW_AGAR_URL || "http://127.0.0.1:8091";
const OUT = process.env.GW_PIXEL_SCREENSHOT || path.join(".local", "agar", "agar-client-stream-pixel.png");
const WIDTH = parseInt(process.env.GW_PIXEL_WIDTH || "1280", 10);
const HEIGHT = parseInt(process.env.GW_PIXEL_HEIGHT || "720", 10);
const TIMEOUT_MS = parseInt(process.env.GW_PIXEL_TIMEOUT_MS || "15000", 10);

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function requestJson(url, options = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request(url, { method: options.method || "GET" }, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => body += chunk);
      res.on("end", () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`${options.method || "GET"} ${url} -> HTTP ${res.statusCode}: ${body}`));
          return;
        }
        try {
          resolve(JSON.parse(body));
        } catch (err) {
          reject(new Error(`invalid JSON from ${url}: ${err.message}`));
        }
      });
    });
    req.on("error", reject);
    req.end();
  });
}

function requestText(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => body += chunk);
      res.on("end", () => resolve({ status: res.statusCode, body }));
    });
    req.on("error", reject);
  });
}

async function waitForJson(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastErr = null;
  while (Date.now() < deadline) {
    try {
      return await requestJson(url);
    } catch (err) {
      lastErr = err;
      await sleep(100);
    }
  }
  throw lastErr || new Error(`timed out waiting for ${url}`);
}

function candidateBrowsers() {
  const env = process.env.GW_PIXEL_BROWSER || process.env.CHROME_PATH;
  const out = [];
  if (env) out.push(env);
  if (process.platform === "win32") {
    const pf = process.env.ProgramFiles;
    const pf86 = process.env["ProgramFiles(x86)"];
    const local = process.env.LocalAppData;
    if (pf) out.push(path.join(pf, "Google", "Chrome", "Application", "chrome.exe"));
    if (pf86) out.push(path.join(pf86, "Google", "Chrome", "Application", "chrome.exe"));
    if (local) out.push(path.join(local, "Google", "Chrome", "Application", "chrome.exe"));
    if (pf) out.push(path.join(pf, "Microsoft", "Edge", "Application", "msedge.exe"));
    if (pf86) out.push(path.join(pf86, "Microsoft", "Edge", "Application", "msedge.exe"));
  } else if (process.platform === "darwin") {
    out.push(
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
      "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
      "/Applications/Chromium.app/Contents/MacOS/Chromium"
    );
  } else {
    out.push("google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge", "msedge");
  }
  return out;
}

function findBrowser() {
  for (const candidate of candidateBrowsers()) {
    if (!candidate) continue;
    if (candidate.includes(path.sep) || path.isAbsolute(candidate)) {
      if (fs.existsSync(candidate)) return candidate;
    } else {
      return candidate;
    }
  }
  throw new Error("no Chrome/Edge browser found; set GW_PIXEL_BROWSER to a Chromium-compatible executable");
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = require("net").createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = address.port;
      server.close(() => resolve(port));
    });
    server.on("error", reject);
  });
}

class CdpClient {
  constructor(wsUrl) {
    this.nextId = 1;
    this.pending = new Map();
    this.ws = new WebSocket(wsUrl);
    this.opened = new Promise((resolve, reject) => {
      this.ws.onopen = resolve;
      this.ws.onerror = reject;
    });
    this.ws.onmessage = event => {
      const msg = JSON.parse(event.data);
      if (msg.id && this.pending.has(msg.id)) {
        const { resolve, reject } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        if (msg.error) reject(new Error(`${msg.error.message || "CDP error"} ${JSON.stringify(msg.error)}`));
        else resolve(msg.result || {});
      }
    };
  }

  async send(method, params = {}) {
    await this.opened;
    const id = this.nextId++;
    const payload = JSON.stringify({ id, method, params });
    const promise = new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
    this.ws.send(payload);
    return promise;
  }

  close() {
    try { this.ws.close(); } catch (_) {}
  }
}

async function createTarget(port) {
  const encoded = encodeURIComponent("about:blank");
  try {
    return await requestJson(`http://127.0.0.1:${port}/json/new?${encoded}`, { method: "PUT" });
  } catch (_) {
    return requestJson(`http://127.0.0.1:${port}/json/new?${encoded}`);
  }
}

async function browserState(cdp) {
  const result = await cdp.send("Runtime.evaluate", {
    returnByValue: true,
    expression: `(() => {
      const cv = document.getElementById("c");
      const hud = document.getElementById("hud");
      if (!cv || !hud) return { ready: false, reason: "missing canvas or hud" };
      const ctx = cv.getContext("2d");
      const data = ctx.getImageData(0, 0, cv.width, cv.height).data;
      let drawn = 0;
      for (let i = 3; i < data.length; i += 4) if (data[i] !== 0) drawn++;
      let player = null;
      try {
        if (typeof myId !== "undefined" && typeof snap !== "undefined") {
          const row = snap.find(v => v.e === myId);
          if (row) player = { id: myId, p: row.p, m: row.m, o: row.o || null };
        }
      } catch (_) {}
      return {
        ready: true,
        width: cv.width,
        height: cv.height,
        hud: hud.textContent,
        drawn_pixels: drawn,
        source_client: hud.textContent.includes("source: CLIENT stream"),
        player
      };
    })()`
  });
  return result.result.value;
}

async function waitForCanvas(cdp) {
  const deadline = Date.now() + TIMEOUT_MS;
  let state = null;
  while (Date.now() < deadline) {
    state = await browserState(cdp);
    if (
      state.ready &&
      state.source_client &&
      state.drawn_pixels > 1000 &&
      state.player &&
      Array.isArray(state.player.p)
    ) {
      return state;
    }
    await sleep(250);
  }
  throw new Error(`canvas did not become ready: ${JSON.stringify(state)}`);
}

async function removeProfileBestEffort(profile) {
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      fs.rmSync(profile, { recursive: true, force: true });
      return;
    } catch (err) {
      if (attempt === 4) {
        console.warn(`warning: could not remove temporary browser profile ${profile}: ${err.message}`);
        return;
      }
      await sleep(200);
    }
  }
}

async function main() {
  if (typeof WebSocket === "undefined") {
    throw new Error("Node WebSocket global is unavailable; use Node 22+ or provide a CDP-capable runtime");
  }
  const browser = findBrowser();
  const port = await freePort();
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), "godworks-agar-pixel-"));
  const args = [
    "--headless=new",
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${profile}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-background-networking",
    "--disable-extensions",
    "--disable-gpu",
    "about:blank"
  ];
  const proc = spawn(browser, args, { stdio: "ignore" });
  let cdp = null;
  try {
    await waitForJson(`http://127.0.0.1:${port}/json/version`, TIMEOUT_MS);
    const target = await createTarget(port);
    cdp = new CdpClient(target.webSocketDebuggerUrl);
    await cdp.send("Page.enable");
    await cdp.send("Runtime.enable");
    await cdp.send("Emulation.setDeviceMetricsOverride", {
      width: WIDTH,
      height: HEIGHT,
      deviceScaleFactor: 1,
      mobile: false
    });
    await cdp.send("Page.navigate", { url: BASE });
    await waitForCanvas(cdp);
    await cdp.send("Input.dispatchMouseEvent", { type: "mouseMoved", x: WIDTH - 24, y: HEIGHT - 24 });
    const before = await browserState(cdp);
    await sleep(1800);
    const after = await waitForCanvas(cdp);
    const moved = before.player && after.player
      ? Math.hypot(after.player.p[0] - before.player.p[0], after.player.p[1] - before.player.p[1])
      : 0;
    if (moved < 0.2) throw new Error(`visible player did not move enough: ${moved.toFixed(3)}`);
    const shot = await cdp.send("Page.captureScreenshot", { format: "png", fromSurface: true });
    fs.mkdirSync(path.dirname(OUT), { recursive: true });
    fs.writeFileSync(OUT, Buffer.from(shot.data, "base64"));
    const report = {
      ok: true,
      base: BASE,
      screenshot: OUT,
      width: after.width,
      height: after.height,
      drawn_pixels: after.drawn_pixels,
      player_moved: moved,
      source_client: after.source_client,
      player: after.player,
      hud_first_line: String(after.hud || "").split("\n")[0]
    };
    console.log(JSON.stringify(report, null, 2));
  } finally {
    if (cdp) cdp.close();
    proc.kill();
    await Promise.race([
      new Promise(resolve => proc.once("exit", resolve)),
      sleep(1200)
    ]);
    await removeProfileBestEffort(profile);
  }
}

main().catch(err => {
  console.error(err.stack || err.message);
  process.exit(1);
});
