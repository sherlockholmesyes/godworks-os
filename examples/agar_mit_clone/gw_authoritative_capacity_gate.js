"use strict";

const http = require("http");

const STATE_URL = process.env.GW_AGAR_STATE_URL || "http://127.0.0.1:3000/state";
const BOTS_URL = process.env.GW_AUTH_BOTS_URL || "http://127.0.0.1:8094/state";
const DURATION_MS = parseInt(process.env.GW_AUTH_CAPACITY_MS || "15000", 10);
const SAMPLE_MS = parseInt(process.env.GW_AUTH_CAPACITY_SAMPLE_MS || "500", 10);
const MIN_SAMPLES = parseInt(process.env.GW_AUTH_CAPACITY_MIN_SAMPLES || "8", 10);
const MIN_OK_SAMPLES = parseInt(process.env.GW_AUTH_CAPACITY_MIN_OK_SAMPLES || "8", 10);
const MIN_PLAYERS = parseInt(process.env.GW_AUTH_CAPACITY_MIN_PLAYERS || "20", 10);
const MIN_ENTITIES = parseInt(process.env.GW_AUTH_CAPACITY_MIN_ENTITIES || "900", 10);
const MIN_WORKERS = parseInt(process.env.GW_AUTH_CAPACITY_MIN_WORKERS || "16", 10);
const MIN_COMMAND_DELTA = parseInt(process.env.GW_AUTH_CAPACITY_MIN_COMMAND_DELTA || `${MIN_PLAYERS}`, 10);
const MAX_REJECT_DELTA = parseInt(process.env.GW_AUTH_CAPACITY_MAX_REJECT_DELTA || "0", 10);
const MAX_TRANSIENT_REJECT_DELTA = parseInt(process.env.GW_AUTH_CAPACITY_MAX_TRANSIENT_REJECT_DELTA || `${Math.max(MIN_PLAYERS * 2, 50)}`, 10);
const MIN_BOT_ALIVE = parseInt(process.env.GW_AUTH_CAPACITY_MIN_BOT_ALIVE || `${MIN_PLAYERS}`, 10);
const MIN_BOT_FRAME_DELTA = parseInt(process.env.GW_AUTH_CAPACITY_MIN_BOT_FRAME_DELTA || `${MIN_PLAYERS}`, 10);

function getJson(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { body += chunk; });
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode || 0, value: JSON.parse(body || "{}") });
        } catch (e) {
          reject(new Error(`${url} did not return JSON: ${e.message}`));
        }
      });
    });
    req.setTimeout(5000, () => req.destroy(new Error(`timeout ${url}`)));
    req.on("error", reject);
  });
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function assertOk(condition, message, facts) {
  if (!condition) {
    const err = new Error(message);
    err.facts = facts;
    throw err;
  }
}

function numeric(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function ownerCount(state) {
  return Object.keys((state && state.owners) || {}).length;
}

function summarizeNumbers(values) {
  const clean = values.map(numeric);
  if (!clean.length) return { min: 0, max: 0, mean: 0 };
  const sum = clean.reduce((acc, value) => acc + value, 0);
  return { min: Math.min(...clean), max: Math.max(...clean), mean: sum / clean.length };
}

async function main() {
  const initialStateReply = await getJson(STATE_URL);
  assertOk(initialStateReply.status === 200, "server state did not return 200", {
    status: initialStateReply.status,
    stateUrl: STATE_URL,
  });
  const initialBotsReply = await getJson(BOTS_URL);
  assertOk(initialBotsReply.status === 200, "bot state did not return 200", {
    status: initialBotsReply.status,
    botsUrl: BOTS_URL,
  });

  const initialState = initialStateReply.value;
  const initialBots = initialBotsReply.value;
  const initialCommands = numeric(initialState.commandResponses);
  const initialRejects = numeric(initialState.commandRejects);
  const initialTransientRejects = numeric(initialState.commandTransientRejects);
  const initialFrames = numeric(initialBots.frames);

  const started = Date.now();
  const samples = [];
  const botSamples = [];
  const errors = [];

  while (Date.now() - started < DURATION_MS || samples.length < MIN_SAMPLES) {
    try {
      const stateReply = await getJson(STATE_URL);
      assertOk(stateReply.status === 200, "server state did not return 200", { status: stateReply.status });
      samples.push(stateReply.value);
    } catch (e) {
      errors.push(e.message);
    }

    try {
      const botsReply = await getJson(BOTS_URL);
      assertOk(botsReply.status === 200, "bot state did not return 200", { status: botsReply.status });
      botSamples.push(botsReply.value);
    } catch (e) {
      errors.push(e.message);
    }

    await delay(SAMPLE_MS);
  }

  assertOk(samples.length >= MIN_SAMPLES, "too few capacity samples", {
    samples: samples.length,
    minSamples: MIN_SAMPLES,
    errors,
  });

  const okSamples = samples.filter(state =>
    state &&
    state.godworksAuthoritative === true &&
    numeric(state.players) >= MIN_PLAYERS &&
    numeric(state.playerEntities) >= MIN_PLAYERS &&
    numeric(state.entities) >= MIN_ENTITIES &&
    ownerCount(state) >= MIN_WORKERS
  );
  const latest = samples[samples.length - 1] || {};
  const latestBots = botSamples[botSamples.length - 1] || {};
  const commandDelta = numeric(latest.commandResponses) - initialCommands;
  const rejectDelta = numeric(latest.commandRejects) - initialRejects;
  const transientRejectDelta = numeric(latest.commandTransientRejects) - initialTransientRejects;
  const botFrameDelta = numeric(latestBots.frames) - initialFrames;

  assertOk(okSamples.length >= MIN_OK_SAMPLES, "authoritative capacity floor was not sustained", {
    okSamples: okSamples.length,
    minOkSamples: MIN_OK_SAMPLES,
    minPlayers: MIN_PLAYERS,
    minEntities: MIN_ENTITIES,
    minWorkers: MIN_WORKERS,
  });
  assertOk(commandDelta >= MIN_COMMAND_DELTA, "broker command response delta too low under bot load", {
    commandDelta,
    minCommandDelta: MIN_COMMAND_DELTA,
  });
  assertOk(rejectDelta <= MAX_REJECT_DELTA, "broker command rejects exceeded configured cap", {
    rejectDelta,
    maxRejectDelta: MAX_REJECT_DELTA,
  });
  assertOk(transientRejectDelta <= MAX_TRANSIENT_REJECT_DELTA, "broker transient command retries exceeded configured cap", {
    transientRejectDelta,
    maxTransientRejectDelta: MAX_TRANSIENT_REJECT_DELTA,
  });
  assertOk(numeric(latestBots.alive) >= MIN_BOT_ALIVE, "too few bots alive at capacity gate end", {
    alive: latestBots.alive,
    minBotAlive: MIN_BOT_ALIVE,
  });
  assertOk(botFrameDelta >= MIN_BOT_FRAME_DELTA, "bots did not receive enough live frames", {
    botFrameDelta,
    minBotFrameDelta: MIN_BOT_FRAME_DELTA,
  });

  const playerStats = summarizeNumbers(samples.map(s => s.players));
  const playerEntityStats = summarizeNumbers(samples.map(s => s.playerEntities));
  const entityStats = summarizeNumbers(samples.map(s => s.entities));
  const ownerStats = summarizeNumbers(samples.map(ownerCount));
  const botAliveStats = summarizeNumbers(botSamples.map(s => s.alive));

  console.log(JSON.stringify({
    ok: true,
    gate: "godworks_authoritative_agar_capacity",
    stateUrl: STATE_URL,
    botsUrl: BOTS_URL,
    thresholds: {
      durationMs: DURATION_MS,
      sampleMs: SAMPLE_MS,
      minSamples: MIN_SAMPLES,
      minOkSamples: MIN_OK_SAMPLES,
      minPlayers: MIN_PLAYERS,
      minEntities: MIN_ENTITIES,
      minWorkers: MIN_WORKERS,
      minCommandDelta: MIN_COMMAND_DELTA,
      maxRejectDelta: MAX_REJECT_DELTA,
      maxTransientRejectDelta: MAX_TRANSIENT_REJECT_DELTA,
      minBotAlive: MIN_BOT_ALIVE,
      minBotFrameDelta: MIN_BOT_FRAME_DELTA,
    },
    samples: {
      count: samples.length,
      okSamples: okSamples.length,
      durationMs: Date.now() - started,
      players: playerStats,
      playerEntities: playerEntityStats,
      entities: entityStats,
      owners: ownerStats,
      botAlive: botAliveStats,
    },
    deltas: {
      commandResponses: commandDelta,
      commandRejects: rejectDelta,
      transientCommandRejects: transientRejectDelta,
      botFrames: botFrameDelta,
    },
    initialState,
    initialBots,
    latestState: latest,
    latestBots,
  }, null, 2));
}

main().catch(err => {
  console.error(JSON.stringify({
    ok: false,
    error: err.message,
    facts: err.facts || null,
  }, null, 2));
  process.exit(1);
});
