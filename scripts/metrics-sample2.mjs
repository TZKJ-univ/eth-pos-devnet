#!/usr/bin/env node
/**
 * metrics-sample2.mjs
 * 目的: 毎秒のEL/CL/Validatorとネットワーク全体のユニーク指標をCSVに分割出力。
 * 方針: 数値中心、bool/hash不要、重複集約はしない（ネットワークCSVはAPI由来のみ）。
 * 環境変数: ENDPOINTS, BEACON_URLS, INTERVAL_MS, DURATION_SEC のみ。
 */

import fs from 'node:fs';
import path from 'node:path';

function parseList(v, def) {
  const s = (v ?? def).trim();
  return s ? s.split(/[\s,]+/).filter(Boolean) : [];
}

const EL_ENDPOINTS = parseList(process.env.ENDPOINTS, 'http://geth:8545,http://geth-2:8545,http://geth-3:8545');
const CL_ENDPOINTS = parseList(process.env.BEACON_URLS, 'http://prysm:3500,http://prysm-2:3500,http://prysm-3:3500');
const EL_INDEX = Object.fromEntries(EL_ENDPOINTS.map((e, i) => [e, i]));
const CL_INDEX = Object.fromEntries(CL_ENDPOINTS.map((e, i) => [e, i]));
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 1000);
const DURATION_SEC = Number(process.env.DURATION_SEC || 0);

// slots per epoch は config/config.yml から読む（なければ6）
function readSlotsPerEpoch() {
  try {
    const cfgPath = path.join(process.cwd(), 'config', 'config.yml');
    if (fs.existsSync(cfgPath)) {
      const txt = fs.readFileSync(cfgPath, 'utf8');
      const m = txt.match(/SLOTS_PER_EPOCH\s*:\s*(\d+)/i);
      if (m) return Number(m[1]);
    }
  } catch { }
  return 6;
}
const SLOTS_PER_EPOCH = readSlotsPerEpoch();

// コンテナ名はサービス種別 + 1始まりインデックスで記録 (例: geth-1,geth-2 / prysm-1,prysm-2)
function elContainerName(i) { return `geth-${i + 1}`; }
function clContainerName(i) { return `prysm-${i + 1}`; }
const PROMETHEUS_URL = process.env.PROMETHEUS_URL || 'http://localhost:19090';

// Prometheus instance -> container name mapping
const PROM_INSTANCE_MAP = {
  'host.docker.internal:8080': 'prysm-1',
  'host.docker.internal:8081': 'prysm-2',
  'host.docker.internal:8082': 'prysm-3',
  'host.docker.internal:6060': 'geth-1',
  'host.docker.internal:6061': 'geth-2',
  'host.docker.internal:6062': 'geth-3',
};

// 分割出力用のCSVファイル（EL/CL/Validator/Performance）
let EL_CSV = '';
// let CL_CSV = ''; // Removed
let VAL_CSV = '';
let NET_CSV = '';
let PERF_CL_CSV = '';
let PERF_EL_CSV = '';

function tsBase() { return new Date().toISOString().replace(/[:.]/g, '-'); }
function initCsvFiles() {
  const dir = './metrics';
  fs.mkdirSync(dir, { recursive: true });
  const base = tsBase();
  EL_CSV = path.join(dir, `el-${base}.csv`);
  // CL_CSV = path.join(dir, `cl-${base}.csv`); // Removed
  VAL_CSV = path.join(dir, `validator-${base}.csv`);
  NET_CSV = path.join(dir, `net-${base}.csv`);
  PERF_CL_CSV = path.join(dir, `perf-cl-${base}.csv`);
  PERF_EL_CSV = path.join(dir, `perf-el-${base}.csv`);

  const elHeader = [
    // 動的指標のみ（静的/布尔は除外）
    'ts_ms', 'container_name',
    'basefee_wei', 'gasprice_wei', 'block_gas_used', 'block_gas_limit',
    'block_tx_count'
  ].join(',') + '\n';
  // CL CSV is deprecated/removed
  const valHeader = [
    // boolは出力しない
    'ts_ms', 'source_container', 'avg_effective_balance_gwei'
  ].join(',') + '\n';
  const netHeader = [
    // ネットワーク全体API由来のユニークな指標（重複/集約は含めない）
    'ts_ms',
    // Finality/進捗に関わるネットワーク状態
    'head_slot', 'current_epoch', 'justified_epoch', 'finalized_epoch', 'finality_gap_slots', 'finality_gap_epochs',
    // 集約的に意味を持つ最小限のバリデータ指標
    'validators_total', 'activation_queue_len', 'exit_queue_len', 'active_validator_count',
    'participation_rate'
  ].join(',') + '\n';

  const perfElHeader = [
    'ts_ms', 'container_name',
    'txpool_pending', 'p2p_peers', 'block_number',
    'block_exec_latency_p95',
    'cpu_seconds_total', 'memory_bytes'
  ].join(',') + '\n';

  const perfClHeader = [
    'ts_ms', 'container_name',
    'state_transition_ms', 'head_slot', 'justified_epoch', 'finalized_epoch', 'peer_count',
    'block_import_time_sum', 'active_validators',
    'cpu_seconds_total', 'memory_bytes'
  ].join(',') + '\n';

  fs.writeFileSync(EL_CSV, elHeader);
  // fs.writeFileSync(CL_CSV, clHeader); // Removed
  fs.writeFileSync(VAL_CSV, valHeader);
  fs.writeFileSync(NET_CSV, netHeader);
  fs.writeFileSync(PERF_CL_CSV, perfClHeader);
  fs.writeFileSync(PERF_EL_CSV, perfElHeader);
}

function unixMs() { return Date.now(); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function jsonRpc(url, method, params = []) {
  const t0 = Date.now();
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params })
  });
  const t1 = Date.now();
  const latency = t1 - t0;
  if (!res.ok) throw Object.assign(new Error(`${method} HTTP ${res.status}`), { latency });
  const json = await res.json();
  if (json.error) throw Object.assign(new Error(`${method} error: ${json.error.message || json.error.code}`), { latency });
  return { result: json.result, latency };
}

async function beaconGet(url, path) {
  const t0 = Date.now();
  const res = await fetch(url.replace(/\/$/, '') + path, { headers: { 'accept': 'application/json' } });
  const t1 = Date.now();
  const latency = t1 - t0;
  if (!res.ok) throw Object.assign(new Error(`${path} HTTP ${res.status}`), { latency });
  const json = await res.json();
  return { json, latency };
}

async function beaconGetWithFailover(path) {
  for (const endpoint of CL_ENDPOINTS) {
    try {
      return await beaconGet(endpoint, path);
    } catch (e) {
      // ignore and try next
    }
  }
  throw new Error(`All CL endpoints failed for ${path}`);
}

async function beaconGetRaw(url, path) {
  const t0 = Date.now();
  const res = await fetch(url.replace(/\/$/, '') + path);
  const t1 = Date.now();
  const latency = t1 - t0;
  return { status: res.status, latency };
}

// Prometheus Query Helper
async function queryPrometheus(query) {
  try {
    const url = `${PROMETHEUS_URL}/api/v1/query?query=${encodeURIComponent(query)}`;
    const res = await fetch(url);
    if (!res.ok) return [];
    const json = await res.json();
    if (json.status !== 'success') return [];
    return json.data.result || [];
  } catch (e) {
    return [];
  }
}

function csvEsc(v) {
  if (v === undefined || v === null) return '';
  const s = String(v);
  if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}
function appendCsvRow(file, columns) { fs.appendFileSync(file, columns.map(csvEsc).join(',') + '\n'); }

function hexToNum(h) { try { return Number(BigInt(h)); } catch { return null; } }

const MISSING = -1;

async function sampleEl(endpoint) {
  const ts_ms = unixMs();
  const endpoint_idx = EL_INDEX[endpoint] ?? MISSING;
  const containerName = elContainerName(endpoint_idx);
  // el_latency removed
  let basefee = MISSING, gasprice = MISSING, gasUsed = MISSING, gasLimit = MISSING;
  let errorFlag = 0;
  let blockTxCount = MISSING;
  // syncing (bool) は出力対象外のため取得省略
  try { const { result: gp } = await jsonRpc(endpoint, 'eth_gasPrice', []); gasprice = hexToNum(gp) ?? MISSING; } catch { errorFlag = 1; }
  // blockNumber removed (moved to perf)
  try { const { result: bn, latency: l1 } = await jsonRpc(endpoint, 'eth_blockNumber', []); /* el_latency = l1; blockNumber = hexToNum(bn) ?? MISSING; */ } catch { errorFlag = 1; }
  try { const { result: blk } = await jsonRpc(endpoint, 'eth_getBlockByNumber', ['latest', false]); if (blk) { basefee = hexToNum(blk.baseFeePerGas) ?? MISSING; gasUsed = hexToNum(blk.gasUsed) ?? MISSING; gasLimit = hexToNum(blk.gasLimit) ?? MISSING; } } catch { errorFlag = 1; }
  try { const { result: txc } = await jsonRpc(endpoint, 'eth_getBlockTransactionCountByNumber', ['latest']); blockTxCount = hexToNum(txc) ?? MISSING; } catch { errorFlag = 1; }

  appendCsvRow(EL_CSV, [
    ts_ms, containerName,
    basefee, gasprice, gasUsed, gasLimit,
    blockTxCount
  ]);
  return { ts_ms, containerName, basefee, gasprice, gasUsed, gasLimit, blockTxCount };
}

// sampleCl removed (all metrics moved to perf)



// New function to fetch participation rate from Prysm metrics if available, or calculate it
async function getParticipationRate() {
  // Try to get it from Prometheus if Prysm exposes it
  // metric: validator_statuses{status="active_ongoing"} is count.
  // metric: beacon_attestation_participation_ratio is not standard.
  // We will try to use a heuristic: (justified_epoch_target_balance / active_balance) from standard API?
  // Standard API: /eth/v1/beacon/states/head/committees is heavy.
  // Let's try to query Prometheus for 'beacon_attestations_aggregated_total' rate? No.

  // Let's use a simple query for now: 
  // If we can't get it easily, we return -1.
  // Actually, let's try to query 'beacon_head_participation' if it exists (some clients have it).
  const res = await queryPrometheus('beacon_head_participation');
  if (res.length > 0) return Number(res[0].value[1]);
  return -1;
}

// Override sampleNetwork to include participation rate
async function sampleNetwork() {
  // endpointはfailoverで動的に決定
  const ts_ms = unixMs();
  // head/epoch/finality 関連
  let headSlot = -1, curEpoch = -1, justEpoch = -1, finEpoch = -1;
  let finGapSlots = -1, finGapEpochs = -1;
  let cPendInit = -1, cPendQueue = -1, cActOngo = -1, cActExit = -1;
  try { const { json } = await beaconGetWithFailover('/eth/v1/beacon/headers/head'); const d = json.data || {}; headSlot = Number(d?.header?.message?.slot ?? d?.slot ?? -1); } catch { }
  try { const { json } = await beaconGetWithFailover('/eth/v1/beacon/states/head/finality_checkpoints'); const d = json.data || {}; justEpoch = Number(d?.current_justified?.epoch ?? -1); finEpoch = Number(d?.finalized?.epoch ?? -1); } catch { }
  curEpoch = (headSlot >= 0 && Number.isFinite(SLOTS_PER_EPOCH)) ? Math.floor(headSlot / SLOTS_PER_EPOCH) : -1;
  const finSlot = (finEpoch >= 0 && Number.isFinite(SLOTS_PER_EPOCH)) ? finEpoch * SLOTS_PER_EPOCH : -1;
  finGapSlots = (headSlot >= 0 && finSlot >= 0) ? Math.max(0, headSlot - finSlot) : -1;
  finGapEpochs = (curEpoch >= 0 && finEpoch >= 0) ? Math.max(0, curEpoch - finEpoch) : -1;
  async function countStatus(statuses) {
    try {
      const { json } = await beaconGetWithFailover(`/eth/v1/beacon/states/head/validators?status=${statuses}`);
      return Array.isArray(json?.data) ? json.data.length : -1;
    } catch { return -1; }
  }
  cPendInit = await countStatus('pending_initialized');
  cPendQueue = await countStatus('pending_queued');
  cActOngo = await countStatus('active_ongoing');
  cActExit = await countStatus('active_exiting');
  const total = [cPendInit, cPendQueue, cActOngo, cActExit].filter(n => n >= 0).reduce((a, b) => a + b, 0) || -1;
  const activationQueueLen = (cPendInit >= 0 && cPendQueue >= 0) ? (cPendInit + cPendQueue) : -1;
  const exitQueueLen = cActExit;
  const activeValidatorCount = cActOngo;

  const participationRate = await getParticipationRate();

  appendCsvRow(NET_CSV, [
    ts_ms,
    headSlot, curEpoch, justEpoch, finEpoch, finGapSlots, finGapEpochs,
    total, activationQueueLen, exitQueueLen, activeValidatorCount,
    participationRate
  ]);
}

async function sampleValidators() {
  const ts_ms = unixMs();
  // failover対応
  let avgBal = -1;
  let source = 'any'; // 取得できたノード
  try {
    const { json } = await beaconGetWithFailover('/eth/v1/beacon/states/head/validators?status=active');
    const arr = (json.data || []).map(v => Number(v?.balance ?? v?.effective_balance ?? -1)).filter(n => n >= 0);
    if (arr.length) {
      let sum = 0;
      for (const b of arr) { sum += b; }
      avgBal = Math.round(sum / arr.length);
    }
  } catch { }
  appendCsvRow(VAL_CSV, [ts_ms, source, avgBal]);
}

async function samplePerfMetrics() {
  const ts_ms = unixMs();
  // Query Prometheus
  // 1. Metric 1: state_transition_processing_milliseconds_sum (Prysm) OR txpool_pending (Geth)
  // 2. Metric 2: beacon_head_slot (Prysm) OR p2p_peers (Geth)
  // 3. Metric 3: beacon_current_justified_epoch (Prysm) OR chain_head_block (Geth)
  // 4. Metric 4: beacon_finalized_epoch (Prysm)
  // 5. Metric 5: p2p_peer_count (Prysm)
  // 6. Metric 6: chain_service_processing_milliseconds_sum (Prysm) OR chain_execution{quantile="0.95"} (Geth)
  // 7. Metric 7: beacon_current_active_validators (Prysm)
  const [res1, res2, res3, res4, res5, res6, res7] = await Promise.all([
    queryPrometheus('(state_transition_processing_milliseconds_sum or txpool_pending) and on(instance) up == 1'),
    queryPrometheus('(beacon_head_slot or p2p_peers) and on(instance) up == 1'),
    queryPrometheus('(beacon_current_justified_epoch or chain_head_block) and on(instance) up == 1'),
    queryPrometheus('beacon_finalized_epoch and on(instance) up == 1'),
    queryPrometheus('p2p_peer_count{state="Connected"} and on(instance) up == 1'),
    queryPrometheus('(chain_service_processing_milliseconds_sum or chain_execution{quantile="0.95"}) and on(instance) up == 1'),
    queryPrometheus('beacon_current_active_validators and on(instance) up == 1'),
    queryPrometheus('rate(process_cpu_seconds_total[1m]) and on(instance) up == 1'),
    queryPrometheus('process_resident_memory_bytes and on(instance) up == 1')
  ]);

  // Aggregate by container
  const data = {}; // container -> { m1, m2, m3, m4, m5, m6, m7, m8, m9 }

  // Initialize with all expected containers and default values
  const expectedContainers = [
    ...Object.values(PROM_INSTANCE_MAP), // geth-1..3, prysm-1..3
  ];
  for (const c of expectedContainers) {
    data[c] = { m1: MISSING, m2: MISSING, m3: MISSING, m4: MISSING, m5: MISSING, m6: MISSING, m7: MISSING, m8: MISSING, m9: MISSING };
  }

  // Helper to extract container name from metric labels
  const getContainer = (metric) => {
    const instance = metric.instance;
    return PROM_INSTANCE_MAP[instance] || instance; // Fallback to raw instance if not mapped
  };

  const update = (res, key) => {
    res.forEach(r => {
      const c = getContainer(r.metric);
      if (data[c]) { // Only update if container is expected (or already in data)
        data[c][key] = Number(r.value[1]);
      }
    });
  };

  update(res1, 'm1');
  update(res2, 'm2');
  update(res3, 'm3');
  update(res4, 'm4');
  update(res5, 'm5');
  update(res6, 'm6');
  update(res7, 'm7');
  update(res8, 'm8'); // CPU
  update(res9, 'm9'); // Memory

  // Write rows
  for (const [container, metrics] of Object.entries(data)) {
    if (container.startsWith('prysm')) {
      appendCsvRow(PERF_CL_CSV, [ts_ms, container, metrics.m1, metrics.m2, metrics.m3, metrics.m4, metrics.m5, metrics.m6, metrics.m7, metrics.m8, metrics.m9]);
    } else if (container.startsWith('geth')) {
      appendCsvRow(PERF_EL_CSV, [ts_ms, container, metrics.m1, metrics.m2, metrics.m3, metrics.m6, metrics.m8, metrics.m9]);
    }
  }
}

async function main() {
  // graceful stop on Ctrl+C / SIGTERM
  let stop = false;
  process.on('SIGINT', () => { stop = true; });
  process.on('SIGTERM', () => { stop = true; });
  initCsvFiles();
  const endAt = DURATION_SEC > 0 ? Date.now() + DURATION_SEC * 1000 : Number.POSITIVE_INFINITY;
  // 固定レート(既定1秒)でサンプリング: 処理時間を含めて厳密に1Hzを目指す
  let nextAt = Date.now();
  while (!stop && Date.now() < endAt) {
    await Promise.allSettled([
      ...EL_ENDPOINTS.map(e => sampleEl(e)),
      // ...CL_ENDPOINTS.map(b => sampleCl(b)), // Removed
      sampleValidators(),
      sampleNetwork(),
      samplePerfMetrics(),
    ]);
    nextAt += INTERVAL_MS; // 次のターゲット時刻
    const now = Date.now();
    const delay = Math.max(0, nextAt - now);
    if (delay > 0) await sleep(delay);
  }
  console.log('metrics-sample2: done');
}

main().catch(e => { console.error('metrics-sample2 fatal:', e); process.exit(1); });
