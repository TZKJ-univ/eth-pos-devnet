#!/usr/bin/env node
import fs from 'node:fs';
import { execSync } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';

const ENV_PATH = new URL('../.env', import.meta.url).pathname;
const BEACON_SERVICES = ['prysm', 'prysm-2', 'prysm-3'];

async function fetchIdentity(url) {
  const res = await fetch(`${url}/eth/v1/node/identity`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return (await res.json())?.data;
}

async function waitForEnr(service, timeoutMs = 90000) {
  const start = Date.now();
  const url = serviceUrl(service);
  while (Date.now() - start < timeoutMs) {
    try {
      const data = await fetchIdentity(url);
      const enr = data?.enr;
      if (enr) return data;
    } catch { }
    await sleep(2000);
  }
  throw new Error(`timeout waiting ENR for ${service}`);
}

function serviceUrl(service) {
  // ports: prysm-1:3500, prysm-2:3500 (mapped 3502), prysm-3:3500 (mapped 3503)
  const map = { prysm: 'http://127.0.0.1:3500', 'prysm-2': 'http://127.0.0.1:3502', 'prysm-3': 'http://127.0.0.1:3503' };
  return map[service];
}

function containerIp(service) {
  try {
    const id = execSync(`docker compose ps -q ${service}`, { encoding: 'utf8' }).trim();
    if (!id) return null;
    const ip = execSync(`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' ${id}`, { encoding: 'utf8' }).trim();
    return ip.split(' ')[0] || null;
  } catch { return null; }
}

function patchEnrIp(enr, ip) {
  if (!ip) return enr;
  return enr.replace(/(\/ip4\/)(\d+\.\d+\.\d+\.\d+)(\/)/g, `$1${ip}$3`);
}

function upsertEnv(path, key, value) {
  let content = '';
  if (fs.existsSync(path)) content = fs.readFileSync(path, 'utf8');
  const lines = content.split(/\r?\n/).filter(Boolean);
  const kv = `${key}=${value}`;
  const idx = lines.findIndex(l => l.startsWith(`${key}=`));
  if (idx >= 0) lines[idx] = kv; else lines.push(kv);
  fs.writeFileSync(path, lines.join('\n') + '\n');
}

async function main() {
  const enrList = [];
  // Collect all multiaddrs first
  const multiaddrs = {}; // service -> multiaddr
  for (const s of BEACON_SERVICES) {
    try {
      const data = await waitForEnr(s);
      const ip = containerIp(s);
      const enr = patchEnrIp(data.enr, ip);

      const addrs = Array.isArray(data.p2p_addresses) ? data.p2p_addresses : [];
      const peer = addrs.find(a => /\/ip4\//.test(a)) || addrs[0];

      if (peer) {
        // Use service name as DNS name
        const dnsName = s === 'prysm' ? 'prysm' : s;
        const peerPatched = peer.replace(/(\/ip4\/)(\d+\.\d+\.\d+\.\d+)(\/)/, `/dns4/${dnsName}$3`);
        multiaddrs[s] = peerPatched;
      }

      // Still write individual ENRs/Peers for backward compatibility or other uses
      enrList.push(enr); // Note: using patched ENR string here, though multiaddr is preferred for peering
      upsertEnv(ENV_PATH, `PRYSM_BOOTSTRAP_ENR_${enrList.length}`, enr);

    } catch (e) { console.error('bootstrap-enr-all:', s, e.message); }
  }

  // Generate peer lists excluding self
  const services = BEACON_SERVICES; // ['prysm', 'prysm-2', 'prysm-3']
  services.forEach((s, i) => {
    const peers = services
      .filter(other => other !== s) // Exclude self
      .map(other => multiaddrs[other])
      .filter(Boolean); // Filter out undefined if fetch failed

    const envVar = `PRYSM_PEERS_${i + 1}`; // PRYSM_PEERS_1, _2, _3
    upsertEnv(ENV_PATH, envVar, peers.join(','));
    console.log(`bootstrap-enr-all: wrote ${envVar} with ${peers.length} peers`);
  });

  if (enrList.length) {
    upsertEnv(ENV_PATH, 'PRYSM_BOOTSTRAP_ENR', enrList.join(','));
  }
  console.log('bootstrap-enr-all: wrote multi ENRs to .env');
}

main().catch(e => { console.error(e); process.exit(1); });
