import http from 'k6/http';
import { check, sleep } from 'k6';

const wavFile  = open('../data/sample.wav',  'b');
const pdfFile  = open('../data/sample.pdf',  'b');
const webpFile = open('../data/sample.webp', 'b');
const rarFile  = open('../data/sample.rar',  'b');

const BASE_URL = 'http://localhost:8000';

const algorithms = [
  'round_robin',
  'random',
  'least_connections',
  'ip_hash',
  'power_of_two',
];

const ALG_FROM_ENV = __ENV.LB_ALG; // LB_ALG=least_connections k6 run load_test.js

function pickAlgorithm() {
  if (ALG_FROM_ENV && algorithms.includes(ALG_FROM_ENV)) {
    return ALG_FROM_ENV;
  }
  return algorithms[Math.floor(Math.random() * algorithms.length)];
}

export const options = {
  scenarios: {
    wav2mp3_requests: {
      executor: 'per-vu-iterations',
      vus: 20,
      iterations: 50,
      maxDuration: '5m',
      exec: 'wav2mp3Scenario',
    },
    pdf2png_requests: {
      executor: 'per-vu-iterations',
      vus: 20,
      iterations: 50,
      startTime: '5s',
      maxDuration: '5m',
      exec: 'pdf2pngScenario',
    },
    webp2png_requests: {
      executor: 'per-vu-iterations',
      vus: 20,
      iterations: 50,
      startTime: '10s',
      maxDuration: '5m',
      exec: 'webp2pngScenario',
    },
    rar2zip_requests: {
      executor: 'per-vu-iterations',
      vus: 20,
      iterations: 50,
      startTime: '15s',
      maxDuration: '5m',
      exec: 'rar2zipScenario',
    },
  },
};

// WAV -> MP3
export function wav2mp3Scenario() {
  const algo = pickAlgorithm();
  const res = http.post(`${BASE_URL}/wav2mp3`, {
    file: http.file(wavFile, 'sample.wav', 'audio/wav'),
    algorithm: algo,
  });
  check(res, { 'wav2mp3 status is 200': (r) => r.status === 200 });
  sleep(0.1);
}

// PDF -> PNG
export function pdf2pngScenario() {
  const algo = pickAlgorithm();
  const res = http.post(`${BASE_URL}/pdf2png`, {
    file: http.file(pdfFile, 'sample.pdf', 'application/pdf'),
    algorithm: algo,
  });
  check(res, { 'pdf2png status is 200': (r) => r.status === 200 });
  sleep(0.1);
}

// WEBP -> PNG
export function webp2pngScenario() {
  const algo = pickAlgorithm();
  const res = http.post(`${BASE_URL}/webp2png`, {
    file: http.file(webpFile, 'sample.webp', 'image/webp'),
    algorithm: algo,
  });
  check(res, { 'webp2png status is 200': (r) => r.status === 200 });
  sleep(0.1);
}

// RAR -> ZIP
export function rar2zipScenario() {
  const algo = pickAlgorithm();
  const res = http.post(`${BASE_URL}/ziprar`, {
    file: http.file(rarFile, 'sample.rar', 'application/vnd.rar'),
    algorithm: algo,
  });
  check(res, { 'rar2zip status is 200': (r) => r.status === 200 });
  sleep(0.1);
}
