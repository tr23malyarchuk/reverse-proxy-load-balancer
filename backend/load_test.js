import http from 'k6/http';
import { check, sleep } from 'k6';

const wavFile  = open('data/input/sample.wav',  'b');
const pdfFile  = open('data/input/sample.pdf',  'b');
const webpFile = open('data/input/sample.webp', 'b');
const rarFile  = open('data/input/sample.rar',  'b');

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
      vus: 5,
      iterations: 50,      // 250
      maxDuration: '5m',
      exec: 'wav2mp3Scenario',
    },
    pdf2png_requests: {
      executor: 'per-vu-iterations',
      vus: 5,
      iterations: 50,      // 250
      startTime: '5s',
      maxDuration: '5m',
      exec: 'pdf2pngScenario',
    },
    webp2png_requests: {
      executor: 'per-vu-iterations',
      vus: 5,
      iterations: 50,      // 250
      startTime: '10s',
      maxDuration: '5m',
      exec: 'webp2pngScenario',
    },
    rar2zip_requests: {
      executor: 'per-vu-iterations',
      vus: 5,
      iterations: 50,      // 250
      startTime: '15s',
      maxDuration: '5m',
      exec: 'rar2zipScenario',
    },
  },
};

// WAV -> MP3
export function wav2mp3Scenario() {
  const algo = pickAlgorithm();
  const url = 'http://localhost:9001/convert/wav-to-mp3';

  const formData = {
    file: http.file(wavFile, 'sample.wav', 'audio/wav'),
    algorithm: algo,
  };

  const res = http.post(url, formData);

  check(res, {
    'wav2mp3 status is 200': (r) => r.status === 200,
  });

  sleep(0.1);
}

// PDF -> PNG
export function pdf2pngScenario() {
  const algo = pickAlgorithm();
  const url = 'http://localhost:9002/convert/pdf-to-png';

  const formData = {
    file: http.file(pdfFile, 'sample.pdf', 'application/pdf'),
    algorithm: algo,
  };

  const res = http.post(url, formData);

  check(res, {
    'pdf2png status is 200': (r) => r.status === 200,
  });

  sleep(0.1);
}

// WEBP -> PNG
export function webp2pngScenario() {
  const algo = pickAlgorithm();
  const url = 'http://localhost:9003/convert/webp-to-png';

  const formData = {
    file: http.file(webpFile, 'sample.webp', 'image/webp'),
    algorithm: algo,
  };

  const res = http.post(url, formData);

  check(res, {
    'webp2png status is 200': (r) => r.status === 200,
  });

  sleep(0.1);
}

// RAR -> ZIP
export function rar2zipScenario() {
  const algo = pickAlgorithm();
  const url = 'http://localhost:9005/convert/rar-to-zip';

  const formData = {
    file: http.file(rarFile, 'sample.rar', 'application/vnd.rar'),
    algorithm: algo,
  };

  const res = http.post(url, formData);

  check(res, {
    'rar2zip status is 200': (r) => r.status === 200,
  });

  sleep(0.1);
}

