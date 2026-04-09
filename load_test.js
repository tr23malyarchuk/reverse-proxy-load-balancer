import http from 'k6/http';
import { check, sleep } from 'k6';

// Загружаем WAV-файл для /file-request (бинарно)
const wavFile = open('./sample.wav', 'b');

export const options = {
  scenarios: {
    // 1000 запросов на /request (JSON)
    json_requests: {
      executor: 'per-vu-iterations',
      vus: 10,
      iterations: 100, // 10 VU * 100 = 1000 запросов
      maxDuration: '5m',
      exec: 'jsonScenario',
    },
    // 300 запросов на /file-request (multipart + файл)
    file_requests: {
      executor: 'per-vu-iterations',
      vus: 10,
      iterations: 30, // 10 VU * 30 = 300 запросов
      startTime: '10s', // стартуем чуть позже, чтобы сервер "проснулся"
      maxDuration: '5m',
      exec: 'fileScenario',
    },
  },
};

const algorithms = [
  'round_robin',
  'random',
  'least_connections',
  'ip_hash',
  'power_of_two',
];

// --- Сценарий для /request (JSON) ---

export function jsonScenario() {
  const algo = algorithms[Math.floor(Math.random() * algorithms.length)];

  const url = 'http://localhost:8000/request';
  const payload = JSON.stringify({
    algorithm: algo,
    processing_time: 0.5,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  sleep(0.1);
}

// --- Сценарий для /file-request (multipart/form-data + файл) ---

export function fileScenario() {
  const algo = algorithms[Math.floor(Math.random() * algorithms.length)];

  const url = 'http://localhost:8000/file-request';

  const formData = {
    file: http.file(wavFile, 'sample.wav', 'audio/wav'),
    algorithm: algo,
  };

  const res = http.post(url, formData);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  sleep(0.1);
}

