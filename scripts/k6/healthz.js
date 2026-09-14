import http from "k6/http";
import { check, sleep } from "k6";

const baseURL = __ENV.K6_BASE_URL || "http://127.0.0.1:8080";

export const options = {
  scenarios: {
    health: {
      executor: "constant-vus",
      vus: 20,
      duration: "15s",
      gracefulStop: "2s",
    },
  },
  thresholds: {
    checks: ["rate==1"],
    http_req_failed: ["rate==0"],
    http_req_duration: ["p(95)<250"],
  },
};

export default function () {
  const response = http.get(`${baseURL}/healthz`, {
    tags: { name: "healthz" },
  });

  check(response, {
    "healthz returns 200": (result) => result.status === 200,
    "healthz returns expected body": (result) => result.body === '{"status":"ok"}\n',
  });

  sleep(0.1);
}
