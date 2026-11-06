// Loopback proxy: /api/* to the backend (prefix stripped, as the dev rewrite does), everything else to `next start`.
// usage: node proxy.mjs <listen port> <next port> <backend port>
import http from "node:http";

const [listen, nextPort, backendPort] = process.argv.slice(2).map(Number);
http.createServer((request, response) => {
  const toBackend = request.url.startsWith("/api/");
  const upstream = http.request({
    host: "127.0.0.1", port: toBackend ? backendPort : nextPort, method: request.method,
    path: toBackend ? request.url.slice(4) : request.url, headers: request.headers,
  }, (reply) => {
    response.writeHead(reply.statusCode, reply.headers);
    reply.pipe(response);
  });
  upstream.on("error", () => { response.writeHead(502); response.end(); });
  request.pipe(upstream);
}).listen(listen, "127.0.0.1");
