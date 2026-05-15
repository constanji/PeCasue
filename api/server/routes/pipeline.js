/**
 * Reverse proxy: /api/pipeline/* -> Pipeline FastAPI sidecar (default :8001).
 *
 * Implementation notes:
 *  - Uses Node's built-in `http` so we can stream multipart uploads (Phase 2)
 *    and SSE responses (Phase 4) without buffering.
 *  - Re-applies the original `/api/pipeline/...` path so the sidecar mounts
 *    routers at the same prefix (see pipeline-svc/server/main.py).
 *  - All requests are gated by `requireJwtAuth` + admin role here so the sidecar
 *    can trust traffic from this router (Phase 6 will extend to FINANCE role).
 */
const express = require('express');
const http = require('http');
const { URL } = require('url');
const { requireJwtAuth, checkAdmin } = require('../middleware');
const { logger } = require('@because/data-schemas');

const router = express.Router();

const PIPELINE_SVC_URL =
  process.env.PIPELINE_SVC_URL ||
  `http://${process.env.PIPELINE_SVC_HOST || 'localhost'}:${
    process.env.PIPELINE_SVC_PORT || '8001'
  }`;

router.use(requireJwtAuth, checkAdmin);

// Expose a tiny info endpoint for debugging.
router.get('/_proxy/info', (req, res) => {
  res.json({
    upstream: PIPELINE_SVC_URL,
    proxied_via: '/api/pipeline',
    user: { id: req.user?.id, role: req.user?.role },
  });
});

router.use((req, res) => {
  let upstream;
  try {
    upstream = new URL(PIPELINE_SVC_URL);
  } catch (err) {
    logger.error('[pipeline-proxy] Invalid PIPELINE_SVC_URL', err);
    return res.status(500).json({ message: 'Pipeline upstream is misconfigured' });
  }

  const upstreamPath = `/api/pipeline${req.url}`;

  // Detect SSE endpoints so we can bypass Express compression buffering.
  const isSse = req.url.includes('/stream');

  if (isSse) {
    // Tell compression() to skip this response entirely.
    res.setHeader('Content-Encoding', 'identity');
    // Also signal compression middleware directly.
    res.noCompression = true;
    // Nginx reverse-proxy: disable proxy buffering for SSE.
    res.setHeader('X-Accel-Buffering', 'no');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    // Disable Transfer-Encoding chunking to ensure SSE text/event-stream flushes immediately.
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  }

  const headers = { ...req.headers };
  const method = String(req.method || 'GET').toUpperCase();
  const contentType = String(req.headers['content-type'] || '');
  const hasParsedJsonBody =
    method !== 'GET' &&
    method !== 'HEAD' &&
    req.body != null &&
    typeof req.body === 'object' &&
    !Buffer.isBuffer(req.body) &&
    contentType.includes('application/json');

  let jsonBodyBuffer = null;
  if (hasParsedJsonBody) {
    jsonBodyBuffer = Buffer.from(JSON.stringify(req.body));
    headers['content-type'] = 'application/json';
    headers['content-length'] = String(jsonBodyBuffer.length);
  }

  delete headers['host'];
  delete headers['connection'];
  if (!jsonBodyBuffer) {
    delete headers['content-length'];
  }
  // Forward the authenticated user so the sidecar can attribute audit events.
  if (req.user) {
    headers['x-pecause-user-id'] = String(req.user.id || '');
    headers['x-pecause-user-role'] = String(req.user.role || '');
    if (req.user.email) headers['x-pecause-user-email'] = String(req.user.email);
  }

  const upstreamReq = http.request(
    {
      protocol: upstream.protocol,
      hostname: upstream.hostname,
      port: upstream.port,
      method: req.method,
      path: upstreamPath,
      headers,
    },
    (upstreamRes) => {
      res.status(upstreamRes.statusCode || 502);
      const respHeaders = { ...upstreamRes.headers };
      delete respHeaders['transfer-encoding'];
      Object.entries(respHeaders).forEach(([k, v]) => {
        if (v !== undefined) res.setHeader(k, v);
      });
      upstreamRes.pipe(res);
    },
  );

  upstreamReq.on('error', (err) => {
    logger.warn(
      `[pipeline-proxy] Upstream error for ${req.method} ${upstreamPath}: ${err.message}`,
    );
    if (!res.headersSent) {
      res.status(502).json({
        message: 'Pipeline service unavailable',
        upstream: PIPELINE_SVC_URL,
        detail: err.message,
      });
    } else {
      res.end();
    }
  });

  req.on('aborted', () => upstreamReq.destroy());
  if (jsonBodyBuffer) {
    upstreamReq.end(jsonBodyBuffer);
  } else {
    req.pipe(upstreamReq);
  }
});

module.exports = router;
