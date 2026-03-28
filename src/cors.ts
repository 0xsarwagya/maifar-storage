/** Merge CORS headers for browser / SPA / doc UIs calling this API from another origin. */
export function withCors(req: Request, res: Response): Response {
  const origin = resolveAllowOrigin(req);
  const headers = new Headers(res.headers);
  headers.set("Access-Control-Allow-Origin", origin);
  headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
  headers.append("Vary", "Origin");
  const reqHeaders = req.headers.get("Access-Control-Request-Headers");
  if (reqHeaders) {
    headers.set("Access-Control-Allow-Headers", reqHeaders);
  } else {
    headers.set(
      "Access-Control-Allow-Headers",
      "Content-Type, Authorization, Accept",
    );
  }
  headers.set("Access-Control-Max-Age", "86400");
  return new Response(res.body, {
    status: res.status,
    statusText: res.statusText,
    headers,
  });
}

export function corsPreflightResponse(req: Request): Response {
  return withCors(
    req,
    new Response(null, {
      status: 204,
      headers: { "Cache-Control": "no-store" },
    }),
  );
}

/** `CORS_ORIGIN`: e.g. `https://app.example.com`. Default `*` (public read API). */
function resolveAllowOrigin(_req: Request): string {
  const configured = process.env.CORS_ORIGIN?.trim();
  if (configured && configured !== "") {
    return configured;
  }
  return "*";
}
