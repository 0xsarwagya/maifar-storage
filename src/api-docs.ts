import { buildOpenApiDocument } from "./openapi-spec";

const SWAGGER_UI_CSS =
  "https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css";
const SWAGGER_UI_BUNDLE =
  "https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js";

/** Pinned @scalar/api-reference browser bundle (see package `browser` / `exports`). */
const SCALAR_STANDALONE =
  "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.49.5/dist/browser/standalone.js";

const REDOC_SCRIPT =
  "https://cdn.jsdelivr.net/npm/redoc@2.1.5/bundles/redoc.standalone.js";

function htmlPage(title: string, body: string): Response {
  const doc = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${title}</title>
</head>
<body>
${body}
</body>
</html>`;
  return new Response(doc, {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function swaggerUiHtml(): Response {
  return htmlPage(
    "API docs — Swagger UI",
    `<div id="swagger-ui"></div>
<link rel="stylesheet" href="${SWAGGER_UI_CSS}" crossorigin="anonymous" />
<script src="${SWAGGER_UI_BUNDLE}" crossorigin="anonymous"></script>
<script>
  window.onload = function () {
    var specUrl = new URL("/openapi.json", window.location.href).href;
    window.ui = SwaggerUIBundle({
      url: specUrl,
      dom_id: "#swagger-ui",
      deepLinking: true,
      persistAuthorization: true,
    });
  };
</script>`,
  );
}

function scalarHtml(): Response {
  return htmlPage(
    "API docs — Scalar",
    `<script id="api-reference"></script>
<script>
  (function () {
    var el = document.getElementById("api-reference");
    el.setAttribute("data-url", new URL("/openapi.json", window.location.href).href);
  })();
</script>
<script src="${SCALAR_STANDALONE}" crossorigin="anonymous"></script>`,
  );
}

function redocHtml(): Response {
  return htmlPage(
    "API docs — ReDoc",
    `<redoc id="redoc-el"></redoc>
<script>
  document.getElementById("redoc-el").setAttribute(
    "spec-url",
    new URL("/openapi.json", window.location.href).href,
  );
</script>
<script src="${REDOC_SCRIPT}" crossorigin="anonymous"></script>`,
  );
}

/**
 * OpenAPI JSON and interactive docs (Swagger UI, Scalar, ReDoc).
 * Returns `null` if the request is not handled here.
 */
export function tryServeApiDocs(req: Request): Response | null {
  if (req.method !== "GET") return null;

  const url = new URL(req.url);
  const path = url.pathname;

  if (path === "/openapi.json") {
    const origin = url.origin;
    const doc = buildOpenApiDocument(origin);
    return Response.json(doc, {
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": "application/vnd.oai.openapi+json; charset=utf-8",
      },
    });
  }

  if (path === "/docs" || path === "/docs/") {
    return swaggerUiHtml();
  }

  if (path === "/scalar" || path === "/scalar/") {
    return scalarHtml();
  }

  if (path === "/redoc" || path === "/redoc/") {
    return redocHtml();
  }

  return null;
}
