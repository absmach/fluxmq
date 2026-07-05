"use client";

import { createOpenAPIPage } from "fumadocs-openapi/ui";

// fumadocs-openapi v11 renamed `createAPIPage` to `createOpenAPIPage`, no longer
// binds a server instance here (each generated MDX page passes its `document`
// and operations as props), and ships `fumadocs-openapi/ui` as a client module —
// so the factory must be called from a client component.
export const APIPage = createOpenAPIPage();
