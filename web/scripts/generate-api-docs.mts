import { generateFiles } from "fumadocs-openapi";
import { createOpenAPI } from "fumadocs-openapi/server";
import path from "node:path";

const openapi = createOpenAPI({
	input: ["../api/openapi.yaml"],
});

await generateFiles({
	input: openapi,
	output: path.resolve(import.meta.dirname, "../content/docs/reference/admin-api"),
	per: "tag",
	addGeneratedComment: true,
});

console.log("API docs generated");
