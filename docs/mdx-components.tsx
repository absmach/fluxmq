import type { InferPageType } from "fumadocs-core/source";
import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";
import { APIPage } from "@/components/mdx/api-page";
import { Mermaid } from "@/components/mdx/mermaid";
import { assetPath } from "@/lib/base-path";
import { openapi } from "@/lib/openapi";
import type { source } from "@/lib/source";

export function getMDXComponents(
  page: InferPageType<typeof source>,
  components?: MDXComponents,
): MDXComponents {
  // fumadocs-openapi v11 moved document bundling to the server: the generated
  // MDX renders <OpenAPIPage document=... operations=... /> and the server must
  // inject the bundled document via `preloaded`. Wrap the client component so
  // each OpenAPI page is preloaded for the page currently being rendered.
  const OpenAPIPage = async (props: React.ComponentProps<typeof APIPage>) => (
    <APIPage {...(await openapi.preloadOpenAPIPage(page))} {...props} />
  );

  return {
    ...defaultMdxComponents,
    Mermaid,
    APIPage: OpenAPIPage,
    OpenAPIPage,
    img: (props) => {
      const src =
        typeof props.src === "string" ? assetPath(props.src) : props.src;
      const Image = defaultMdxComponents.img;
      if (Image) return <Image {...props} src={src} />;
      return null;
    },
    ...components,
  };
}
