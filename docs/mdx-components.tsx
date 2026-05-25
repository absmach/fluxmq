import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";
import { APIPage } from "@/components/mdx/api-page";
import { Mermaid } from "@/components/mdx/mermaid";
import { assetPath } from "@/lib/base-path";

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    Mermaid,
    APIPage,
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
