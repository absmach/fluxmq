import {
  DocsBody,
  DocsDescription,
  DocsPage,
  DocsTitle,
} from "fumadocs-ui/layouts/docs/page";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getPageImage, source } from "@/lib/source";
import { getMDXComponents } from "@/mdx-components";
import { RootRedirect } from "./root-redirect";

export default async function Page(props: PageProps<"/[[...slug]]">) {
  const params = await props.params;
  const slug = params.slug ?? [];
  const page = source.getPage(slug);

  if (!page) {
    if (slug.length === 0) return <RootRedirect />;
    notFound();
  }

  const MDX = page.data.body;

  return (
    <DocsPage toc={page.data.toc} full={page.data.full}>
      <DocsTitle>{page.data.title}</DocsTitle>
      <DocsDescription>{page.data.description}</DocsDescription>
      <DocsBody>
        <MDX
          components={getMDXComponents({
            a: createRelativeLink(source, page),
          })}
        />
      </DocsBody>
    </DocsPage>
  );
}

export async function generateStaticParams() {
  return [{ slug: [] }, ...source.generateParams()];
}

export async function generateMetadata(
  props: PageProps<"/[[...slug]]">,
): Promise<Metadata> {
  const params = await props.params;
  const slug = params.slug ?? [];
  const page = source.getPage(slug);
  if (!page) return { title: "FluxMQ Docs" };

  return {
    title: page.data.title,
    description: page.data.description,
    openGraph: {
      images: getPageImage(page).url,
    },
  };
}
