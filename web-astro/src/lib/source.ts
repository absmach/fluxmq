import type { Source } from 'fumadocs-core/source';
import { loader } from 'fumadocs-core/source';
import { type CollectionEntry, getCollection } from 'astro:content';
import * as path from 'node:path';
import type { StructuredData } from 'fumadocs-core/mdx-plugins';

export const source = loader({
  source: await createSource(),
  baseUrl: '/docs',
});

const docs = import.meta.glob('/src/content/docs/**/*.{md,mdx}');

export async function getFullExport(entry: CollectionEntry<'docs'>) {
  return (await docs['/' + entry.filePath!]()) as {
    structuredData: StructuredData;
  };
}

async function createSource() {
  const out: Source<{
    metaData: CollectionEntry<'meta'>['data'];
    pageData: CollectionEntry<'docs'>['data'] & {
      _raw: CollectionEntry<'docs'>;
    };
  }> = {
    files: [],
  };

  for (const page of await getCollection('docs')) {
    const virtualPath = path.relative('src/content/docs', page.filePath!);

    out.files.push({
      type: 'page',
      path: virtualPath,
      data: {
        ...page.data,
        _raw: page,
      },
    });
  }

  for (const meta of await getCollection('meta')) {
    const virtualPath = path.relative('src/content/docs', meta.filePath!);

    out.files.push({
      type: 'meta',
      path: virtualPath,
      data: meta.data,
    });
  }

  return out;
}
