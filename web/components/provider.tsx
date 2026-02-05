'use client';
import SearchDialog from '@/components/search';
import { RootProvider } from 'fumadocs-ui/provider/next';
import { type ReactNode } from 'react';

export function Provider({ children }: { children: ReactNode }) {
  // @ts-expect-error - fumadocs type definitions don't match implementation for children prop
  return <RootProvider search={{ SearchDialog }}>{children}</RootProvider>;
}
