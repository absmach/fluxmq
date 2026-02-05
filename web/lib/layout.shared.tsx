import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: 'FluxMQ',
      url: '/',
    },
    links: [
      {
        text: 'Features',
        url: '/#features',
      },
      {
        text: 'Performance',
        url: '/#performance',
      },
      {
        text: 'Use Cases',
        url: '/#use-cases',
      },
      {
        text: 'Quick Start',
        url: '/#quick-start',
      },
      {
        text: 'Documentation',
        url: '/docs',
      },
      {
        text: 'GitHub',
        url: 'https://github.com/absmach/fluxmq',
        external: true,
      },
    ],
  };
}
