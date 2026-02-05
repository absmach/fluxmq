import { type Metadata } from 'next';
import { Provider } from '@/components/provider';
import './global.css';

export const metadata: Metadata = {
  title: {
    default: 'FluxMQ — High-Performance Multi-Protocol Message Broker',
    template: '%s | FluxMQ',
  },
  description: 'FluxMQ is a high-performance, multi-protocol message broker written in Go. Supports MQTT 3.1.1/5.0, WebSocket, HTTP and CoAP with durable queues, clustering, and event-driven architecture.',
  keywords: ['FluxMQ', 'MQTT broker', 'message broker', 'IoT', 'high-performance', 'durable queues', 'clustering', 'event-driven', 'open-source', 'Go'],
  authors: [{ name: 'Abstract Machines' }],
  openGraph: {
    type: 'website',
    siteName: 'FluxMQ',
    title: 'FluxMQ — High-Performance Multi-Protocol Message Broker',
    description: 'High-performance, multi-protocol message broker for IoT and event-driven architectures. MQTT 3.1.1/5.0, WebSocket, HTTP, CoAP with durable queues and clustering.',
    images: [
      {
        url: '/og-image.png',
        width: 1200,
        height: 630,
        alt: 'FluxMQ - Multi-Protocol Message Broker',
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    site: '@absmach',
    title: 'FluxMQ — High-Performance Multi-Protocol Message Broker',
    description: 'FluxMQ is a high-performance, multi-protocol message broker written in Go. Supports MQTT 3.1.1/5.0, WebSocket, HTTP and CoAP with durable queues, clustering, and event-driven architecture.',
    images: ['/og-image.png'],
  },
  robots: {
    index: true,
    follow: true,
  },
};

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <link rel="icon" href="/favicon.ico" sizes="any" />
        <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
        <link rel="apple-touch-icon" href="/apple-touch-icon.png" />
        <link rel="canonical" href="https://absmach.eu/fluxmq/" />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify({
              '@context': 'https://schema.org',
              '@type': 'SoftwareApplication',
              name: 'FluxMQ',
              operatingSystem: 'Linux, Multi-platform',
              applicationCategory: 'DeveloperApplication',
              description: 'FluxMQ is a high-performance, multi-protocol message broker for IoT and event-driven architectures, supporting MQTT 3.1.1/5.0, WebSocket, HTTP and CoAP.',
              offers: {
                '@type': 'Offer',
                price: '0',
                priceCurrency: 'USD',
              },
              creator: {
                '@type': 'Organization',
                name: 'Abstract Machines',
                url: 'https://absmach.eu',
              },
            }),
          }}
        />
      </head>
      <body className="flex flex-col min-h-screen" style={{ fontFamily: 'Verdana, Geneva, sans-serif' }}>
        <Provider>{children}</Provider>
      </body>
    </html>
  );
}
