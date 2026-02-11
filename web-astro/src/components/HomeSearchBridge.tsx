import { useEffect } from 'react';
import { FrameworkProvider } from 'fumadocs-core/framework';
import { useSearchContext } from 'fumadocs-ui/contexts/search';
import { RootProvider } from 'fumadocs-ui/provider/base';
import SearchDialog from '@/components/search';

declare global {
  interface Window {
    __fluxmqOpenSearch?: () => void;
  }
}

function HomeSearchBridgeInner() {
  const { setOpenSearch } = useSearchContext();

  useEffect(() => {
    const handler = () => setOpenSearch(true);
    window.__fluxmqOpenSearch = handler;
    window.addEventListener('fluxmq:open-search', handler);
    return () => {
      delete window.__fluxmqOpenSearch;
      window.removeEventListener('fluxmq:open-search', handler);
    };
  }, [setOpenSearch]);

  return null;
}

export default function HomeSearchBridge() {
  return (
    <FrameworkProvider
      usePathname={() => window.location.pathname}
      useParams={() => ({})}
      useRouter={() => ({
        push(nextPathname) {
          window.location.assign(nextPathname);
        },
        refresh() {
          window.location.reload();
        },
      })}
    >
      <RootProvider search={{ SearchDialog }}>
        <HomeSearchBridgeInner />
      </RootProvider>
    </FrameworkProvider>
  );
}
