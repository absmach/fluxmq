export const clientURL = (client: { client_id: string; node_id: string }) =>
  `/dashboard/sessions/details?${new URLSearchParams({ id: client.client_id, node: client.node_id })}`;
export const subscriptionURL = (filter: string, node = 'all') =>
  `/dashboard/subscriptions/details?${new URLSearchParams({ filter, node })}`;
export function followLink(event: MouseEvent, action: () => void) {
  if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey)
    return;
  event.preventDefault();
  action();
}
