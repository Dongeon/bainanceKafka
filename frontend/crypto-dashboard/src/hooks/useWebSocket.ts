import { useEffect, useRef, useState } from 'react';
import { Client } from '@stomp/stompjs';
import { Ticker } from '../types';
import { WS_URL } from '../constants';

export function useTickerStream() {
  const [tickers, setTickers] = useState<Record<string, Ticker>>({});
  const [connected, setConnected] = useState(false);
  const clientRef = useRef<Client | null>(null);

  useEffect(() => {
    const client = new Client({
      brokerURL: WS_URL,
      onConnect: () => {
        setConnected(true);
        client.subscribe('/topic/tickers', (msg) => {
          const data: Ticker[] = JSON.parse(msg.body);
          setTickers(prev => {
            const next = { ...prev };
            data.forEach(t => { next[t.symbol] = t; });
            return next;
          });
        });
      },
      onDisconnect: () => setConnected(false),
      reconnectDelay: 5000,
    });
    client.activate();
    clientRef.current = client;
    return () => { client.deactivate(); };
  }, []);

  return { tickers, connected };
}
