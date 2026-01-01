"use client";

import { useEffect, useState } from "react";
import { io } from "socket.io-client";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { ArrowUpRight, ArrowDownRight, Minus, Terminal } from "lucide-react";

// Define the shape of our data
interface MarketNews {
  id: string;
  headline: string;
  source: string;
  timestamp: string;
  analysis: string; // The raw JSON string from Ollama
}

interface ParsedAnalysis {
  sentiment: "BULLISH" | "BEARISH" | "NEUTRAL";
  reason: string;
}

export default function Home() {
  const [newsFeed, setNewsFeed] = useState<MarketNews[]>([]);
  const [isConnected, setIsConnected] = useState(false);

  useEffect(() => {
    // 1. Connect to our Bridge Server
    const socket = io("http://localhost:3001");

    socket.on("connect", () => {
      console.log("Connected to WebSocket Bridge");
      setIsConnected(true);
    });

    socket.on("disconnect", () => {
      console.log("Disconnected");
      setIsConnected(false);
    });

    // 2. Listen for Real-Time Events
    socket.on("market_event", (data: string) => {
      try {
        const parsedData: MarketNews = JSON.parse(data);
        // Add new item to the top of the list
        setNewsFeed((prev) => [parsedData, ...prev].slice(0, 50));
      } catch (e) {
        console.error("Failed to parse incoming news:", e);
      }
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  return (
    <main className="flex min-h-screen flex-col bg-black text-white p-8 font-mono">
      <div className="flex justify-between items-center mb-8 border-b border-gray-800 pb-4">
        <h1 className="text-4xl font-bold tracking-tighter text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-green-400">
          PULSE<span className="text-white">STREAM</span>
        </h1>
        <div className="flex items-center gap-2">
          <div className={`h-3 w-3 rounded-full ${isConnected ? "bg-green-500 animate-pulse" : "bg-red-500"}`} />
          <span className="text-xs text-gray-500 uppercase">
            {isConnected ? "Live Data Feed" : "Connecting..."}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Left Panel: Statistics (Placeholder) */}
        <Card className="bg-gray-900 border-gray-800 col-span-1 hidden md:block">
          <CardHeader>
            <CardTitle className="text-gray-400 flex items-center gap-2">
              <Terminal className="w-4 h-4" /> System Status
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="p-2 bg-black rounded border border-gray-800 text-xs text-green-400">
              {">"} System: ONLINE<br/>
              {">"} Model: Llama-3-8b<br/>
              {">"} Latency: ~24ms
            </div>
          </CardContent>
        </Card>

        {/* Right Panel: The Feed */}
        <div className="col-span-2">
          <ScrollArea className="h-[80vh] pr-4">
            <div className="space-y-4">
              {newsFeed.length === 0 && (
                 <div className="text-gray-500 text-center py-20">Waiting for market data...</div>
              )}
              
              {newsFeed.map((item) => {
                // Parse the inner JSON from Ollama safely
                let analysis: ParsedAnalysis = { sentiment: "NEUTRAL", reason: "Analysis pending..." };
                try {
                  analysis = JSON.parse(item.analysis);
                } catch (e) {}

                return (
                  <Card key={item.id} className="bg-zinc-950 border-zinc-800 hover:border-zinc-700 transition-colors">
                    <CardContent className="p-6">
                      <div className="flex justify-between items-start mb-2">
                        <Badge variant="outline" className="text-zinc-500 border-zinc-700">
                          {item.source}
                        </Badge>
                        <span className="text-xs text-zinc-600">
                          {new Date(item.timestamp).toLocaleTimeString()}
                        </span>
                      </div>
                      
                      <h3 className="text-xl font-semibold mb-4 text-zinc-100 leading-tight">
                        {item.headline}
                      </h3>

                      <div className="flex items-start gap-4 p-3 rounded bg-zinc-900/50 border border-zinc-800/50">
                        <SentimentBadge sentiment={analysis.sentiment} />
                        <p className="text-sm text-zinc-400 italic">
                          "{analysis.reason}"
                        </p>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          </ScrollArea>
        </div>
      </div>
    </main>
  );
}

function SentimentBadge({ sentiment }: { sentiment: string }) {
  if (sentiment?.includes("BULLISH")) {
    return <Badge className="bg-green-900/30 text-green-400 hover:bg-green-900/50 border-green-800 gap-1"><ArrowUpRight className="w-3 h-3"/> BULLISH</Badge>;
  }
  if (sentiment?.includes("BEARISH")) {
    return <Badge className="bg-red-900/30 text-red-400 hover:bg-red-900/50 border-red-800 gap-1"><ArrowDownRight className="w-3 h-3"/> BEARISH</Badge>;
  }
  return <Badge className="bg-gray-800 text-gray-400 hover:bg-gray-700 gap-1"><Minus className="w-3 h-3"/> NEUTRAL</Badge>;
}