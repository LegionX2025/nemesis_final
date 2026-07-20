#!/usr/bin/env python3
"""
Nemesis Full Deployment Automation
- Deploys WebSocket backend (Worker + Durable Objects)
- Patches frontend to replace Socket.IO with native WebSocket
- Fixes null TypeError in initializeFromLanding
- Builds and deploys frontend to Cloudflare Pages
"""

import os
import re
import sys
import json
import shutil
import subprocess
from pathlib import Path

# ─── Configuration ──────────────────────────────────────────────
BACKEND_DIR = "nemesis-backend"
FRONTEND_DIR = "nemesis-frontend"  # Adjust if your frontend repo is elsewhere
PAGES_PROJECT = "nemesis-frontend"
COMPAT_DATE = "2025-07-20"

# ─── Colors ─────────────────────────────────────────────────────
class C:
    R = "\033[0m"
    G = "\033[92m"
    Y = "\033[93m"
    R2 = "\033[91m"
    B = "\033[94m"
    BOLD = "\033[1m"

def log(msg, color=C.G):
    print(f"{color}{msg}{C.R}")

def step(n, msg):
    log(f"\n{'═'*60}", C.B)
    log(f"  STEP {n}: {msg}", C.BOLD)
    log(f"{'═'*60}", C.B)

def run(cmd, cwd=None, check=True, capture=False):
    """Run a shell command with nice output."""
    log(f"  $ {cmd}", C.Y)
    if capture:
        result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
        if check and result.returncode != 0:
            log(f"  ERROR: {result.stderr}", C.R2)
            if check:
                sys.exit(1)
        return result
    else:
        result = subprocess.run(cmd, shell=True, cwd=cwd)
        if check and result.returncode != 0:
            log(f"  Command failed with code {result.returncode}", C.R2)
            if check:
                sys.exit(1)
        return result

# ─── Worker Backend Code ────────────────────────────────────────
WORKER_CODE = r'''import { DurableObject } from "cloudflare:workers";

// ═══════════════════════════════════════════════════════════════
//  NemesisRoom Durable Object — Real-time WebSocket coordination
// ═══════════════════════════════════════════════════════════════
export class NemesisRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);

    // ── REST Endpoints ──────────────────────────────────────
    if (request.headers.get("Upgrade") !== "websocket") {
      if (url.pathname === "/api/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.ctx.getWebSockets().length,
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
        });
      }

      if (url.pathname === "/api/stats") {
        const sockets = this.ctx.getWebSockets();
        const users = sockets.map(ws => {
          const att = this.ctx.deserializeAttachment(ws);
          return { id: att?.id || "anon", name: att?.name || "Anonymous" };
        });
        return new Response(JSON.stringify({
          online: sockets.length,
          users,
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
        });
      }

      return new Response("Not found", { status: 404 });
    }

    // ── WebSocket Upgrade ───────────────────────────────────
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Accept with Hibernation API for cost efficiency
    const clientId = url.searchParams.get("id") || "anon";
    const clientName = url.searchParams.get("name") || "Anonymous";
    this.ctx.acceptWebSocket(server, [clientId]);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ── WebSocket Event Handlers ─────────────────────────────
  async webSocketMessage(ws, message) {
    let data;
    try {
      data = JSON.parse(message);
    } catch {
      ws.send(JSON.stringify({ type: "error", message: "Invalid JSON format" }));
      return;
    }

    switch (data.type) {
      case "join":
        this.ctx.serializeAttachment(ws, { id: data.id, name: data.name });
        ws.send(JSON.stringify({ type: "joined", id: data.id, name: data.name }));
        this.broadcast({
          type: "user_joined",
          id: data.id,
          name: data.name,
          timestamp: Date.now()
        }, ws);
        this.sendOnlineCount(ws);
        break;

      case "message":
        const sender = this.ctx.deserializeAttachment(ws);
        const msgPayload = {
          type: "message",
          id: sender?.id || data.id || "anon",
          name: sender?.name || data.name || "Anonymous",
          content: data.content,
          timestamp: Date.now()
        };
        this.broadcast(msgPayload);
        break;

      case "typing":
        const typist = this.ctx.deserializeAttachment(ws);
        this.broadcast({
          type: "typing",
          id: typist?.id || "anon",
          name: typist?.name || "Anonymous",
          timestamp: Date.now()
        }, ws);
        break;

      case "stop_typing":
        const stopper = this.ctx.deserializeAttachment(ws);
        this.broadcast({
          type: "stop_typing",
          id: stopper?.id || "anon",
          timestamp: Date.now()
        }, ws);
        break;

      case "ping":
        ws.send(JSON.stringify({ type: "pong", timestamp: Date.now() }));
        break;

      case "get_online":
        this.sendOnlineCount(ws);
        break;

      default:
        ws.send(JSON.stringify({ type: "error", message: `Unknown message type: ${data.type}` }));
    }
  }

  async webSocketClose(ws, code, reason) {
    const user = this.ctx.deserializeAttachment(ws);
    if (user) {
      this.broadcast({
        type: "user_left",
        id: user.id,
        name: user.name,
        timestamp: Date.now()
      });
    }
  }

  async webSocketError(ws, error) {
    console.error("[NemesisRoom] WebSocket error:", error);
    try {
      ws.close(1011, "Internal server error");
    } catch {}
  }

  // ── Helpers ──────────────────────────────────────────────
  sendOnlineCount(ws) {
    const count = this.ctx.getWebSockets()
