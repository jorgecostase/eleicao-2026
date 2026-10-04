"""
Servidor Web e Orquestrador de Apuração - Eleições 2026
Executa simultaneamente:
1. O worker assíncrono de acompanhamento e sincronização em tempo real com o CDN do TSE;
2. O servidor HTTP nativo de alta performance para alimentar o Dashboard Web via JSON.

Execução:
    py app.py
    (acesse http://localhost:8000 no navegador)
"""

import os
import sys
import json
import time
import socket
import asyncio
import threading
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Optional, Dict, Any, List

from engine import ElectionState, TseClient, FILE_SECOES_ZZ

PORT = int(os.environ.get("PORT", 8000))
BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML_PATH = BASE_DIR / "index.html"

# Estado global compartilhado
state = ElectionState()
latest_payload_lock = threading.Lock()
latest_payload: Dict[str, Any] = {}
is_running = True


# ==============================================================================
# WORKER ASSÍNCRONO DE SINCRONIZAÇÃO COM O TSE
# ==============================================================================
async def apuracao_worker_loop():
    """Loop contínuo de sincronização com o TSE (Metodologia Híbrida)."""
    global latest_payload, is_running

    # Carrega catálogo de seções do exterior
    fila_secoes_zz: List[Dict[str, Any]] = []
    if FILE_SECOES_ZZ.exists():
        try:
            with open(FILE_SECOES_ZZ, "r", encoding="utf-8") as f:
                dados_secoes = json.load(f)
            for a in dados_secoes.get("abr", []):
                for mu in a.get("mu", []):
                    cd_mun = int(mu["cd"])
                    for z in mu.get("zon", []):
                        nr_zona = int(z["cd"])
                        for sec in z.get("sec", []):
                            nr_sec = str(sec["ns"]).strip()
                            if (cd_mun, nr_sec) not in state.secoes_bu_processadas:
                                fila_secoes_zz.append({
                                    "cd_municipio": cd_mun,
                                    "nr_zona": nr_zona,
                                    "nr_secao": nr_sec,
                                })
        except Exception as e:
            print(f"[Worker] Aviso ao carregar seções ZZ: {e}")

    ciclo_num = 0
    ultima_atualizacao = f"Inicializado com {state.secoes_apuradas_zz} BUs do exterior já em cache."

    # Atualiza o payload inicial
    with latest_payload_lock:
        latest_payload = state.get_dashboard_payload(
            rate_instantanea=0.0,
            total_requisicoes=0,
            ultima_atualizacao=ultima_atualizacao,
            ciclo_num=ciclo_num,
        )

    async with TseClient() as client:
        while is_running:
            ciclo_num += 1
            houve_atualizacao = False
            secoes_abertas_ciclo = 0

            # ------------------------------------------------------------------
            # FASE A: Verificação de Seções Pendentes do Exterior (ZZ via BU)
            # ------------------------------------------------------------------
            if fila_secoes_zz:
                lote = fila_secoes_zz[:20]
                fila_secoes_zz = fila_secoes_zz[20:]

                for s in lote:
                    if not is_running:
                        break
                    cd_mun = int(s["cd_municipio"])
                    nr_sec = str(s["nr_secao"]).strip()
                    if (cd_mun, nr_sec) in state.secoes_bu_processadas:
                        continue

                    parsed = await client.check_and_download_bu(cd_mun=cd_mun, nr_zona=1, nr_secao=nr_sec)
                    if parsed:
                        if state.update_secao_bu(parsed):
                            houve_atualizacao = True
                            secoes_abertas_ciclo += 1
                            ultima_atualizacao = (
                                f"BU Seção {int(nr_sec):04d} recebido: ZZ (Lula: +{parsed.get('votos_lula', 0)} | "
                                f"Bols: +{parsed.get('votos_bolsonaro', 0)})"
                            )
                    else:
                        fila_secoes_zz.append(s)

            # ------------------------------------------------------------------
            # FASE B: Verificação da Cascata do Brasil (BR -> UFs -> Zonas)
            # ------------------------------------------------------------------
            dados_br = await client.get_acompanhamento_brasil()
            if dados_br:
                ufs_para_atualizar = []
                for ab in dados_br.get("abr", []):
                    cd_abr = ab.get("cd", "").lower()
                    ht_abr = ab.get("ht", "").strip()
                    if cd_abr and cd_abr != "br":
                        if state.uf_ht.get(cd_abr) != ht_abr:
                            ufs_para_atualizar.append((cd_abr, ht_abr))

                for uf, ht_novo in ufs_para_atualizar:
                    if not is_running:
                        break
                    dados_uf = await client.get_acompanhamento_uf(uf)
                    if not dados_uf:
                        continue

                    state.uf_ht[uf] = ht_novo
                    muns_para_atualizar = []
                    for mu in dados_uf.get("mu", []):
                        cd_mun = mu.get("cd", "").strip()
                        ht_mun = mu.get("ht", "").strip()
                        mun_key = (uf, cd_mun)
                        if state.mun_ht.get(mun_key) != ht_mun:
                            muns_para_atualizar.append((cd_mun, ht_mun))

                    for cd_mun, ht_mun in muns_para_atualizar:
                        if not is_running:
                            break
                        mun_key = (uf, cd_mun)
                        state.mun_ht[mun_key] = ht_mun
                        zonas_do_mun = state.mun_to_zonas.get(mun_key, [])

                        for k_zona in zonas_do_mun:
                            zona = state.zonas.get(k_zona)
                            if not zona:
                                continue

                            url_z = f"{client.client.base_url or ''}/oficial/ele2026/6257/dados/{uf}/{uf}{cd_mun}-z{zona.nr_zona_tse}-c0001-e006257-u.json"
                            dados_z = await client.fetch_json(url_z)
                            if dados_z and state.update_zona(k_zona, dados_z):
                                houve_atualizacao = True
                                secoes_abertas_ciclo += 1
                                ultima_atualizacao = f"Zona {zona.nr_zona} atualizada em {zona.nm_municipio}/{zona.sg_uf}"

            # Atualiza payload compartilhado para o servidor web
            rate = client.rate_limiter.current_rate()
            total_req = client.total_requests

            with latest_payload_lock:
                latest_payload = state.get_dashboard_payload(
                    rate_instantanea=rate,
                    total_requisicoes=total_req,
                    ultima_atualizacao=ultima_atualizacao,
                    ciclo_num=ciclo_num,
                )

            # Pausa de ciclo
            wait_s = 2.0 if houve_atualizacao else 5.0
            await asyncio.sleep(wait_s)


def start_background_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(apuracao_worker_loop())
    finally:
        loop.close()


# ==============================================================================
# SERVIDOR HTTP REST & ESTÁTICO NATIVO
# ==============================================================================
class DualStackServer(ThreadingHTTPServer):
    """Servidor multithread com suporte simultâneo a IPv4 (127.0.0.1) e IPv6 (::1 / localhost)."""
    address_family = socket.AF_INET6

    def server_bind(self):
        try:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        except Exception:
            pass
        super().server_bind()


class DashboardHTTPHandler(BaseHTTPRequestHandler):
    """Handler HTTP leve e multithread para servir o frontend e API JSON."""

    def do_GET(self):
        try:
            path = self.path.split("?")[0]
            if path in ("/", "/index.html"):
                self.serve_index()
            elif path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
            elif path.startswith("/api/dados"):
                self.serve_api_dados()
            elif path == "/api/health":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"OK")
            else:
                self.send_error(404, "Arquivo nao encontrado")
        except Exception as e:
            self.send_error(500, f"Erro no servidor: {e}")

    def serve_index(self):
        if not INDEX_HTML_PATH.exists():
            self.send_error(500, "Arquivo index.html nao encontrado")
            return

        try:
            with open(INDEX_HTML_PATH, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_error(500, f"Erro interno: {e}")

    def serve_api_dados(self):
        with latest_payload_lock:
            data_json = json.dumps(latest_payload, ensure_ascii=False).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data_json)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(data_json)

    def log_message(self, format, *args):
        # Silencia logs repetitivos de /api/dados no terminal para manter a saída limpa
        try:
            if args and isinstance(args[0], str) and "/api/dados" in args[0]:
                return
            super().log_message(format, *args)
        except Exception:
            pass


def main():
    print("=" * 70)
    print("   APURAÇÃO ELEIÇÕES 2026 - DASHBOARD ONLINE & SERVER INICIANDO")
    print("=" * 70)
    print("Carregando bases eleitorais e inicializando projeção fixa...")
    state.inicializar()
    print(f"-> Base inicializada: {len(state.zonas):,} zonas carregadas.")
    print(f"-> BUs do exterior ingeridos: {state.secoes_apuradas_zz} seções ativas.")

    # Dispara worker assíncrono em thread separada
    worker_thread = threading.Thread(target=start_background_loop, daemon=True)
    worker_thread.start()
    print("-> Sincronizador assíncrono do TSE iniciado em background (HTTP/2).")

    # Inicia Servidor Web Dual-Stack (IPv4 + IPv6 localhost)
    try:
        server = DualStackServer(("::", PORT), DashboardHTTPHandler)
    except Exception as e:
        print(f"-> Nota: Dual-stack IPv6 indisponível ({e}), vinculando a 0.0.0.0 IPv4.")
        server = ThreadingHTTPServer(("0.0.0.0", PORT), DashboardHTTPHandler)

    print("=" * 70)
    print(f" DASHBOARD DISPONÍVEL EM:")
    print(f" -> http://localhost:{PORT}")
    print(f" -> http://127.0.0.1:{PORT}")
    print("=" * 70)
    print(" Pressione Ctrl+C para encerrar com segurança.")
    print("=" * 70)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Server] Encerrando servidor e worker...")
        global is_running
        is_running = False
        server.server_close()
        print("[Server] Finalizado com sucesso.")


if __name__ == "__main__":
    main()
