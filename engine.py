"""
Engine Unificado - Apuração e Projeção Eleições 2026
Consolida:
1. Configurações e Endpoints do CDN do TSE;
2. Rate Limiter Assíncrono (<20 req/s);
3. Parser ASN.1 BER para Boletins de Urna (BUs do exterior 'ZZ');
4. Cliente HTTP/2 Assíncrono de Baixa Latência (TseClient);
5. Motor Estatístico de Projeção Fixa Incremental O(1) e Incerteza Gaussiana.
"""

import os
import sys
import math
import time
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set
import pandas as pd
import httpx

# ==============================================================================
# 1. CONFIGURAÇÕES & CONSTANTES DO CDN DO TSE
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

FILE_ZONAS = DATA_DIR / "dados_zonas_2026.csv"
FILE_CANDIDATOS = DATA_DIR / "candidatos_2026.csv"
FILE_SECOES_ZZ = DATA_DIR / "zz_config_secoes.json"
DIR_BOLETINS_ZZ = DATA_DIR / "boletins_urna_zz"
ZIP_BOLETINS_ZZ = DATA_DIR / "boletins_urna_zz.zip"

CICLO_ELEITORAL = "ele2026"
CD_PLEITO = "3220"
CD_ELEICAO = "6257"

TSE_BR_HOST = "https://resultados.tse.jus.br"
TSE_ABRANGENCIA_HOST = "https://divulgacao-abrangencia.tse.jus.br"
TSE_ARQUIVO_URNA_HOST = "https://arquivo-urna.tse.jus.br"

URL_ACOMPANHAMENTO_BRASIL = f"{TSE_BR_HOST}/oficial/{CICLO_ELEITORAL}/{CD_ELEICAO}/dados/br/br-e{CD_ELEICAO:0>6}-ab.json"
URL_ACOMPANHAMENTO_UF_TEMPLATE = f"{TSE_BR_HOST}/oficial/{CICLO_ELEITORAL}/{CD_ELEICAO}/dados/{{uf}}/{{uf}}-e{CD_ELEICAO:0>6}-ab.json"
URL_DADOS_ZONA_TEMPLATE = f"{TSE_BR_HOST}/oficial/{CICLO_ELEITORAL}/{CD_ELEICAO}/dados/{{uf}}/{{uf}}{{cd_mun_tse}}-z{{nr_zona_tse}}-c0001-e{CD_ELEICAO:0>6}-u.json"
URL_BU_TEMPLATE = f"{TSE_ARQUIVO_URNA_HOST}/arquivo-urna/{CD_PLEITO}/dados/zz/{{cd_mun_tse}}/{{nr_zona_tse}}/{{nr_secao_tse}}/p00{CD_PLEITO}-zz-m{{cd_mun_tse}}-z{{nr_zona_tse}}-s{{nr_secao_tse}}-bu.dat"

MAX_REQUESTS_PER_SECOND = 16.0  # Limite rigoroso abaixo de 20 req/s

METRICAS_VOTOS = [
    "lula",
    "bolsonaro",
    "outros",
    "validos",
    "brancos",
    "nulos",
    "comparecimento",
    "ausentes",
]

# ==============================================================================
# 2. RATE LIMITER ASSÍNCRONO
# ==============================================================================
class AsyncRateLimiter:
    """Rate limiter por janela deslizante para garantir conexões seguras com o CDN."""
    def __init__(self, max_rate: float = MAX_REQUESTS_PER_SECOND):
        self.max_rate = max_rate
        self.interval = 1.0 / max_rate
        self.lock = asyncio.Lock()
        self.timestamps: List[float] = []

    async def acquire(self) -> None:
        async with self.lock:
            now = time.monotonic()
            cutoff = now - 1.0
            self.timestamps = [t for t in self.timestamps if t > cutoff]

            if len(self.timestamps) >= self.max_rate:
                wait_time = 1.0 - (now - self.timestamps[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                now = time.monotonic()
                cutoff = now - 1.0
                self.timestamps = [t for t in self.timestamps if t > cutoff]

            if self.timestamps:
                last_t = self.timestamps[-1]
                delta = now - last_t
                if delta < self.interval:
                    await asyncio.sleep(self.interval - delta)
                    now = time.monotonic()

            self.timestamps.append(now)

    def current_rate(self) -> float:
        now = time.monotonic()
        cutoff = now - 1.0
        self.timestamps = [t for t in self.timestamps if t > cutoff]
        return float(len(self.timestamps))

# ==============================================================================
# 3. PARSER ASN.1 BER PARA BOLETINS DE URNA (BU)
# ==============================================================================
def parse_bu_bytes(raw: bytes, filepath: Optional[Path] = None) -> Dict[str, Any]:
    """Decodifica o binário ASN.1 DER/BER de um Boletim de Urna (BU) oficial do TSE."""
    def decode_asn1(b):
        items = []
        idx = 0
        while idx < len(b):
            if idx + 2 > len(b):
                break
            tag = b[idx]
            len_byte = b[idx+1]
            hdr = 2
            if len_byte < 128:
                l = len_byte
            elif len_byte == 0x81:
                l = b[idx+2]
                hdr = 3
            elif len_byte == 0x82:
                l = (b[idx+2] << 8) | b[idx+3]
                hdr = 4
            elif len_byte == 0x83:
                l = (b[idx+2] << 16) | (b[idx+3] << 8) | b[idx+4]
                hdr = 5
            else:
                break
            val = b[idx+hdr : idx+hdr+l]
            is_constructed = bool(tag & 0x20)
            if is_constructed:
                items.append((tag, decode_asn1(val)))
            elif tag == 0x04 and len(val) > 2 and val[0] == 0x30:
                items.append((tag, decode_asn1(val)))
            else:
                items.append((tag, val))
            idx += hdr + l
        return items

    tree = decode_asn1(raw)
    res = {
        'filepath': str(filepath) if filepath else '',
        'cd_municipio': None,
        'nr_zona': None,
        'nr_secao': None,
        'qt_aptos': 0,
        'qt_comparecimento': 0,
        'votos_lula': 0,
        'votos_bolsonaro': 0,
        'votos_outros': 0,
        'votos_validos': 0,
        'votos_brancos': 0,
        'votos_nulos': 0,
        'candidatos': {}
    }

    # 1. Identificação da Seção (Tag 0xa0)
    def find_ident(node):
        if isinstance(node, list):
            for child in node:
                if isinstance(child, tuple):
                    if child[0] == 0xa0 and isinstance(child[1], list):
                        cl = child[1]
                        if len(cl) >= 3 and cl[0][0] == 0x30 and isinstance(cl[0][1], list):
                            inner = cl[0][1]
                            if len(inner) >= 2 and inner[0][0] == 0x02 and inner[1][0] == 0x02:
                                res['cd_municipio'] = int.from_bytes(inner[0][1], 'big')
                                res['nr_zona'] = int.from_bytes(inner[1][1], 'big')
                                res['nr_secao'] = int.from_bytes(cl[-1][1], 'big')
                                return True
                    if find_ident(child[1]):
                        return True
        return False
    find_ident(tree)

    # 2. Dados da Eleição 6257 (Presidente)
    def find_ele_6257(node):
        if isinstance(node, list):
            for child in node:
                if isinstance(child, tuple) and isinstance(child[1], list):
                    val = child[1]
                    for sub in val:
                        if isinstance(sub, tuple) and sub[0] == 0x02 and int.from_bytes(sub[1], 'big') == 6257:
                            return val
                    res_sub = find_ele_6257(child[1])
                    if res_sub is not None:
                        return res_sub
        return None

    ele_node = find_ele_6257(tree)
    if ele_node:
        if len(ele_node) > 1 and ele_node[1][0] == 0x02:
            res['qt_aptos'] = int.from_bytes(ele_node[1][1], 'big')

        def parse_single_record(fields):
            tipo = None
            qtd = 0
            cand = None
            for f in fields:
                ftag, fval = f
                if ftag == 0x81:
                    tipo = int.from_bytes(fval, 'big')
                elif ftag == 0x82:
                    qtd = int.from_bytes(fval, 'big')
                elif ftag == 0xa3 and isinstance(fval, list):
                    for sub in fval:
                        if sub[0] == 0x02:
                            cand = int.from_bytes(sub[1], 'big')
                            break

            if tipo == 1 and cand is not None:
                res['candidatos'][cand] = res['candidatos'].get(cand, 0) + qtd
                if cand == 13:
                    res['votos_lula'] += qtd
                elif cand == 22:
                    res['votos_bolsonaro'] += qtd
                else:
                    res['votos_outros'] += qtd
                res['votos_validos'] += qtd
            elif tipo == 2:
                res['votos_brancos'] += qtd
            elif tipo == 3:
                res['votos_nulos'] += qtd

        def walk_votos(node):
            if isinstance(node, list):
                for elem in node:
                    walk_votos(elem)
            elif isinstance(node, tuple):
                tag, val = node
                if tag == 0x30 and isinstance(val, list):
                    has_vote_records = False
                    for child in val:
                        if isinstance(child, tuple) and child[0] == 0x30 and isinstance(child[1], list):
                            tags = [c[0] for c in child[1]]
                            if 0x81 in tags and 0x82 in tags:
                                has_vote_records = True
                                break
                    if has_vote_records:
                        for rec in val:
                            if isinstance(rec, tuple) and rec[0] == 0x30 and isinstance(rec[1], list):
                                parse_single_record(rec[1])
                    else:
                        walk_votos(val)

        walk_votos(ele_node)
        res['qt_comparecimento'] = res['votos_validos'] + res['votos_brancos'] + res['votos_nulos']

    return res


def parse_bu_file(filepath: Path) -> Optional[Dict[str, Any]]:
    if not filepath.exists():
        return None
    try:
        with open(filepath, "rb") as f:
            data = f.read()
        return parse_bu_bytes(data, filepath=filepath)
    except Exception:
        return None

# ==============================================================================
# 4. CLIENTE HTTP/2 TSE (TseClient)
# ==============================================================================
class TseClient:
    """Cliente HTTP/2 persistente para download com baixa latência e rate limiting estrito."""
    def __init__(self, max_rate: float = MAX_REQUESTS_PER_SECOND):
        self.rate_limiter = AsyncRateLimiter(max_rate=max_rate)
        self.total_requests = 0

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "application/json, application/octet-stream, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        limits = httpx.Limits(max_keepalive_connections=50, max_connections=100, keepalive_expiry=60.0)
        timeout = httpx.Timeout(10.0, connect=5.0)

        try:
            self.client = httpx.AsyncClient(http2=True, limits=limits, timeout=timeout, headers=headers, follow_redirects=True)
            self.http2_enabled = True
        except ImportError:
            self.client = httpx.AsyncClient(http2=False, limits=limits, timeout=timeout, headers=headers, follow_redirects=True)
            self.http2_enabled = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_json(self, url: str) -> Optional[Dict[str, Any]]:
        await self.rate_limiter.acquire()
        self.total_requests += 1
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception:
            return None

    async def get_acompanhamento_brasil(self) -> Optional[Dict[str, Any]]:
        return await self.fetch_json(URL_ACOMPANHAMENTO_BRASIL)

    async def get_acompanhamento_uf(self, uf: str) -> Optional[Dict[str, Any]]:
        url = URL_ACOMPANHAMENTO_UF_TEMPLATE.format(uf=uf.lower())
        return await self.fetch_json(url)

    async def check_and_download_bu(self, cd_mun: int, nr_zona: int, nr_secao: str) -> Optional[Dict[str, Any]]:
        cd_mun_tse = f"{cd_mun:05d}"
        nr_zona_tse = f"{nr_zona:04d}"
        nr_secao_tse = f"{int(nr_secao):04d}"

        dest_dir = DIR_BOLETINS_ZZ
        dest_dir.mkdir(parents=True, exist_ok=True)
        filename = f"bu_zz_{cd_mun}_{nr_secao}.dat"
        dest_path = dest_dir / filename

        if dest_path.exists():
            return parse_bu_file(dest_path)

        url = URL_BU_TEMPLATE.format(cd_mun_tse=cd_mun_tse, nr_zona_tse=nr_zona_tse, nr_secao_tse=nr_secao_tse)
        await self.rate_limiter.acquire()
        self.total_requests += 1
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200 and len(resp.content) > 100:
                with open(dest_path, "wb") as f:
                    f.write(resp.content)
                parsed = decode_bu_bytes(resp.content)
                parsed["cd_municipio"] = cd_mun
                parsed["nr_secao"] = nr_secao
                return parsed
        except Exception:
            pass
        return None

# ==============================================================================
# 5. ESTADO ELEITORAL & MOTOR DE PROJEÇÃO FIXA
# ==============================================================================
class ZonaState:
    """Estado individual em memória de cada uma das 6.292 zonas eleitorais."""
    def __init__(
        self,
        chave_zona: str,
        sg_uf: str,
        cd_municipio: int,
        cd_municipio_tse: str,
        nm_municipio: str,
        nr_zona: int,
        nr_zona_tse: str,
        qt_secoes: int,
        qt_aptos: int,
        url: str,
        esperados: Dict[str, float],
    ):
        self.chave_zona = chave_zona
        self.sg_uf = sg_uf.upper()
        self.uf_lower = sg_uf.lower()
        self.cd_municipio = cd_municipio
        self.cd_municipio_tse = cd_municipio_tse
        self.nm_municipio = nm_municipio
        self.nr_zona = nr_zona
        self.nr_zona_tse = nr_zona_tse
        self.qt_secoes = qt_secoes
        self.qt_aptos = qt_aptos
        self.url = url
        self.esperados = esperados

        self.st: int = 0
        self.ts: int = qt_secoes
        self.ht: str = "00:00:00"
        self.apurados: Dict[str, int] = {m: 0 for m in METRICAS_VOTOS}
        self.contribuicao_projecao: Dict[str, float] = {m: float(esperados[m]) for m in METRICAS_VOTOS}
        self.prev_apurados: Dict[str, int] = {m: 0 for m in METRICAS_VOTOS}
        self.prev_contribuicao: Dict[str, float] = {m: float(esperados[m]) for m in METRICAS_VOTOS}
        self.secoes_bu: Set[str] = set()


class ElectionState:
    """Orquestrador do estado eleitoral global com atualização incremental O(1)."""
    def __init__(self):
        self.zonas: Dict[str, ZonaState] = {}
        self.mun_to_zonas: Dict[Tuple[str, str], List[str]] = {}
        self.mun_ht: Dict[Tuple[str, str], str] = {}
        self.uf_ht: Dict[str, str] = {}
        self.candidatos: Dict[int, Dict[str, str]] = {}
        self.secoes_bu_processadas: Set[Tuple[int, str]] = set()

        self.total_secoes_nacional = 0
        self.total_aptos_nacional = 0
        self.secoes_apuradas_nacional = 0
        self.eleitores_apurados_nacional = 0

        self.total_secoes_br = 0
        self.secoes_apuradas_br = 0
        self.total_secoes_zz = 0
        self.secoes_apuradas_zz = 0

        self.totais_esperados: Dict[str, float] = {m: 0.0 for m in METRICAS_VOTOS}
        self.totais_apurados: Dict[str, int] = {m: 0 for m in METRICAS_VOTOS}
        self.totais_projetados: Dict[str, float] = {m: 0.0 for m in METRICAS_VOTOS}

    def inicializar(self) -> None:
        """Carrega a base consolidada de zonas e candidatos de 2026."""
        if FILE_CANDIDATOS.exists():
            df_cand = pd.read_csv(FILE_CANDIDATOS, sep=";", encoding="utf-8-sig")
            for _, row in df_cand.iterrows():
                self.candidatos[int(row["numero"])] = {
                    "nome_urna": row["nome_urna"],
                    "partido_sigla": row["partido_sigla"],
                }

        if not FILE_ZONAS.exists():
            raise FileNotFoundError(f"Arquivo consolidado {FILE_ZONAS} não encontrado.")

        df_zonas = pd.read_csv(FILE_ZONAS, sep=";", encoding="utf-8-sig")

        for _, r in df_zonas.iterrows():
            k26 = str(r["chave_zona"])
            uf = str(r["sg_uf"]).lower()
            cd_mun_tse = f"{int(r['cd_municipio']):05d}"
            cd_mun = int(r["cd_municipio"])
            nr_zona = int(r["nr_zona"])
            nr_zona_tse = f"{nr_zona:04d}"
            secoes = int(r["qt_secoes"])
            aptos = int(r["qt_aptos"])
            url = str(r.get("url", ""))

            esp = {
                "lula": float(r["esperado_lula"]),
                "bolsonaro": float(r["esperado_bolsonaro"]),
                "outros": float(r["esperado_outros_nominais"]),
                "validos": float(r["esperado_votos_validos"]),
                "brancos": float(r["esperado_brancos"]),
                "nulos": float(r["esperado_nulos"]),
                "comparecimento": float(r["esperado_comparecimento"]),
                "ausentes": float(r["esperado_ausentes"]),
            }

            zona = ZonaState(
                chave_zona=k26,
                sg_uf=r["sg_uf"],
                cd_municipio=cd_mun,
                cd_municipio_tse=cd_mun_tse,
                nm_municipio=str(r["nm_municipio"]),
                nr_zona=nr_zona,
                nr_zona_tse=nr_zona_tse,
                qt_secoes=secoes,
                qt_aptos=aptos,
                url=url,
                esperados=esp,
            )
            self.zonas[k26] = zona

            mun_key = (uf, cd_mun_tse)
            if mun_key not in self.mun_to_zonas:
                self.mun_to_zonas[mun_key] = []
                self.mun_ht[mun_key] = "00:00:00"
            self.mun_to_zonas[mun_key].append(k26)

            self.total_secoes_nacional += secoes
            self.total_aptos_nacional += aptos
            if uf == "zz":
                self.total_secoes_zz += secoes
            else:
                self.total_secoes_br += secoes

            for m in METRICAS_VOTOS:
                self.totais_esperados[m] += esp[m]

        for m in METRICAS_VOTOS:
            self.totais_projetados[m] = self.totais_esperados[m]

        self.carregar_bus_locais()

    def carregar_bus_locais(self) -> int:
        if ZIP_BOLETINS_ZZ.exists():
            try:
                import zipfile
                DIR_BOLETINS_ZZ.mkdir(parents=True, exist_ok=True)
                if not any(DIR_BOLETINS_ZZ.glob("*.dat")):
                    with zipfile.ZipFile(ZIP_BOLETINS_ZZ, "r") as z:
                        z.extractall(DIR_BOLETINS_ZZ)
            except Exception:
                pass

        if not DIR_BOLETINS_ZZ.exists():
            return 0
        arquivos = list(DIR_BOLETINS_ZZ.glob("*.dat"))
        carregados = 0
        for arq in arquivos:
            try:
                partes = arq.stem.split("_")
                if len(partes) >= 4 and partes[1] == "zz":
                    cd_mun = int(partes[2].replace("m", ""))
                    nr_sec = partes[3].replace("s", "")
                    parsed = parse_bu_file(arq)
                    if parsed:
                        parsed["cd_municipio"] = cd_mun
                        parsed["nr_secao"] = nr_sec
                        if self.update_secao_bu(parsed, is_initial=True):
                            carregados += 1
            except Exception:
                continue
        return carregados

    def update_secao_bu(self, parsed: Dict[str, Any], is_initial: bool = False) -> bool:
        cd_mun = int(parsed.get("cd_municipio", 0))
        nr_secao = str(parsed.get("nr_secao", "")).strip()

        if not cd_mun or not nr_secao:
            return False

        sec_id = (cd_mun, nr_secao)
        if sec_id in self.secoes_bu_processadas:
            return False

        chave_zona = f"ZZ_{cd_mun}_1"
        zona = self.zonas.get(chave_zona)
        if not zona:
            return False

        self.secoes_bu_processadas.add(sec_id)
        zona.secoes_bu.add(nr_secao)

        v_lula = int(parsed.get("votos_lula", 0))
        v_bols = int(parsed.get("votos_bolsonaro", 0))
        v_outr = int(parsed.get("votos_outros", 0))
        v_val = int(parsed.get("votos_validos", v_lula + v_bols + v_outr))
        v_bra = int(parsed.get("votos_brancos", 0))
        v_nul = int(parsed.get("votos_nulos", 0))
        v_comp = int(parsed.get("qt_comparecimento", v_val + v_bra + v_nul))
        aptos_sec = int(parsed.get("qt_aptos", v_comp))
        v_aus = max(0, aptos_sec - v_comp)

        novos_apurados = {
            "lula": zona.apurados["lula"] + v_lula,
            "bolsonaro": zona.apurados["bolsonaro"] + v_bols,
            "outros": zona.apurados["outros"] + v_outr,
            "validos": zona.apurados["validos"] + v_val,
            "brancos": zona.apurados["brancos"] + v_bra,
            "nulos": zona.apurados["nulos"] + v_nul,
            "comparecimento": zona.apurados["comparecimento"] + v_comp,
            "ausentes": zona.apurados["ausentes"] + v_aus,
        }

        secoes_apuradas = len(zona.secoes_bu)
        total_secoes = zona.qt_secoes
        p_i = (secoes_apuradas / total_secoes) if total_secoes > 0 else 1.0
        p_i = max(0.0, min(1.0, p_i))

        nova_contribuicao = {}
        for m in METRICAS_VOTOS:
            nova_contribuicao[m] = float(novos_apurados[m]) + (1.0 - p_i) * zona.esperados[m]

        for m in METRICAS_VOTOS:
            delta_apurado = novos_apurados[m] - zona.prev_apurados[m]
            delta_projetado = nova_contribuicao[m] - zona.prev_contribuicao[m]

            self.totais_apurados[m] += delta_apurado
            self.totais_projetados[m] += delta_projetado

            zona.prev_apurados[m] = novos_apurados[m]
            zona.prev_contribuicao[m] = nova_contribuicao[m]

        self.secoes_apuradas_nacional += 1
        self.secoes_apuradas_zz += 1
        zona.st = secoes_apuradas
        zona.apurados = novos_apurados
        zona.contribuicao_projecao = nova_contribuicao

        media_aptos_secao = (zona.qt_aptos / total_secoes) if total_secoes > 0 else 0
        self.eleitores_apurados_nacional += int(media_aptos_secao)
        return True

    def update_zona(self, chave_zona: str, data: Dict[str, Any]) -> bool:
        zona = self.zonas.get(chave_zona)
        if not zona:
            return False

        st_novo = int(data.get("s", {}).get("st", 0))
        ts_novo = int(data.get("s", {}).get("ts", zona.ts))
        ht_novo = data.get("ht", "").strip()

        if zona.sg_uf == "ZZ" and zona.secoes_bu:
            if st_novo <= len(zona.secoes_bu):
                return False

        if st_novo < zona.st:
            return False

        v_lula = 0
        v_bols = 0
        v_outr = 0

        cargos = data.get("carg", [])
        if cargos:
            for agr in cargos[0].get("agr", []):
                for par in agr.get("par", []):
                    for cand in par.get("cand", []):
                        n = int(cand.get("n", 0))
                        vap = int(cand.get("vap", 0))
                        if n == 13:
                            v_lula += vap
                        elif n == 22:
                            v_bols += vap
                        else:
                            v_outr += vap

        v_val = v_lula + v_bols + v_outr
        v_bra = int(data.get("v", {}).get("vb", 0))
        v_nul = int(data.get("v", {}).get("tvn", 0))
        v_comp = int(data.get("e", {}).get("c", v_val + v_bra + v_nul))
        v_aus = int(data.get("e", {}).get("a", 0))

        novos_apurados = {
            "lula": v_lula,
            "bolsonaro": v_bols,
            "outros": v_outr,
            "validos": v_val,
            "brancos": v_bra,
            "nulos": v_nul,
            "comparecimento": v_comp,
            "ausentes": v_aus,
        }

        p_i = (st_novo / ts_novo) if ts_novo > 0 else 1.0
        p_i = max(0.0, min(1.0, p_i))

        nova_contribuicao = {}
        for m in METRICAS_VOTOS:
            nova_contribuicao[m] = float(novos_apurados[m]) + (1.0 - p_i) * zona.esperados[m]

        for m in METRICAS_VOTOS:
            delta_apurado = novos_apurados[m] - zona.prev_apurados[m]
            delta_projetado = nova_contribuicao[m] - zona.prev_contribuicao[m]

            self.totais_apurados[m] += delta_apurado
            self.totais_projetados[m] += delta_projetado

            zona.prev_apurados[m] = novos_apurados[m]
            zona.prev_contribuicao[m] = nova_contribuicao[m]

        delta_secoes = st_novo - zona.st
        self.secoes_apuradas_nacional += delta_secoes
        if zona.sg_uf == "ZZ":
            self.secoes_apuradas_zz += delta_secoes
        else:
            self.secoes_apuradas_br += delta_secoes

        self.eleitores_apurados_nacional += int(delta_secoes / ts_novo * zona.qt_aptos) if ts_novo > 0 else 0

        zona.st = st_novo
        zona.ts = ts_novo
        zona.apurados = novos_apurados
        zona.contribuicao_projecao = nova_contribuicao
        if ht_novo:
            zona.ht = ht_novo

        return delta_secoes > 0 or novos_apurados["validos"] > 0

    def calcular_cenarios_probabilidade(self, sigma0: float = 0.015) -> List[Tuple[str, float]]:
        """Calcula probabilidades dos 4 cenários garantindo soma estritamente 100%."""
        tot_sec = self.total_secoes_nacional
        ap_sec = self.secoes_apuradas_nacional
        f = (ap_sec / tot_sec) if tot_sec > 0 else 0.0

        pr_val = self.totais_projetados["validos"]
        pr_lula = self.totais_projetados["lula"]
        pr_bols = self.totais_projetados["bolsonaro"]

        pct_lula = (pr_lula / pr_val) if pr_val > 0 else 0.4850
        pct_bols = (pr_bols / pr_val) if pr_val > 0 else 0.4316

        if f >= 0.9999:
            if pct_lula > 0.50:
                return [
                    ("LULA GANHA NO 1º TURNO:", 100.0),
                    ("VAI PARA O 2º TURNO, LULA NA FRENTE", 0.0),
                    ("VAI PARA O 2º TURNO, BOLSONARO NA FRENTE", 0.0),
                    ("BOLSONARO GANHA NO 1º TURNO:", 0.0),
                ]
            elif pct_bols > 0.50:
                return [
                    ("LULA GANHA NO 1º TURNO:", 0.0),
                    ("VAI PARA O 2º TURNO, LULA NA FRENTE", 0.0),
                    ("VAI PARA O 2º TURNO, BOLSONARO NA FRENTE", 0.0),
                    ("BOLSONARO GANHA NO 1º TURNO:", 100.0),
                ]
            elif pct_lula >= pct_bols:
                return [
                    ("LULA GANHA NO 1º TURNO:", 0.0),
                    ("VAI PARA O 2º TURNO, LULA NA FRENTE", 100.0),
                    ("VAI PARA O 2º TURNO, BOLSONARO NA FRENTE", 0.0),
                    ("BOLSONARO GANHA NO 1º TURNO:", 0.0),
                ]
            else:
                return [
                    ("LULA GANHA NO 1º TURNO:", 0.0),
                    ("VAI PARA O 2º TURNO, LULA NA FRENTE", 0.0),
                    ("VAI PARA O 2º TURNO, BOLSONARO NA FRENTE", 100.0),
                    ("BOLSONARO GANHA NO 1º TURNO:", 0.0),
                ]

        def norm_cdf(z: float) -> float:
            return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

        sigma = max(0.0001, sigma0 * math.sqrt(max(0.0, 1.0 - f)))

        z_lula = (0.50 - pct_lula) / sigma
        p_lula_1t = max(0.0, min(1.0, 1.0 - norm_cdf(z_lula)))

        z_bols = (0.50 - pct_bols) / sigma
        p_bols_1t = max(0.0, min(1.0, 1.0 - norm_cdf(z_bols)))

        p_2t = max(0.0, 1.0 - p_lula_1t - p_bols_1t)
        diff = pct_lula - pct_bols
        sigma_d = max(0.0001, 1.9 * sigma)
        z_diff = diff / sigma_d
        p_lula_2t = p_2t * norm_cdf(z_diff)
        p_bols_2t = p_2t * (1.0 - norm_cdf(z_diff))

        total = p_lula_1t + p_lula_2t + p_bols_2t + p_bols_1t
        if total <= 0:
            total = 1.0

        return [
            ("LULA GANHA NO 1º TURNO:", (p_lula_1t / total) * 100.0),
            ("VAI PARA O 2º TURNO, LULA NA FRENTE", (p_lula_2t / total) * 100.0),
            ("VAI PARA O 2º TURNO, BOLSONARO NA FRENTE", (p_bols_2t / total) * 100.0),
            ("BOLSONARO GANHA NO 1º TURNO:", (p_bols_1t / total) * 100.0),
        ]

    def get_dashboard_payload(
        self,
        rate_instantanea: float = 0.0,
        total_requisicoes: int = 0,
        ultima_atualizacao: str = "Aguardando apuração...",
        ciclo_num: int = 1,
    ) -> Dict[str, Any]:
        """Gera o payload serializável para consumo do dashboard HTML em tempo real."""
        now_str = time.strftime("%H:%M:%S")

        tot_apt = self.total_aptos_nacional
        ap_apt = self.eleitores_apurados_nacional
        pct_apt = (ap_apt / tot_apt * 100.0) if tot_apt > 0 else 0.0

        ap_val = self.totais_apurados["validos"]
        ap_lula = self.totais_apurados["lula"]
        ap_bols = self.totais_apurados["bolsonaro"]
        ap_outr = self.totais_apurados["outros"]
        ap_bra = self.totais_apurados["brancos"]
        ap_nul = self.totais_apurados["nulos"]
        ap_comp = self.totais_apurados["comparecimento"]
        ap_aus = self.totais_apurados["ausentes"]
        ap_bn = ap_bra + ap_nul

        pr_val = self.totais_projetados["validos"]
        pr_lula = self.totais_projetados["lula"]
        pr_bols = self.totais_projetados["bolsonaro"]
        pr_outr = self.totais_projetados["outros"]
        pr_bra = self.totais_projetados["brancos"]
        pr_nul = self.totais_projetados["nulos"]
        pr_comp = self.totais_projetados["comparecimento"]
        pr_aus = self.totais_projetados["ausentes"]
        pr_bn = pr_bra + pr_nul

        esp_val = self.totais_esperados["validos"]
        esp_lula = self.totais_esperados["lula"]
        esp_bols = self.totais_esperados["bolsonaro"]
        esp_outr = self.totais_esperados["outros"]
        esp_bra = self.totais_esperados["brancos"]
        esp_nul = self.totais_esperados["nulos"]
        esp_comp = self.totais_esperados["comparecimento"]
        esp_aus = self.totais_esperados["ausentes"]
        esp_bn = esp_bra + esp_nul

        pct_pr_lula = (pr_lula / pr_val * 100) if pr_val > 0 else 0.0
        pct_pr_bols = (pr_bols / pr_val * 100) if pr_val > 0 else 0.0
        pct_pr_outr = (pr_outr / pr_val * 100) if pr_val > 0 else 0.0
        pct_pr_bn = (pr_bn / pr_comp * 100) if pr_comp > 0 else 0.0
        pct_pr_aus = (pr_aus / tot_apt * 100) if tot_apt > 0 else 0.0

        dv_lula = pr_lula - esp_lula
        dv_bols = pr_bols - esp_bols
        dv_outr = pr_outr - esp_outr
        dv_bn = pr_bn - esp_bn
        dv_aus = pr_aus - esp_aus

        esp_val_apu = esp_val - (pr_val - ap_val)
        esp_lula_apu = esp_lula - (pr_lula - ap_lula)
        esp_bols_apu = esp_bols - (pr_bols - ap_bols)
        esp_outr_apu = esp_outr - (pr_outr - ap_outr)
        esp_comp_apu = esp_comp - (pr_comp - ap_comp)
        esp_bn_apu = esp_bn - (pr_bn - ap_bn)
        esp_aus_apu = esp_aus - (pr_aus - ap_aus)

        dp_lula = ((ap_lula / ap_val * 100) - (esp_lula_apu / esp_val_apu * 100)) if (ap_val > 0 and esp_val_apu > 0) else 0.0
        dp_bols = ((ap_bols / ap_val * 100) - (esp_bols_apu / esp_val_apu * 100)) if (ap_val > 0 and esp_val_apu > 0) else 0.0
        dp_outr = ((ap_outr / ap_val * 100) - (esp_outr_apu / esp_val_apu * 100)) if (ap_val > 0 and esp_val_apu > 0) else 0.0
        dp_bn = ((ap_bn / ap_comp * 100) - (esp_bn_apu / esp_comp_apu * 100)) if (ap_comp > 0 and esp_comp_apu > 0) else 0.0

        if ap_apt > 0:
            esp_apt_apu = esp_comp_apu + esp_aus_apu
            dp_aus = (ap_aus / ap_apt * 100) - ((esp_aus_apu / esp_apt_apu * 100) if esp_apt_apu > 0 else 0.0)
        else:
            dp_aus = 0.0

        def fmt_num(v: float or int) -> str:
            return f"{int(round(v)):,}".replace(",", ".")

        def fmt_dv(v: float) -> str:
            val = int(round(v))
            if val == 0:
                return "0"
            s = "+" if val > 0 else "-"
            return f"{s}{abs(val):,}".replace(",", ".")

        def fmt_dp(v: float) -> str:
            if abs(v) < 0.005:
                return "0,00%"
            s = "+" if v > 0 else "-"
            return f"{s}{abs(v):.2f}%".replace(".", ",")

        def fmt_pct(v: float) -> str:
            return f"{v:.2f}%".replace(".", ",")

        def fmt_prob(v: float) -> str:
            val = max(0.0, min(100.0, v))
            return f"{val:05.2f}%".replace(".", ",")

        linhas = [
            {
                "candidato": "LULA",
                "partido": "PT",
                "apurados": ap_lula,
                "apurados_fmt": fmt_num(ap_lula),
                "delta_votos": int(round(dv_lula)),
                "delta_votos_fmt": fmt_dv(dv_lula),
                "delta_pp": round(dp_lula, 2),
                "delta_pp_fmt": fmt_dp(dp_lula),
                "projecao_votos": int(round(pr_lula)),
                "projecao_votos_fmt": fmt_num(pr_lula),
                "projecao_pct": round(pct_pr_lula, 2),
                "projecao_pct_fmt": fmt_pct(pct_pr_lula),
                "is_cand": True,
            },
            {
                "candidato": "BOLSONARO",
                "partido": "PL",
                "apurados": ap_bols,
                "apurados_fmt": fmt_num(ap_bols),
                "delta_votos": int(round(dv_bols)),
                "delta_votos_fmt": fmt_dv(dv_bols),
                "delta_pp": round(dp_bols, 2),
                "delta_pp_fmt": fmt_dp(dp_bols),
                "projecao_votos": int(round(pr_bols)),
                "projecao_votos_fmt": fmt_num(pr_bols),
                "projecao_pct": round(pct_pr_bols, 2),
                "projecao_pct_fmt": fmt_pct(pct_pr_bols),
                "is_cand": True,
            },
            {
                "candidato": "OUTROS",
                "partido": "-",
                "apurados": ap_outr,
                "apurados_fmt": fmt_num(ap_outr),
                "delta_votos": int(round(dv_outr)),
                "delta_votos_fmt": fmt_dv(dv_outr),
                "delta_pp": round(dp_outr, 2),
                "delta_pp_fmt": fmt_dp(dp_outr),
                "projecao_votos": int(round(pr_outr)),
                "projecao_votos_fmt": fmt_num(pr_outr),
                "projecao_pct": round(pct_pr_outr, 2),
                "projecao_pct_fmt": fmt_pct(pct_pr_outr),
                "is_cand": True,
            },
            {
                "candidato": "B+N",
                "partido": "Brancos/Nulos",
                "apurados": ap_bn,
                "apurados_fmt": fmt_num(ap_bn),
                "delta_votos": int(round(dv_bn)),
                "delta_votos_fmt": fmt_dv(dv_bn),
                "delta_pp": round(dp_bn, 2),
                "delta_pp_fmt": fmt_dp(dp_bn),
                "projecao_votos": int(round(pr_bn)),
                "projecao_votos_fmt": fmt_num(pr_bn),
                "projecao_pct": round(pct_pr_bn, 2),
                "projecao_pct_fmt": fmt_pct(pct_pr_bn),
                "is_cand": False,
            },
            {
                "candidato": "ABS.",
                "partido": "Abstenções",
                "apurados": ap_aus,
                "apurados_fmt": fmt_num(ap_aus),
                "delta_votos": int(round(dv_aus)),
                "delta_votos_fmt": fmt_dv(dv_aus),
                "delta_pp": round(dp_aus, 2),
                "delta_pp_fmt": fmt_dp(dp_aus),
                "projecao_votos": int(round(pr_aus)),
                "projecao_votos_fmt": fmt_num(pr_aus),
                "projecao_pct": round(pct_pr_aus, 2),
                "projecao_pct_fmt": fmt_pct(pct_pr_aus),
                "is_cand": False,
            },
        ]

        probs = self.calcular_cenarios_probabilidade()
        cenarios_payload = []
        for nome_sc, p in probs:
            cenarios_payload.append({
                "cenario": nome_sc,
                "probabilidade": round(p, 2),
                "probabilidade_fmt": fmt_prob(p),
            })

        return {
            "horario": now_str,
            "ciclo": ciclo_num,
            "taxa_req": round(rate_instantanea, 1),
            "total_req": total_requisicoes,
            "ultima_atualizacao": ultima_atualizacao,
            "progresso": {
                "eleitores_apurados": ap_apt,
                "eleitores_apurados_fmt": fmt_num(ap_apt),
                "eleitores_total": tot_apt,
                "eleitores_total_fmt": fmt_num(tot_apt),
                "pct_apurado": round(pct_apt, 2),
                "pct_apurado_fmt": fmt_pct(pct_apt),
            },
            "tabela": linhas,
            "cenarios": cenarios_payload,
            "observacoes": "*B+N: Brancos e nulos somados; ABS.: Abstenções, ausência; DELTA: apurados - esperados",
        }
