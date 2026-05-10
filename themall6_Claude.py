import asyncio
import platform
import re
import time
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path

import asyncssh  # type: ignore
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import DataTable, Footer, Static


SSH_KEY_PATH = Path.home() / ".ssh" / "id_ed25519_cp"
POLL_INTERVAL_S = 3

# Janela de logs consultada a cada atualização. O app lê apenas eventos recentes
# do chargepoint.service para evitar processar o journal inteiro a cada ciclo.
JOURNAL_LOOKBACK = "30 seconds ago"

# Tempo, em segundos, durante o qual bits de power module continuam visíveis
# após terem aparecido ao menos uma vez. Bits presentes na leitura atual são
# mostrados normais; bits vistos apenas dentro da retenção são mostrados em dim.
PM_BITS_RETENTION_S = 15 * 60

# Host usado para indicar latência de internet na barra inferior.
PING_TARGET_HOST = "8.8.8.8"
PING_INTERVAL_S = 30
PING_TIMEOUT_S = 2

# Estados do protocolo que indicam carregamento ativo. Quando o carregador não
# está nesses estados, os campos elétricos são ocultados com marcadores vazios.
CHARGING_STATES = {"1f", "c4"}


@dataclass
class Charger:
    """Configuração de conexão e identificação de um carregador monitorado."""

    # FIX: nomes de campo normalizados para snake_case (PEP 8)
    charge_point_id: str
    vpn_ip: str
    network_type: str = "eth"  # "eth" ou "4g"
    ssh_port: int = 5022
    ssh_user: str = "admin"


# Lista fixa de carregadores exibidos na tabela. Cada item vira uma linha no
# painel e também define o IP usado para ping/SSH.
CHARGERS: list[Charger] = [
    Charger("poli", "10.53.1.21", "eth"),
    Charger("himix01", "10.53.1.40", "eth"),
    Charger("km3_01", "10.53.1.41", "4g"),
    Charger("himix03", "10.53.1.42", "eth"),
    Charger("himix04", "10.53.1.43", "eth"),
    Charger("himix05", "10.53.1.44", "eth"),
    Charger("himix06", "10.53.1.45", "eth"),
    Charger("himix07", "10.53.1.46", "eth"),
    Charger("himix08", "10.53.1.47", "eth"),
    Charger("himix09", "10.53.1.48", "eth"),
    Charger("himix10", "10.53.1.49", "eth"),
    Charger("himix11", "10.53.1.50", "eth"),
    Charger("himix12", "10.53.1.51", "eth"),
    Charger("himix13", "10.53.1.52", "eth"),
    Charger("12345678901", "10.53.1.53", "eth"),
]

# FIX: SSH_CONCURRENCY definida junto à lista, sem versão comentada no topo.
# Permite todas as consultas em paralelo (uma por carregador).
SSH_CONCURRENCY = len(CHARGERS)


class SSHConnectionManager:
    """Mantém conexões SSH reutilizáveis para reduzir custo de reconexão."""

    def __init__(self) -> None:
        # As conexões são indexadas por IP, pois cada carregador tem um endereço
        # VPN único e pode ser consultado várias vezes durante a execução.
        self.connections: dict[str, asyncssh.SSHClientConnection] = {}

    async def get_connection(self, charger: Charger) -> asyncssh.SSHClientConnection:
        """Abre ou reaproveita uma conexão SSH válida para o carregador."""

        conn = self.connections.get(charger.vpn_ip)

        if conn is not None and not conn.is_closed():
            return conn

        conn = await asyncssh.connect(
            charger.vpn_ip,
            port=charger.ssh_port,
            username=charger.ssh_user,
            client_keys=[str(SSH_KEY_PATH)],
            known_hosts=None,
            connect_timeout=3,
        )

        self.connections[charger.vpn_ip] = conn
        return conn

    async def run(
        self,
        charger: Charger,
        command: str,
        timeout_s: int = 5,
    ) -> tuple[bool, str]:
        """Executa um comando remoto e retorna sucesso + saída textual."""

        try:
            conn = await self.get_connection(charger)

            result = await asyncio.wait_for(
                conn.run(command, check=False),
                timeout=timeout_s,
            )

            if result.exit_status == 0:
                return True, result.stdout.strip()

            return False, result.stderr.strip() or result.stdout.strip()

        except asyncio.TimeoutError:
            await self.close_one(charger.vpn_ip)
            return False, "timeout"

        except (asyncssh.Error, OSError) as exc:
            await self.close_one(charger.vpn_ip)
            return False, str(exc)

    async def close_one(self, ip: str) -> None:
        """Fecha e remove do cache a conexão SSH de um IP específico."""

        conn = self.connections.pop(ip, None)

        if conn is not None:
            conn.close()
            await conn.wait_closed()

    async def close_all(self) -> None:
        """Fecha todas as conexões abertas ao encerrar o aplicativo."""

        for ip in list(self.connections.keys()):
            await self.close_one(ip)


def field(fields: list[str], number: int, default: str = "") -> str:
    """Retorna um campo 1-based da linha do journal já separada por espaços."""

    index = number - 1
    if index < 0 or index >= len(fields):
        return default
    return fields[index]


def dim(text: str) -> str:
    """Aplica estilo apagado do Rich/Textual em textos de placeholder."""

    return f"[dim]{text}[/dim]"


def parse_int(value: str, default: int = 0) -> int:
    """Converte texto em inteiro, usando default quando o valor vier inválido."""

    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_float(value: str, default: float = 0.0) -> float:
    """Converte texto em float, usando default quando o valor vier inválido."""

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def format_seconds_to_hms(value: str) -> str:
    """Formata segundos totais como HH:MM:SS para exibir tempo de carga."""

    total_seconds = parse_int(value)

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


async def ping_host(ip: str, timeout_s: int = 2) -> bool:
    """Retorna True quando o host responde a um ping ICMP."""

    system = platform.system().lower()

    # O ping usa flags diferentes no Windows e em sistemas Unix-like.
    if system == "windows":
        cmd = ["ping", "-n", "1", "-w", str(timeout_s * 1000), ip]
    else:
        cmd = ["ping", "-c", "1", "-W", str(timeout_s), ip]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=timeout_s + 1)
        return proc.returncode == 0

    except (asyncio.TimeoutError, FileNotFoundError):
        return False


# FIX: assinatura quebrada em múltiplas linhas para respeitar PEP 8 (< 100 chars)
async def ping_latency(
    host: str = PING_TARGET_HOST,
    timeout_s: int = PING_TIMEOUT_S,
) -> tuple[bool, int]:
    """Mede a latência aproximada de um ping ICMP em milissegundos."""

    system = platform.system().lower()

    if system == "windows":
        cmd = ["ping", "-n", "1", "-w", str(timeout_s * 1000), host]
    else:
        cmd = ["ping", "-c", "1", "-W", str(timeout_s), host]

    start = time.monotonic()

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=timeout_s + 1)

        if proc.returncode != 0:
            return False, 0

        elapsed_ms = int((time.monotonic() - start) * 1000)
        return True, elapsed_ms

    except (asyncio.TimeoutError, FileNotFoundError):
        return False, 0


class TopBar(Horizontal):
    """Header customizado com título à esquerda e status à direita."""

    def compose(self) -> ComposeResult:
        yield Static("All Chargers", id="topbar_title")
        yield Static("", id="topbar_status")


class ChargerMonitorApp(App):
    """Aplicativo Textual que mostra o estado dos carregadores em uma tabela."""

    CSS = """
    TopBar {
        height: 1;
        background: $boost;
    }

    #topbar_title {
        width: 1fr;
        content-align: left middle;
        padding-left: 1;
        text-style: bold;
    }

    #topbar_status {
        width: auto;
        content-align: right middle;
        padding-right: 1;
    }

    DataTable {
        height: 1fr;
    }
    """

    BINDINGS = [
        ("q", "quit", "Sair"),
        ("r", "refresh", "Atualizar agora"),
    ]

    def compose(self) -> ComposeResult:
        """Monta os widgets principais: cabeçalho, tabela e rodapé."""

        yield TopBar()
        yield DataTable()
        yield Footer()

    def on_mount(self) -> None:
        """Configura a tabela e agenda a atualização periódica dos dados."""

        self.title = "All Chargers"
        self.table = self.query_one(DataTable)
        self.table.cursor_type = None
        self.table.zebra_stripes = True
        self.ssh = SSHConnectionManager()
        self.topbar_status = self.query_one("#topbar_status", Static)
        self.ping_status_text = dim("🌐 -- ms")
        self.last_refresh_text = dim("⟳ --:--:--")
        self.update_status_bar()

        # Histórico por carregador dos bits de power module vistos recentemente.
        # Estrutura: {vpn_ip: {bit: timestamp_monotonic_do_último_aparecimento}}
        self.pm_bits_last_seen: dict[str, dict[int, float]] = {}

        # Cada tupla define: rótulo visível, chave interna da coluna e largura.
        columns = [
            ("  id", "id", 12),
            ("n", "net", 2),
            ("st", "charge_state", 3),
            ("f", "fan", 2),
            ("°C", "temps", 15),
            ("V", "voltage", 5),
            ("I", "current", 5),
            ("Iev", "current_request", 5),
            ("W", "power", 7),
            ("%", "battery_percent", 4),
            ("chrgtime", "charge_time", 8),
            ("bat", "battery_voltage", 5),
            ("enrgy", "energy", 6),
            ("e", "emergency", 2),
            ("Iil", "input_leakage_current", 4),
            ("Rno", "normalized_output_resistance", 5),
            ("eoctime", "eta", 9),
            ("29", "module_29", 4),
            ("pm bits", "pm_bits", 14),
        ]

        for label, key, width in columns:
            self.table.add_column(label, key=key, width=width)

        # FIX: add_row via dict de placeholders mapeados às chaves de coluna,
        # evitando desalinhamento silencioso se a ordem das colunas mudar.
        initial_placeholders: dict[str, str] = {
            "id": "",  # preenchido abaixo com format_id
            "net": dim("·"),
            "charge_state": dim("--"),
            "fan": dim("·"),
            "temps": dim("-----"),
            "voltage": dim("----"),
            "current": dim("---"),
            "current_request": dim("---"),
            "power": dim("------"),
            "battery_percent": dim("---"),
            "charge_time": dim("--------"),
            "battery_voltage": dim("----"),
            "energy": dim("-----"),
            "emergency": dim("·"),
            "input_leakage_current": dim("---"),
            "normalized_output_resistance": dim("----"),
            "eta": dim("--------"),
            "module_29": dim("·"),
            "pm_bits": dim("-"),
        }

        for charger in CHARGERS:
            row_values = {**initial_placeholders, "id": self.format_id(charger)}
            self.table.add_row(
                *[row_values[key] for _, key, _ in columns],
                key=charger.vpn_ip,
            )

        # O semáforo limita quantas consultas simultâneas podem rodar. Aqui ele
        # usa o tamanho da lista, permitindo consultar todos em paralelo.
        self.semaphore = asyncio.Semaphore(SSH_CONCURRENCY)
        self.set_interval(POLL_INTERVAL_S, self.refresh_all)
        self.set_interval(PING_INTERVAL_S, self.update_ping_status)
        self.call_later(self.refresh_all)
        self.call_later(self.update_ping_status)

    async def on_unmount(self) -> None:
        """Fecha conexões SSH quando a interface é desmontada."""

        await self.ssh.close_all()

    async def action_refresh(self) -> None:
        """Atalho da tecla 'r': força uma atualização imediata."""

        await self.refresh_all()

    async def refresh_all(self) -> None:
        """Atualiza todos os carregadores de forma concorrente."""

        await asyncio.gather(
            *(self.check_one_charger(charger) for charger in CHARGERS)
        )
        self.update_last_refresh_status()

    def update_status_bar(self) -> None:
        """Atualiza o status no canto direito do header customizado."""

        self.topbar_status.update(
            f"{self.ping_status_text}   {self.last_refresh_text}"
        )

    def format_ping_status(self, ok: bool, latency_ms: int) -> str:
        """Formata o indicador simples de latência externa."""

        if not ok:
            return "[red]🌐 timeout[/red]"

        if latency_ms < 50:
            color = "green"
        elif latency_ms < 100:
            color = "yellow"
        else:
            color = "red"

        return f"[{color}]⇄ {latency_ms} ms[/{color}]"

    async def update_ping_status(self) -> None:
        """Atualiza periodicamente a latência até o host externo configurado."""

        ok, latency_ms = await ping_latency()
        self.ping_status_text = self.format_ping_status(ok, latency_ms)
        self.update_status_bar()

    def update_last_refresh_status(self) -> None:
        """Registra o timestamp local do último ciclo completo de refresh."""

        timestamp = datetime.now().strftime("%H:%M:%S")
        self.last_refresh_text = f"[gray]⟳ {timestamp}[/gray]"
        self.update_status_bar()

    def format_id(self, charger: Charger) -> str:
        """Adiciona um ícone visual conforme o tipo de rede do carregador."""

        if charger.network_type.lower() == "4g":
            return f"[cyan]⌁[/cyan] {charger.charge_point_id}"
        return f"[dim]▣[/dim] {charger.charge_point_id}"

    def format_network_status(self, ping_ok: bool, ssh_ok: bool | None) -> str:
        """Formata o status de rede: offline, ping sem SSH, ou SSH OK."""

        if not ping_ok:
            return "[red]●[/red]"
        if ssh_ok:
            return "[green]●[/green]"
        return "[yellow]●[/yellow]"

    def format_fan(self, fan_val: str) -> str:
        """Mostra a ventoinha ativa quando o valor recebido é diferente de zero."""

        return "[cyan]🌀[/cyan]" if fan_val != "0" else dim("●")

    def format_emergency(self, emergency_val: str) -> str:
        """Destaca alarme de emergência quando o campo vem diferente de zero."""

        return "[red]●[/red]" if emergency_val != "0" else dim("●")

    def is_charging(self, charge_state: str) -> bool:
        """Indica se o estado atual deve ser tratado como carregamento ativo."""

        return charge_state.lower() in CHARGING_STATES

    def format_charging_data(self, data: dict[str, str]) -> dict[str, str]:
        """Formata campos úteis apenas enquanto o carregador está carregando."""

        voltage = data["voltage"]
        current = data["current"]
        current_request = data["current_request"]

        # A potência exibida é calculada localmente a partir de tensão x corrente,
        # pois a linha de status traz esses valores separadamente.
        voltage_i = parse_int(voltage)
        current_i = parse_int(current)
        power = voltage_i * current_i

        return {
            "voltage": voltage,
            "current": current,
            "current_request": f"{parse_float(current_request):.1f}",
            "power": f"{power:06d}",
            "battery_percent": data["battery_percent"],
            "charge_time": format_seconds_to_hms(data["charge_time"]),
            "battery_voltage": data["battery_voltage"],
            "energy": data["energy"],
            "input_leakage_current": data["input_leakage_current"],
            "normalized_output_resistance": data["normalized_output_resistance"],
            "eta": data["eta"],
        }

    def blank_charging_data(self) -> dict[str, str]:
        """Marcadores apagados para campos escondidos fora da carga ativa."""

        return {
            "voltage": dim("----"),
            "current": dim("---"),
            "current_request": dim("---"),
            "power": dim("------"),
            "battery_percent": dim("---"),
            "charge_time": dim("--------"),
            "battery_voltage": dim("----"),
            "energy": dim("-----"),
            "input_leakage_current": dim("---"),
            "normalized_output_resistance": dim("----"),
            "eta": dim("--------"),
        }

    def format_module_29(self, module1_output: str, module2_output: str) -> str:
        """Mostra alarmes 29 em duas posições: módulo 1 / módulo 2."""

        module1_has_29 = "29" in module1_output
        module2_has_29 = "29" in module2_output

        if not module1_has_29 and not module2_has_29:
            return dim("●")

        left = "[red]![/red]" if module1_has_29 else dim("·")
        right = "[red]![/red]" if module2_has_29 else dim("·")
        return f"{left}{right}"

    async def get_module_output(self, charger: Charger) -> tuple[bool, str]:
        """Busca no journal os logs recentes dos módulos (module1 e module2).

        FIX: extraído como método único para evitar duas chamadas SSH idênticas
        que antes ocorriam em get_module_29_status e get_pm_bits_status.
        """

        command = (
            "journalctl -u chargepoint.service "
            f'--since "{JOURNAL_LOOKBACK}" '
            "-o cat --no-pager "
            "| grep -E 'module1|module2' "
            "| tail -n 20"
        )

        return await self.ssh.run(charger, command, timeout_s=8)

    async def get_module_29_status(self, output: str) -> str:
        """Detecta alarme 29 a partir da saída já obtida do journal."""

        lines = output.splitlines()
        module1_lines = [l for l in lines if "module1" in l]
        module2_lines = [l for l in lines if "module2" in l]

        module1_last = module1_lines[-1] if module1_lines else ""
        module2_last = module2_lines[-1] if module2_lines else ""

        return self.format_module_29(module1_last, module2_last)

    def extract_pm_bits(self, output: str) -> set[int]:
        """Extrai os bits atuais reportados pelos power modules."""

        bits: set[int] = set()

        for line in output.splitlines():
            # Formato esperado, por exemplo:
            #   module1   : clear
            #   module2   : 29 35 47
            #
            # Importante: analisar somente o texto depois dos dois-pontos, para
            # não capturar o número do próprio nome do módulo: module1/module2.
            if ":" not in line:
                continue

            payload = line.split(":", 1)[1].strip()

            if not payload or payload.lower() == "clear":
                continue

            # Os bits são reportados como string no log. Capturamos números
            # isolados sem impor limite superior fixo, então bits como 47 entram
            # normalmente.
            for match in re.finditer(r"(?<!\d)(\d+)(?!\d)", payload):
                bit = int(match.group(1))

                # O bit 29 tem coluna dedicada e não deve ser repetido em
                # pm_bits, para evitar informação duplicada na tabela.
                if bit == 29:
                    continue

                bits.add(bit)

        return bits

    def format_pm_bits(self, current_bits: set[int], retained_bits: set[int]) -> str:
        """Formata bits atuais e bits retidos em uma única lista sem duplicatas."""

        all_bits = current_bits | retained_bits

        if not all_bits:
            return dim("-")

        formatted_bits = []
        for bit in sorted(all_bits):
            if bit in current_bits:
                formatted_bits.append(str(bit))
            else:
                formatted_bits.append(dim(str(bit)))

        return " ".join(formatted_bits)

    def update_pm_bits_retention(self, charger: Charger, current_bits: set[int]) -> str:
        """Atualiza a retenção por bit e retorna o texto da coluna pm_bits."""

        now = time.monotonic()
        last_seen = self.pm_bits_last_seen.setdefault(charger.vpn_ip, {})

        for bit in current_bits:
            last_seen[bit] = now

        expired_bits = [
            bit
            for bit, last_seen_time in last_seen.items()
            if now - last_seen_time > PM_BITS_RETENTION_S
        ]

        for bit in expired_bits:
            del last_seen[bit]

        retained_bits = set(last_seen) - current_bits
        return self.format_pm_bits(current_bits, retained_bits)

    def get_pm_bits_status(self, charger: Charger, output: str) -> str:
        """Aplica retenção visual aos bits da saída já obtida do journal."""

        current_bits = self.extract_pm_bits(output)
        return self.update_pm_bits_retention(charger, current_bits)

    async def get_status_line(self, charger: Charger) -> tuple[bool, dict[str, str]]:
        """Lê a linha de status mais recente do chargepoint.service via journal."""

        command = (
            "journalctl -u chargepoint.service "
            f'--since "{JOURNAL_LOOKBACK}" '
            "-o cat --no-pager "
            '| grep -F "04 64" '
            "| tail -n 1"
        )

        ok, output = await self.ssh.run(charger, command, timeout_s=8)
        if not ok or not output:
            return False, {}

        fields = output.split()

        # O maior campo usado abaixo é o 35 (eta). Se a linha vier menor, ela
        # provavelmente está incompleta ou não segue o formato esperado.
        if len(fields) < 35:
            return False, {}

        # Os índices seguem a posição dos campos no protocolo/log "04 64".
        # A função field() usa numeração humana (1-based) para bater com a
        # documentação ou planilhas de mapeamento desses campos.
        return True, {
            "st": field(fields, 3),
            "voltage": field(fields, 6),
            "current": field(fields, 7),
            "current_request": field(fields, 8),
            "battery_percent": field(fields, 9),
            "charge_time": field(fields, 10),
            "battery_voltage": field(fields, 11),
            "energy": field(fields, 12),
            "fan": field(fields, 13),
            "temps": " ".join(field(fields, index) for index in range(14, 19)),
            "emergency": field(fields, 21),
            "input_leakage_current": field(fields, 22),
            "normalized_output_resistance": field(fields, 23),
            "eta": field(fields, 35),
        }

    async def check_one_charger(self, charger: Charger) -> None:
        """Consulta ping, SSH e status de um carregador, atualizando sua linha."""

        async with self.semaphore:
            ping_ok = await ping_host(charger.vpn_ip)
            ssh_ok = None

            # Começa com uma linha neutra. Conforme as consultas têm sucesso,
            # apenas os campos confiáveis são substituídos por valores reais.
            row = {
                "charge_state": dim("--"),
                "fan": dim("·"),
                "temps": dim("-----"),
                "emergency": dim("·"),
                "module_29": dim("·"),
                "pm_bits": dim("-"),
                **self.blank_charging_data(),
            }

            if ping_ok:
                # FIX: uma única chamada SSH busca os logs de module1/module2,
                # eliminando a chamada duplicada que existia antes.
                ok, data = await self.get_status_line(charger)
                module_ok, module_output = await self.get_module_output(charger)

                ssh_ok = ok or module_ok

                if module_ok:
                    row["module_29"] = await self.get_module_29_status(module_output)
                    row["pm_bits"] = self.get_pm_bits_status(charger, module_output)

                if ok:
                    charge_state = data["st"]
                    row.update(
                        {
                            "charge_state": charge_state,
                            "fan": self.format_fan(data["fan"]),
                            "temps": data["temps"],
                            "emergency": self.format_emergency(data["emergency"]),
                        }
                    )

                    # Dados elétricos e de bateria só são exibidos em estados de
                    # carga ativa, para não sugerir leituras úteis em repouso.
                    if self.is_charging(charge_state):
                        row.update(self.format_charging_data(data))
                else:
                    row["charge_state"] = "[yellow]--[/yellow]"

            row["net"] = self.format_network_status(ping_ok, ssh_ok)

            # Atualiza somente as células da linha do carregador consultado.
            for key, value in row.items():
                self.table.update_cell(charger.vpn_ip, key, value)


if __name__ == "__main__":
    # Ponto de entrada quando o arquivo é executado diretamente.
    app = ChargerMonitorApp()
    app.run()
