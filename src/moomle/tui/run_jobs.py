"""Telemetry TUI for Moomle Crawler Jobs consuming data from RunJobs orchestrator."""

import asyncio
import logging
import sys
from collections import deque
from datetime import datetime
from pathlib import Path
from time import time
from typing import Any

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text


class PitwallTelemetryUI:
    """Telemetry Terminal User Interface for Moomle crawler jobs.

    Provides a live telemetry dashboard featuring the Moomle logo in Google rainbow
    colors, JobProducer buffer and throughput stats, active worker URL monitoring,
    and rolling system logs. Receives all live state directly from RunJobs and handles
    shutdown signals cleanly without tracebacks.

    Attributes:
        runner: The RunJobs runner instance supplying live telemetry data.
        cap: Maximum crawl limit (None for endurance mode).
        batchSize: Number of jobs dispatched per batch.
        logDir: Directory path where persistent log files are stored.
    """

    def __init__(
        self,
        runner: Any = None,
        cap: int | None = None,
        batchSize: int = 10,
        logDir: str = "logs",
    ) -> None:
        """Initializes the telemetry UI, loggers, console, and progress monitors.

        Args:
            runner: Optional RunJobs runner supplying telemetry snapshots.
            cap: Maximum number of jobs to crawl (None for unlimited).
            batchSize: Number of jobs per worker batch (defaults to 10).
            logDir: Path to the directory where file logs should be written.
        """
        self.runner = runner
        self.cap = cap if cap is not None else getattr(runner, "cap", None)
        self.batchSize = batchSize
        self.logDir = Path(logDir)
        self.logDir.mkdir(parents=True, exist_ok=True)
        self.logFile = self.logDir / "run_jobs.log"

        # General run stats
        self.startTime = time()
        self.jobsCompleted = 0
        self.jobsFailed = 0
        self.logs: deque[tuple[str, str, str]] = deque(maxlen=6)
        self._isStopped = False

        # File logger setup
        self.logger = logging.getLogger("moomle.tui.run_jobs")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        fileHandler = logging.FileHandler(self.logFile, encoding="utf-8")
        fileHandler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
        self.logger.addHandler(fileHandler)

        # Console setup
        self.console = Console(force_terminal=True, legacy_windows=False)
        self.live: Live | None = None

        # Progress bar setup if cap is provided
        if self.cap is not None:
            self.progress = Progress(
                TextColumn("[bold #4285F4]{task.description}"),
                BarColumn(complete_style="#4285F4", finished_style="#34A853"),
                TextColumn("[bold #FBBC05]{task.percentage:>3.0f}%[/]"),
                TextColumn("[bold white]({task.completed}/{task.total} crawls)[/]"),
                TimeRemainingColumn(),
                console=self.console,
            )
            self.progressTaskId = self.progress.add_task("Crawl Progress", total=self.cap)
        else:
            self.progress = None
            self.progressTaskId = None

    def bind(self, runner: Any) -> None:
        """Binds the RunJobs runner instance providing telemetry snapshots.

        Args:
            runner: The RunJobs orchestrator instance.
        """
        self.runner = runner
        if self.cap is None and getattr(runner, "cap", None) is not None:
            self.cap = runner.cap
            if self.progress is None:
                self.progress = Progress(
                    TextColumn("[bold #4285F4]{task.description}"),
                    BarColumn(complete_style="#4285F4", finished_style="#34A853"),
                    TextColumn("[bold #FBBC05]{task.percentage:>3.0f}%[/]"),
                    TextColumn("[bold white]({task.completed}/{task.total} crawls)[/]"),
                    TimeRemainingColumn(),
                    console=self.console,
                )
                self.progressTaskId = self.progress.add_task("Crawl Progress", total=self.cap)

    def logMessage(self, message: str, level: str = "INFO") -> None:
        """Logs a message both to the persistent file and to the on-screen live feed buffer.

        Args:
            message: Text message describing the event.
            level: Severity level ('INFO', 'WARN', 'ERROR').
        """
        timeFormatted = datetime.now().strftime("%H:%M:%S")

        if level.upper() == "ERROR":
            self.logger.error(message)
        elif level.upper() in ("WARN", "WARNING"):
            self.logger.warning(message)
        else:
            self.logger.info(message)

        self.logs.append((timeFormatted, level.upper(), message))

    def _getTelemetrySnapshot(self) -> dict[str, Any]:
        """Queries RunJobs for the latest aggregated telemetry snapshot.

        Returns:
            Dictionary containing producer metrics and worker snapshots.
        """
        if self.runner is not None and hasattr(self.runner, "getTelemetryData"):
            return self.runner.getTelemetryData()

        return {
            "bufferLength": 0,
            "bufferSize": 1000,
            "producedCount": 0,
            "completedCount": self.jobsCompleted,
            "failedCount": self.jobsFailed,
            "inFlightCount": 0,
            "isRefilling": False,
            "cap": self.cap,
            "workers": [],
        }

    def _renderHeader(self) -> Panel:
        """Builds the top header banner with Google colors and emojis.

        Returns:
            Rich Panel displaying the Google-colored Moomle telemetry header.
        """
        title = Text(justify="center")
        title.append("🏎️  ", style="bold white")
        # Google rainbow: Blue, Red, Yellow, Blue, Green, Red
        title.append("m", style="bold #4285F4")
        title.append("o", style="bold #EA4335")
        title.append("o", style="bold #FBBC05")
        title.append("m", style="bold #4285F4")
        title.append("l", style="bold #34A853")
        title.append("e", style="bold #EA4335")
        title.append("  🏁  ", style="bold white")
        title.append("TELEMETRY DASHBOARD", style="bold white")
        title.append("  ⏱️", style="bold white")

        return Panel(title, border_style="#4285F4", padding=(0, 1))

    def _renderProgressBar(self) -> Panel:
        """Builds the progress bar panel if cap exists, or endurance stats if unlimited.

        Returns:
            Rich Panel representing overall crawl progress.
        """
        snapshot = self._getTelemetrySnapshot()
        completed = snapshot.get("completedCount", self.jobsCompleted)
        activeCap = snapshot.get("cap", self.cap)

        if activeCap is not None and self.progress is not None and self.progressTaskId is not None:
            self.progress.update(self.progressTaskId, completed=completed)
            return Panel(self.progress, border_style="#34A853", padding=(0, 1))

        elapsed = max(time() - self.startTime, 0.001)
        speed = completed / elapsed
        text = Text(justify="center")
        text.append("🏎️  ENDURANCE MODE (CAP: ∞)  ", style="bold cyan")
        text.append(f"•  Completed: {completed} crawls  ", style="bold #34A853")
        text.append(f"•  Pace: {speed:.1f} crawls/s  ", style="bold #FBBC05")
        text.append(f"•  Elapsed: {int(elapsed // 60):02d}:{int(elapsed % 60):02d}", style="bold #4285F4")
        return Panel(text, border_style="#34A853", padding=(0, 1))

    def _renderTelemetry(self) -> Panel:
        """Builds the general run statistics table using data returned by RunJobs.

        Returns:
            Rich Panel displaying elapsed time, buffer levels, counts, pace, and database health.
        """
        snapshot = self._getTelemetrySnapshot()
        elapsed = max(time() - self.startTime, 0.001)
        completed = snapshot.get("completedCount", self.jobsCompleted)
        failed = snapshot.get("failedCount", self.jobsFailed)
        produced = snapshot.get("producedCount", 0)
        inFlight = snapshot.get("inFlightCount", 0)
        bufferLen = snapshot.get("bufferLength", 0)
        bufferSize = snapshot.get("bufferSize", 1000)
        isRefilling = snapshot.get("isRefilling", False)

        speed = completed / elapsed
        totalProcessed = completed + failed
        successRate = (completed / max(totalProcessed, 1)) * 100
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        timeStr = f"{mins:02d}:{secs:02d}"

        bufferStr = f"{bufferLen} / {bufferSize}"
        dbStatus = "[bold yellow]REFILLING...[/]" if isRefilling else "[bold green]ONLINE[/]"

        table = Table(box=None, show_header=False, expand=True, padding=(0, 1))
        table.add_column(justify="left", style="bold white")
        table.add_column(justify="right", style="bold #FBBC05")

        table.add_row("⏱️  Session Time:", timeStr)
        table.add_row("📦  Job Buffer:", bufferStr)
        table.add_row("⚡  Crawl Pace:", f"{speed:.2f} crawls/s")
        table.add_row("🚀  Leased Jobs:", f"{produced}")
        table.add_row("👥  In-Flight:", f"{inFlight}")
        table.add_row("✅  Completed:", f"[bold #34A853]{completed}[/]")
        table.add_row("❌  Failed:", f"[bold #EA4335]{failed}[/]")
        table.add_row("🎯  Success Rate:", f"{successRate:.1f}%")
        table.add_row("💾  Database:", dbStatus)

        return Panel(table, title="[bold #FBBC05]📊 JobProducer Telemetry[/]", border_style="#FBBC05")

    def _renderLiveTrack(self) -> Panel:
        """Builds the live workers monitor table displaying each worker's current URL.

        Returns:
            Rich Panel displaying active workers, their current URLs, and crawl durations.
        """
        snapshot = self._getTelemetrySnapshot()
        workers = snapshot.get("workers", [])

        table = Table(expand=True, box=None, show_header=True, header_style="bold #4285F4", padding=(0, 1))
        table.add_column("W#", width=4, justify="right")
        table.add_column("Current URL", ratio=1)
        table.add_column("Batch", width=6, justify="center")
        table.add_column("Time", width=6, justify="right")

        if not workers:
            table.add_row("-", "Waiting for workers to initialize...", "-", "-")
            return Panel(table, title="[bold #4285F4]🏎️ Live Workers Monitor[/]", border_style="#4285F4")

        for w in workers:
            wId = f"#{w['workerId']}"
            batchProg = w.get("batchProgress", "-")
            status = w.get("status", "IDLE")
            url = w.get("currentUrl")
            durationStr = w.get("duration", "-")

            if url:
                displayUrl = url if len(url) <= 38 else url[:35] + "..."
            else:
                displayUrl = f"[dim]{status}[/]"

            table.add_row(wId, displayUrl, batchProg, durationStr)

        return Panel(table, title="[bold #4285F4]🏎️ Live Workers Monitor[/]", border_style="#4285F4")

    def _renderPitRadio(self) -> Panel:
        """Builds the rolling system events and feed log panel.

        Returns:
            Rich Panel displaying recent system and crawl events.
        """
        logText = Text()
        if not self.logs:
            logText.append("Telemetry standing by for events...\n", style="dim")
        else:
            for timeStr, level, msg in list(self.logs)[-5:]:
                logText.append(f"{timeStr} ", style="dim")
                if level == "ERROR":
                    logText.append(f"[{level}] ", style="bold red")
                elif level in ("WARN", "WARNING"):
                    logText.append(f"[{level}] ", style="bold #FBBC05")
                else:
                    logText.append(f"[{level}] ", style="bold #34A853")
                logText.append(f"{msg}\n")

        return Panel(
            logText,
            title="[bold #EA4335]📻 System Logs & Live Feed[/]",
            subtitle=f"[dim]{self.logFile}[/]",
            border_style="#EA4335",
        )

    def _renderLayout(self) -> Layout:
        """Composes the full telemetry screen layout.

        Returns:
            Rich Layout combining header, progress, telemetry, track, and radio.
        """
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="progress", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=7),
        )
        layout["main"].split_row(
            Layout(name="telemetry", ratio=1),
            Layout(name="track", ratio=1),
        )

        layout["header"].update(self._renderHeader())
        layout["progress"].update(self._renderProgressBar())
        layout["telemetry"].update(self._renderTelemetry())
        layout["track"].update(self._renderLiveTrack())
        layout["footer"].update(self._renderPitRadio())

        return layout

    def start(self) -> "PitwallTelemetryUI":
        """Starts the live telemetry display and initializes logging.

        Returns:
            Self instance.
        """
        self._isStopped = False
        self.logMessage("🏎️ === Moomle Telemetry Session Started ===", level="INFO")
        self.live = Live(
            get_renderable=self._renderLayout,
            console=self.console,
            refresh_per_second=4,
            screen=False,
        )
        self.live.start()
        return self

    def stop(self) -> None:
        """Stops the live display and prints the summary banner gracefully."""
        if self._isStopped:
            return
        self._isStopped = True

        if self.live is not None:
            try:
                self.live.stop()
            except Exception:
                pass
            self.live = None

        elapsed = max(time() - self.startTime, 0.001)
        snapshot = self._getTelemetrySnapshot()
        completed = snapshot.get("completedCount", self.jobsCompleted)
        failed = snapshot.get("failedCount", self.jobsFailed)
        speed = completed / elapsed

        self.logMessage(
            f"🏁 === Session Finished: {completed} crawled, {failed} errors, "
            f"elapsed: {elapsed:.1f}s ({speed:.2f} crawls/s) ===",
            level="INFO",
        )

        banner = Text()
        banner.append("\n🏁  ", style="bold white")
        banner.append("m", style="bold #4285F4")
        banner.append("o", style="bold #EA4335")
        banner.append("o", style="bold #FBBC05")
        banner.append("m", style="bold #4285F4")
        banner.append("l", style="bold #34A853")
        banner.append("e", style="bold #EA4335")
        banner.append(
            f" Crawling Stopped! Total Crawled: {completed} | "
            f"Errors: {failed} | Time: {elapsed:.1f}s | Speed: {speed:.2f} crawls/s\n",
            style="bold white",
        )
        self.console.print(banner)

    def __enter__(self) -> "PitwallTelemetryUI":
        """Enters the context manager and starts live telemetry."""
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        """Exits the context manager, stops live display, and suppresses keyboard interrupts."""
        self.stop()
        if exc_type is not None and issubclass(exc_type, (KeyboardInterrupt, asyncio.CancelledError)):
            return True
        return False


# Alias for clean and flexible importing
TelemetryUI = PitwallTelemetryUI
