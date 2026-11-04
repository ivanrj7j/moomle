"""Telemetry TUI for Moomle Crawler Jobs.

Provides a live, multi-panel terminal telemetry interface.
Features the Moomle logo rendered in Google rainbow colors, real-time progress
tracking, crawl metrics, live job monitor, and rolling system logs.
"""

import logging
import sys
from collections import deque
from datetime import datetime
from pathlib import Path
from time import time

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
    colors, live progress tracking, crawl metrics, active worker telemetry, and rolling logs.

    Attributes:
        cap: Maximum crawl limit (None for endurance mode).
        batchSize: Concurrency limit per batch.
        logDir: Directory path where persistent log files are stored.
    """

    def __init__(self, cap: int | None = None, batchSize: int = 10, logDir: str = "logs") -> None:
        """Initializes the telemetry UI, loggers, console, and progress monitors.

        Args:
            cap: Maximum number of jobs to crawl (None for unlimited).
            batchSize: Number of concurrent jobs in each sequential batch.
            logDir: Path to the directory where file logs should be written.
        """
        self.cap = cap
        self.batchSize = batchSize
        self.logDir = Path(logDir)
        self.logDir.mkdir(parents=True, exist_ok=True)
        self.logFile = self.logDir / "run_jobs.log"

        # General run stats
        self.startTime = time()
        self.totalBatches = 0
        self.currentBatch = 0
        self.jobsCompleted = 0
        self.jobsFailed = 0
        self.activeJobs: dict[int, dict] = {}
        self.recentJobs: deque[dict] = deque(maxlen=6)
        self.logs: deque[tuple[str, str, str]] = deque(maxlen=6)

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

    def batchStarted(self, batchIndex: int, totalBatches: int, batchJobCount: int) -> None:
        """Notifies telemetry of the start of a new sequential batch.

        Args:
            batchIndex: The 1-based index of the starting batch.
            totalBatches: Total number of planned batches.
            batchJobCount: Number of jobs scheduled in this batch.
        """
        self.currentBatch = batchIndex
        self.totalBatches = totalBatches
        self.logMessage(
            f"🏎️ Batch {batchIndex}/{totalBatches} — Launched {batchJobCount} concurrent jobs",
            level="INFO",
        )

    def batchCompleted(self, batchIndex: int, batchSuccesses: int, batchErrors: int) -> None:
        """Notifies telemetry of the completion of a batch.

        Args:
            batchIndex: The 1-based index of the completed batch.
            batchSuccesses: Number of successful crawls in the batch.
            batchErrors: Number of failed crawls in the batch.
        """
        self.logMessage(
            f"🏁 Batch {batchIndex} Completed — {batchSuccesses} OK, {batchErrors} ERR",
            level="INFO" if batchErrors == 0 else "WARN",
        )

    def jobStarted(self, jobId: int, url: str) -> None:
        """Records the beginning of an individual asynchronous crawl job.

        Args:
            jobId: Database ID of the job.
            url: Target URL being crawled.
        """
        self.activeJobs[jobId] = {"url": url, "startTime": time()}

    def jobFinished(
        self,
        jobId: int,
        url: str,
        success: bool,
        duration: float,
        errorMsg: str | None = None,
    ) -> None:
        """Records the completion of an individual crawl job and updates stats.

        Args:
            jobId: Database ID of the job.
            url: Target URL crawled.
            success: Whether the crawl was successful.
            duration: Duration of crawl execution in seconds.
            errorMsg: Optional error string if the crawl failed.
        """
        self.activeJobs.pop(jobId, None)

        if success:
            self.jobsCompleted += 1
            if self.progress is not None and self.progressTaskId is not None:
                self.progress.update(self.progressTaskId, completed=self.jobsCompleted)
            self.recentJobs.append({
                "id": jobId,
                "url": url,
                "status": "[bold green]DONE [200][/]",
                "duration": f"{duration:.2f}s",
            })
            self.logMessage(f"Job #{jobId} crawled in {duration:.2f}s: {url}", level="INFO")
        else:
            self.jobsFailed += 1
            self.recentJobs.append({
                "id": jobId,
                "url": url,
                "status": "[bold red]ERR[/]",
                "duration": f"{duration:.2f}s",
            })
            self.logMessage(f"Job #{jobId} failed ({errorMsg or 'Error'}): {url}", level="WARN")

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
        if self.cap is not None and self.progress is not None:
            return Panel(self.progress, border_style="#34A853", padding=(0, 1))

        elapsed = max(time() - self.startTime, 0.001)
        speed = self.jobsCompleted / elapsed
        text = Text(justify="center")
        text.append("🏎️  ENDURANCE MODE (CAP: ∞)  ", style="bold cyan")
        text.append(f"•  Completed: {self.jobsCompleted} crawls  ", style="bold #34A853")
        text.append(f"•  Pace: {speed:.1f} jobs/s  ", style="bold #FBBC05")
        text.append(f"•  Elapsed: {int(elapsed // 60):02d}:{int(elapsed % 60):02d}", style="bold #4285F4")
        return Panel(text, border_style="#34A853", padding=(0, 1))

    def _renderTelemetry(self) -> Panel:
        """Builds the general run statistics table.

        Returns:
            Rich Panel displaying elapsed time, speeds, counts, and health.
        """
        elapsed = max(time() - self.startTime, 0.001)
        speed = self.jobsCompleted / elapsed
        totalProcessed = self.jobsCompleted + self.jobsFailed
        successRate = (self.jobsCompleted / max(totalProcessed, 1)) * 100
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        timeStr = f"{mins:02d}:{secs:02d}"

        batchStr = f"{self.currentBatch} / {self.totalBatches}" if self.totalBatches else "STANDBY"
        workersStr = f"{len(self.activeJobs)} / {self.batchSize}"

        table = Table(box=None, show_header=False, expand=True, padding=(0, 1))
        table.add_column(justify="left", style="bold white")
        table.add_column(justify="right", style="bold #FBBC05")

        table.add_row("⏱️  Session Time:", timeStr)
        table.add_row("🏎️  Current Batch:", batchStr)
        table.add_row("👥  Active Workers:", workersStr)
        table.add_row("✅  Completed Crawls:", f"[bold #34A853]{self.jobsCompleted}[/]")
        table.add_row("❌  Failed Crawls:", f"[bold #EA4335]{self.jobsFailed}[/]")
        table.add_row("⚡  Crawl Pace:", f"{speed:.2f} j/s")
        table.add_row("🎯  Success Rate:", f"{successRate:.1f}%")
        table.add_row("💾  Database:", "[bold green]ONLINE[/]")

        return Panel(table, title="[bold #FBBC05]📊 Telemetry Stats[/]", border_style="#FBBC05")

    def _renderLiveTrack(self) -> Panel:
        """Builds the live jobs monitor table showing active and recent crawl jobs.

        Returns:
            Rich Panel displaying active workers and recent completions.
        """
        table = Table(expand=True, box=None, show_header=True, header_style="bold #4285F4", padding=(0, 1))
        table.add_column("Job#", width=5, justify="right")
        table.add_column("Target URL", ratio=1)
        table.add_column("Status", width=15, justify="center")
        table.add_column("Duration", width=9, justify="right")

        # Show active jobs first
        for jobId, jobData in list(self.activeJobs.items())[:4]:
            url = jobData["url"]
            if len(url) > 30:
                url = url[:27] + "..."
            duration = f"{time() - jobData['startTime']:.1f}s"
            table.add_row(str(jobId), url, "[bold cyan]⚡ CRAWLING[/]", duration)

        # Then show recently finished jobs
        for jobData in list(self.recentJobs)[-4:]:
            url = jobData["url"]
            if len(url) > 30:
                url = url[:27] + "..."
            table.add_row(str(jobData["id"]), url, jobData["status"], jobData["duration"])

        if not self.activeJobs and not self.recentJobs:
            table.add_row("-", "Standing by. Waiting for crawler jobs...", "[dim]STANDBY[/]", "-")

        return Panel(table, title="[bold #4285F4]🏎️ Live Jobs Monitor[/]", border_style="#4285F4")

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
        """Stops the live display and prints the summary banner."""
        if self.live is not None:
            self.live.stop()
            self.live = None

        elapsed = max(time() - self.startTime, 0.001)
        speed = self.jobsCompleted / elapsed
        self.logMessage(
            f"🏁 === Session Finished: {self.jobsCompleted} crawled, {self.jobsFailed} errors, "
            f"elapsed: {elapsed:.1f}s ({speed:.2f} j/s) ===",
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
            f" Crawling Finished! Total Crawled: {self.jobsCompleted} | "
            f"Errors: {self.jobsFailed} | Time: {elapsed:.1f}s | Speed: {speed:.2f} crawls/s\n",
            style="bold white",
        )
        self.console.print(banner)

    def __enter__(self) -> "PitwallTelemetryUI":
        """Enters the context manager and starts live telemetry."""
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exits the context manager and cleans up."""
        self.stop()


# Alias for clean and flexible importing
TelemetryUI = PitwallTelemetryUI
