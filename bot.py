from __future__ import annotations

import asyncio
import logging
import os
import random
import shutil
import threading
from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import discord
import yt_dlp
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ─── config ──────────────────────────────────────────────────────────────────

NAME = "SHE SINGS"
TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
PREFIX = os.getenv("PREFIX", "!")
STAY_CONNECTED = os.getenv("STAY_CONNECTED", "true").lower() == "true"
DEFAULT_VOLUME = float(os.getenv("DEFAULT_VOLUME", "0.5"))
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "").strip() or None

BRAND = 0xF45C8E
BRAND_SOFT = 0xFFB3CE
INK = 0x2B0B18
OK = 0x57F287
WARN = 0xFEE75C
ERR = 0xED4245

MAX_QUEUE = 500
MAX_PLAYLIST_ITEMS = 50
IDLE_DISCONNECT_SECONDS = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger("shesings")

YTDL_OPTIONS = {
    "format": "bestaudio/best",
    "quiet": True,
    "no_warnings": True,
    "default_search": "ytsearch1",
    "noplaylist": False,
    "extract_flat": False,
    "source_address": "0.0.0.0",
    "nocheckcertificate": True,
    "ignoreerrors": False,
    "cachedir": False,
    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
}

FFMPEG_BEFORE = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -nostdin -loglevel error"
FFMPEG_OPTIONS = "-vn"


# ─── helpers ─────────────────────────────────────────────────────────────────

def fmt_duration(seconds: Optional[float]) -> str:
    if not seconds:
        return "LIVE"
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def panel(title: str, description: str = "", colour: int = BRAND) -> discord.Embed:
    e = discord.Embed(title=title, description=description, colour=colour)
    e.set_footer(text=NAME)
    return e


async def send(ctx: commands.Context, *, content: str | None = None,
               embed: discord.Embed | None = None, ephemeral: bool = False) -> None:
    if ctx.interaction is not None:
        try:
            if not ctx.interaction.response.is_done():
                await ctx.interaction.response.send_message(
                    content=content, embed=embed, ephemeral=ephemeral)
                return
            await ctx.interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
            return
        except discord.HTTPException:
            pass
    await ctx.reply(content=content, embed=embed, mention_author=False)


@dataclass
class Track:
    query: str
    title: str
    duration: Optional[float]
    requester: discord.abc.User
    webpage_url: Optional[str] = None
    thumbnail: Optional[str] = None
    stream_url: Optional[str] = None


async def extract(query: str) -> dict:
    def _run() -> dict:
        with yt_dlp.YoutubeDL(YTDL_OPTIONS) as ydl:
            return ydl.extract_info(query, download=False)

    return await asyncio.get_running_loop().run_in_executor(None, _run)


async def build_tracks(query: str, requester: discord.abc.User) -> list[Track]:
    data = await extract(query)
    if not data:
        raise RuntimeError("no results")

    if data.get("entries"):
        tracks: list[Track] = []
        for entry in data["entries"][:MAX_PLAYLIST_ITEMS]:
            if not entry:
                continue
            url = entry.get("webpage_url") or entry.get("url")
            if not url:
                continue
            tracks.append(Track(
                query=url,
                title=entry.get("title") or "Unknown",
                duration=entry.get("duration"),
                requester=requester,
                webpage_url=url,
                thumbnail=entry.get("thumbnail"),
            ))
        if not tracks:
            raise RuntimeError("playlist had no playable entries")
        return tracks

    url = data.get("webpage_url") or data.get("url") or query
    return [Track(
        query=url,
        title=data.get("title") or "Unknown",
        duration=data.get("duration"),
        requester=requester,
        webpage_url=url,
        thumbnail=data.get("thumbnail"),
        stream_url=data.get("url"),
    )]


# ─── player ──────────────────────────────────────────────────────────────────

class MusicPlayer:
    def __init__(self, bot: "SheSings", guild: discord.Guild):
        self.bot = bot
        self.guild = guild
        self.queue: deque[Track] = deque()
        self.current: Optional[Track] = None
        self.voice: Optional[discord.VoiceClient] = None
        self.channel: Optional[discord.abc.Messageable] = None
        self.volume = DEFAULT_VOLUME
        self.loop_track = False
        self.loop_queue = False
        self.stay = STAY_CONNECTED
        self._lock = asyncio.Lock()
        self._skip = False
        self._idle: Optional[asyncio.Task] = None

    async def connect(self, channel: discord.VoiceChannel) -> None:
        if self.voice and self.voice.is_connected():
            if self.voice.channel != channel:
                await self.voice.move_to(channel)
            return
        self.voice = await channel.connect(self_deafen=True, timeout=30.0, reconnect=True)

    def _after(self, error: Optional[Exception]) -> None:
        asyncio.run_coroutine_threadsafe(self.play_next(error), self.bot.loop)

    async def _source(self, track: Track) -> discord.AudioSource:
        info = await extract(track.webpage_url or track.query)
        if not info:
            raise RuntimeError("extract failed")
        if info.get("entries"):
            info = info["entries"][0]
        url = info.get("url")
        if not url:
            raise RuntimeError("no stream url")

        track.title = info.get("title") or track.title
        track.duration = info.get("duration") or track.duration
        track.stream_url = url

        raw = discord.FFmpegPCMAudio(
            url, before_options=FFMPEG_BEFORE, options=FFMPEG_OPTIONS, executable=FFMPEG_PATH)
        return discord.PCMVolumeTransformer(raw, volume=self.volume)

    async def play_next(self, error: Optional[Exception] = None) -> None:
        async with self._lock:
            if error:
                log.warning("[%s] playback error: %r", self.guild.id, error)
            if self.voice is None or not self.voice.is_connected():
                return

            skipping = self._skip
            self._skip = False

            if self.current and not skipping:
                if self.loop_track:
                    self.queue.appendleft(self.current)
                elif self.loop_queue:
                    self.queue.append(self.current)

            self.current = None

            while self.queue:
                track = self.queue.popleft()
                try:
                    source = await self._source(track)
                except Exception as exc:
                    log.warning("[%s] drop %r: %r", self.guild.id, track.title, exc)
                    continue
                self.current = track
                self.voice.play(source, after=self._after)
                await self._announce(track)
                return

            await self._idle_soon()

    async def _announce(self, track: Track) -> None:
        if not self.channel:
            return
        e = panel("Now Playing", f"**[{track.title}]({track.webpage_url})**")
        e.add_field(name="Duration", value=f"`{fmt_duration(track.duration)}`", inline=True)
        e.add_field(name="Requested by", value=track.requester.mention, inline=True)
        e.add_field(name="Queue", value=f"`{len(self.queue)}` upcoming", inline=True)
        if track.thumbnail:
            e.set_thumbnail(url=track.thumbnail)
        try:
            await self.channel.send(embed=e, view=Controls(self.bot))
        except discord.HTTPException:
            pass

    async def _idle_soon(self) -> None:
        if self.stay:
            return
        if self._idle:
            self._idle.cancel()
        self._idle = self.bot.loop.create_task(self._idle_timer())

    async def _idle_timer(self) -> None:
        try:
            await asyncio.sleep(IDLE_DISCONNECT_SECONDS)
        except asyncio.CancelledError:
            return
        if not self.current and not self.queue and self.voice and self.voice.is_connected():
            await self.voice.disconnect(force=True)
            self.voice = None

    async def enqueue(self, tracks: list[Track]) -> int:
        added = 0
        for t in tracks:
            if len(self.queue) >= MAX_QUEUE:
                break
            self.queue.append(t)
            added += 1
        return added

    async def start_if_idle(self) -> None:
        if self.voice and not self.voice.is_playing() and self.current is None:
            await self.play_next()

    def skip(self) -> bool:
        if self.voice and (self.voice.is_playing() or self.voice.is_paused()):
            self._skip = True
            self.voice.stop()
            return True
        return False

    def pause(self) -> bool:
        if self.voice and self.voice.is_playing():
            self.voice.pause()
            return True
        return False

    def resume(self) -> bool:
        if self.voice and self.voice.is_paused():
            self.voice.resume()
            return True
        return False

    def set_volume(self, value: float) -> None:
        self.volume = max(0.0, min(2.0, value))
        src = self.voice.source if self.voice else None
        if isinstance(src, discord.PCMVolumeTransformer):
            src.volume = self.volume

    def shuffle(self) -> None:
        items = list(self.queue)
        random.shuffle(items)
        self.queue = deque(items)

    async def stop(self, disconnect: bool = True) -> None:
        self.queue.clear()
        self.current = None
        self.loop_track = self.loop_queue = False
        if self.voice:
            self.voice.stop()
            if disconnect and self.voice.is_connected():
                await self.voice.disconnect(force=True)
            self.voice = None


# ─── views ───────────────────────────────────────────────────────────────────

class Controls(discord.ui.View):
    def __init__(self, bot: "SheSings"):
        super().__init__(timeout=None)
        self.bot = bot

    def _player(self, interaction: discord.Interaction) -> MusicPlayer:
        return self.bot.player(interaction.guild)

    @discord.ui.button(emoji="⏯", label="Pause / Resume", style=discord.ButtonStyle.secondary, custom_id="ss:pause")
    async def pause_btn(self, interaction: discord.Interaction, _):
        p = self._player(interaction)
        if p.pause():
            await interaction.response.send_message(embed=panel("Paused", "Playback halted.", WARN), ephemeral=True)
        elif p.resume():
            await interaction.response.send_message(embed=panel("Resumed", "Playback continuing.", OK), ephemeral=True)
        else:
            await interaction.response.send_message(embed=panel("Nothing playing", "Queue is empty.", ERR), ephemeral=True)

    @discord.ui.button(emoji="⏭", label="Skip", style=discord.ButtonStyle.primary, custom_id="ss:skip")
    async def skip_btn(self, interaction: discord.Interaction, _):
        p = self._player(interaction)
        ok = p.skip()
        await interaction.response.send_message(
            embed=panel("Skipped" if ok else "Nothing to skip",
                        "Moving to the next track." if ok else "Queue is empty.",
                        OK if ok else ERR), ephemeral=True)

    @discord.ui.button(emoji="⏹", label="Stop", style=discord.ButtonStyle.danger, custom_id="ss:stop")
    async def stop_btn(self, interaction: discord.Interaction, _):
        p = self._player(interaction)
        await p.stop(disconnect=True)
        await interaction.response.send_message(
            embed=panel("Stopped", "Cleared the queue and left the channel.", ERR), ephemeral=True)

    @discord.ui.button(emoji="📜", label="Queue", style=discord.ButtonStyle.secondary, custom_id="ss:queue")
    async def queue_btn(self, interaction: discord.Interaction, _):
        p = self._player(interaction)
        await interaction.response.send_message(embed=queue_embed(p), ephemeral=True)

    @discord.ui.button(emoji="🔁", label="Loop", style=discord.ButtonStyle.secondary, custom_id="ss:loop")
    async def loop_btn(self, interaction: discord.Interaction, _):
        p = self._player(interaction)
        if p.loop_track:
            p.loop_track, p.loop_queue = False, True
        elif p.loop_queue:
            p.loop_track = p.loop_queue = False
        else:
            p.loop_track = True
        mode = "track" if p.loop_track else "queue" if p.loop_queue else "off"
        await interaction.response.send_message(
            embed=panel("Loop Mode", f"Set to **{mode}**.", BRAND_SOFT), ephemeral=True)


class QueuePages(discord.ui.View):
    def __init__(self, pages: list[discord.Embed]):
        super().__init__(timeout=120)
        self.pages = pages
        self.index = 0
        self._sync()

    def _sync(self) -> None:
        self.prev.disabled = self.index == 0
        self.nxt.disabled = self.index >= len(self.pages) - 1

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: discord.Interaction, _):
        self.index = max(0, self.index - 1)
        self._sync()
        await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def nxt(self, interaction: discord.Interaction, _):
        self.index = min(len(self.pages) - 1, self.index + 1)
        self._sync()
        await interaction.response.edit_message(embed=self.pages[self.index], view=self)


def queue_embed(p: MusicPlayer) -> discord.Embed:
    e = panel("Queue")
    if p.current:
        e.add_field(
            name="Now Playing",
            value=f"[{p.current.title}]({p.current.webpage_url}) — `{fmt_duration(p.current.duration)}`",
            inline=False,
        )
    if not p.queue:
        e.add_field(name="Up Next", value="*Nothing queued.*", inline=False)
    else:
        lines = [
            f"`{i:02d}` [{t.title}]({t.webpage_url}) — `{fmt_duration(t.duration)}`"
            for i, t in enumerate(list(p.queue)[:10], start=1)
        ]
        e.add_field(name="Up Next", value="\n".join(lines), inline=False)
        if len(p.queue) > 10:
            e.add_field(name="\u200b", value=f"*…and {len(p.queue) - 10} more*", inline=False)
    e.add_field(name="Loop", value=f"`{'track' if p.loop_track else 'queue' if p.loop_queue else 'off'}`", inline=True)
    e.add_field(name="Volume", value=f"`{int(p.volume * 100)}%`", inline=True)
    e.add_field(name="24/7", value=f"`{'on' if p.stay else 'off'}`", inline=True)
    e.set_footer(text=f"{NAME} · {len(p.queue)} queued")
    return e


def queue_pages(p: MusicPlayer) -> list[discord.Embed]:
    pages: list[discord.Embed] = []
    if p.current:
        head = panel("Now Playing",
                     f"**[{p.current.title}]({p.current.webpage_url})**\n"
                     f"`{fmt_duration(p.current.duration)}` · {p.current.requester.mention}")
        if p.current.thumbnail:
            head.set_image(url=p.current.thumbnail)
        pages.append(head)

    items = list(p.queue)
    chunk = 10
    for start in range(0, max(len(items), 1), chunk):
        block = items[start:start + chunk]
        e = panel(f"Queue · {start + 1}–{start + len(block)} of {len(items)}")
        if block:
            e.description = "\n".join(
                f"`{start + i:02d}` [{t.title}]({t.webpage_url}) — `{fmt_duration(t.duration)}`"
                for i, t in enumerate(block, start=1)
            )
        else:
            e.description = "*Queue is empty.*"
        pages.append(e)
    return pages


# ─── bot ─────────────────────────────────────────────────────────────────────

class SheSings(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        intents.voice_states = True
        intents.guilds = True
        super().__init__(command_prefix=PREFIX, intents=intents, help_command=None)
        self.players: dict[int, MusicPlayer] = {}
        self._presence_index = 0

    async def setup_hook(self) -> None:
        if not shutil.which("ffmpeg") and not FFMPEG_PATH:
            log.error("ffmpeg missing — audio will not play")
        self.add_view(Controls(self))
        try:
            await self.tree.sync()
            log.info("slash commands synced")
        except Exception as exc:
            log.warning("slash sync failed: %r", exc)
        self.rotate_presence.start()

    def player(self, guild: discord.Guild) -> MusicPlayer:
        p = self.players.get(guild.id)
        if p is None:
            p = MusicPlayer(self, guild)
            self.players[guild.id] = p
        return p

    async def on_ready(self) -> None:
        log.info("%s online as %s (%s) — %d guilds",
                 NAME, self.user, self.user.id, len(self.guilds))

    async def on_voice_state_update(self, member, before, after):
        if member.id != self.user.id or not STAY_CONNECTED:
            return
        p = self.players.get(member.guild.id)
        if p is None or not before.channel or after.channel is not None:
            return
        log.info("[%s] disconnected — rejoining %s", member.guild.id, before.channel)
        try:
            p.voice = await before.channel.connect(self_deafen=True, timeout=20.0, reconnect=True)
            await p.play_next()
        except Exception as exc:
            log.warning("rejoin failed: %r", exc)

    @tasks.loop(seconds=45)
    async def rotate_presence(self) -> None:
        cycle = [
            (discord.ActivityType.listening, f"{PREFIX}play · {NAME}"),
            (discord.ActivityType.listening, "your requests"),
            (discord.ActivityType.watching, f"{len(self.guilds)} servers"),
        ]
        kind, text = cycle[self._presence_index % len(cycle)]
        self._presence_index += 1
        await self.change_presence(activity=discord.Activity(type=kind, name=text))

    @rotate_presence.before_loop
    async def _wait(self) -> None:
        await self.wait_until_ready()


bot = SheSings()


@bot.check
async def _guild_only(ctx: commands.Context) -> bool:
    return ctx.guild is not None or (ctx.command is not None and ctx.command.name == "help")


# ─── commands ────────────────────────────────────────────────────────────────

def guard(ctx: commands.Context) -> tuple[bool, Optional[discord.Embed]]:
    if not ctx.author.voice or not ctx.author.voice.channel:
        return False, panel("Join a voice channel first", "SHE SINGS needs a channel to sing into.", ERR)
    return True, None


@bot.hybrid_command(name="play", aliases=["p"], description="Queue a song by name, YouTube link, or playlist.")
async def play(ctx: commands.Context, *, query: str):
    ok, err = guard(ctx)
    if not ok:
        return await send(ctx, embed=err, ephemeral=True)

    p = bot.player(ctx.guild)
    p.channel = ctx.channel
    await p.connect(ctx.author.voice.channel)
    await ctx.defer()

    try:
        tracks = await build_tracks(query, ctx.author)
    except Exception as exc:
        return await send(ctx, embed=panel("No results", f"Couldn't find anything for `{query}`.\n`{exc}`", ERR))

    added = await p.enqueue(tracks)
    if added == 0:
        return await send(ctx, embed=panel("Queue full", f"Limit is {MAX_QUEUE} tracks.", WARN))

    if added == 1:
        t = tracks[0]
        e = panel("Added to Queue", f"**[{t.title}]({t.webpage_url})**")
        e.add_field(name="Duration", value=f"`{fmt_duration(t.duration)}`", inline=True)
        e.add_field(name="Position", value=f"`{len(p.queue)}`", inline=True)
        if t.thumbnail:
            e.set_thumbnail(url=t.thumbnail)
    else:
        e = panel("Playlist Added", f"Queued **{added}** tracks.")
        e.add_field(name="First", value=tracks[0].title, inline=False)

    await send(ctx, embed=e)
    await p.start_if_idle()


@bot.hybrid_command(name="pause", description="Pause playback.")
async def pause(ctx: commands.Context):
    p = bot.player(ctx.guild)
    await send(ctx, embed=panel("Paused", "Playback halted.", WARN) if p.pause()
               else panel("Nothing playing", "Queue is idle.", ERR), ephemeral=True)


@bot.hybrid_command(name="resume", aliases=["unpause"], description="Resume playback.")
async def resume(ctx: commands.Context):
    p = bot.player(ctx.guild)
    await send(ctx, embed=panel("Resumed", "Playback continuing.", OK) if p.resume()
               else panel("Not paused", "Nothing to resume.", ERR), ephemeral=True)


@bot.hybrid_command(name="skip", aliases=["s", "next"], description="Skip the current track.")
async def skip(ctx: commands.Context):
    p = bot.player(ctx.guild)
    ok = p.skip()
    await send(ctx, embed=panel("Skipped", "Moving on." if ok else "Nothing to skip.",
                                OK if ok else ERR), ephemeral=True)


@bot.hybrid_command(name="stop", aliases=["leave"], description="Stop and disconnect.")
async def stop(ctx: commands.Context):
    p = bot.player(ctx.guild)
    await p.stop(disconnect=True)
    await send(ctx, embed=panel("Stopped", "Queue cleared. Left the channel.", ERR))


@bot.hybrid_command(name="queue", aliases=["q"], description="Show the queue.")
async def queue_cmd(ctx: commands.Context):
    p = bot.player(ctx.guild)
    pages = queue_pages(p)
    await send(ctx, embed=pages[0])
    if len(pages) > 1 and ctx.interaction is not None and ctx.interaction.response.is_done():
        try:
            await ctx.interaction.edit_original_response(view=QueuePages(pages))
        except discord.HTTPException:
            pass


@bot.hybrid_command(name="nowplaying", aliases=["np"], description="Show the current track.")
async def nowplaying(ctx: commands.Context):
    p = bot.player(ctx.guild)
    if not p.current:
        return await send(ctx, embed=panel("Nothing playing", "Queue is idle.", ERR), ephemeral=True)
    t = p.current
    e = panel("Now Playing", f"**[{t.title}]({t.webpage_url})**")
    e.add_field(name="Duration", value=f"`{fmt_duration(t.duration)}`", inline=True)
    e.add_field(name="Requested by", value=t.requester.mention, inline=True)
    if t.thumbnail:
        e.set_image(url=t.thumbnail)
    await send(ctx, embed=e)


@bot.hybrid_command(name="loop", description="Loop the track, the queue, or turn looping off.")
async def loop_cmd(ctx: commands.Context, mode: str = "track"):
    p = bot.player(ctx.guild)
    mode = mode.lower()
    if mode in ("off", "none", "0"):
        p.loop_track = p.loop_queue = False
    elif mode in ("queue", "all"):
        p.loop_queue, p.loop_track = True, False
    else:
        p.loop_track, p.loop_queue = True, False
    current = "track" if p.loop_track else "queue" if p.loop_queue else "off"
    await send(ctx, embed=panel("Loop Mode", f"Set to **{current}**.", BRAND_SOFT), ephemeral=True)


@bot.hybrid_command(name="shuffle", description="Shuffle the queue.")
async def shuffle_cmd(ctx: commands.Context):
    p = bot.player(ctx.guild)
    p.shuffle()
    await send(ctx, embed=panel("Shuffled", f"{len(p.queue)} tracks reordered.", BRAND_SOFT), ephemeral=True)


@bot.hybrid_command(name="volume", aliases=["vol"], description="Set volume 0–200%.")
async def volume_cmd(ctx: commands.Context, value: int):
    p = bot.player(ctx.guild)
    p.set_volume(value / 100)
    await send(ctx, embed=panel("Volume", f"Set to **{int(p.volume * 100)}%**.", BRAND_SOFT), ephemeral=True)


@bot.hybrid_command(name="stay", description="Toggle 24/7 stay-connected mode.")
async def stay_cmd(ctx: commands.Context):
    p = bot.player(ctx.guild)
    p.stay = not p.stay
    await send(ctx, embed=panel("24/7 Mode", f"Now **{'on' if p.stay else 'off'}**.", BRAND_SOFT), ephemeral=True)


@bot.hybrid_command(name="join", description="Summon SHE SINGS into your channel.")
async def join_cmd(ctx: commands.Context):
    ok, err = guard(ctx)
    if not ok:
        return await send(ctx, embed=err, ephemeral=True)
    p = bot.player(ctx.guild)
    p.channel = ctx.channel
    await p.connect(ctx.author.voice.channel)
    await send(ctx, embed=panel("Connected", f"Singing in **{ctx.author.voice.channel.name}**.", OK))


@bot.hybrid_command(name="help", description="Show the command list.")
async def help_cmd(ctx: commands.Context):
    e = panel(NAME, "A voice channel where the songs never stop.")
    e.add_field(name="Playback", value=(
        f"`{PREFIX}play <name | url | playlist>`\n"
        f"`{PREFIX}pause` · `{PREFIX}resume` · `{PREFIX}skip` · `{PREFIX}stop`"
    ), inline=False)
    e.add_field(name="Queue", value=(
        f"`{PREFIX}queue` · `{PREFIX}np` · `{PREFIX}shuffle`\n"
        f"`{PREFIX}loop [track|queue|off]` · `{PREFIX}volume <0-200>`"
    ), inline=False)
    e.add_field(name="Session", value=f"`{PREFIX}join` · `{PREFIX}stay`", inline=False)
    if bot.user:
        e.set_thumbnail(url=bot.user.display_avatar.url)
    e.set_footer(text=f"{NAME} · slash commands available")
    await send(ctx, embed=e, ephemeral=True)


@bot.event
async def on_command_error(ctx: commands.Context, error: Exception):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        return await send(ctx, embed=panel("Missing argument", f"Try `{PREFIX}help`.", WARN),
                          ephemeral=True)
    log.exception("command error: %r", error)
    await send(ctx, embed=panel("Something broke", f"`{error}`", ERR), ephemeral=True)


# ─── keep-alive (Render / Koyeb free web services) ───────────────────────────

def start_health_server() -> None:
    """Binds $PORT so a free web-service host accepts the deploy, and so a
    free uptime pinger can keep the container awake. No-op when PORT is unset."""
    port = os.getenv("PORT", "").strip()
    if not port:
        return

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = f"{NAME} alive".encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    try:
        srv = HTTPServer(("0.0.0.0", int(port)), Handler)
    except OSError as exc:
        log.warning("health server failed on port %s: %r", port, exc)
        return

    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("health server listening on 0.0.0.0:%s", port)


def _retry_wait(exc: discord.HTTPException, fallback: int) -> int:
    """Honour Retry-After when Discord sends one; otherwise use our backoff."""
    try:
        value = exc.response.headers.get("Retry-After")
        if value:
            return max(fallback, min(900, int(float(value))))
    except Exception:
        pass
    return fallback


async def run_with_backoff() -> None:
    """Discord rate-limits by IP. Shared hosts (Render, Railway, Fly) get 429s
    that clear in minutes. Hold the process alive and retry the login instead of
    exiting, so the container never crash-loops into a longer ban. The health
    server is already up, so the deploy reads Live while we wait."""
    delay = 30
    attempt = 0

    async with bot:
        while True:
            attempt += 1
            try:
                await bot.login(TOKEN)
            except discord.HTTPException as exc:
                if exc.status != 429:
                    raise
                wait = _retry_wait(exc, min(900, delay))
                log.warning("429 from Discord — attempt %d, holding %ds before retry",
                            attempt, wait)
                await asyncio.sleep(wait)
                delay = min(900, delay * 2)
                continue
            break

        await bot.connect(reconnect=True)


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("DISCORD_TOKEN missing — fill .env")
    start_health_server()
    asyncio.run(run_with_backoff())
