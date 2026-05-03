import asyncio
import os
import discord
import yt_dlp
from discord.ext import commands, tasks
import logging
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from collections import deque

# Load environment variables
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Ensure a download directory exists
DOWNLOAD_DIR = "./downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Discord intents
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

# YTDL options
ytdl_format_options = {
    "format": "bestaudio/best",
    "outtmpl": os.path.join(DOWNLOAD_DIR, "%(title)s.%(ext)s"),
    "restrictfilenames": True,
    "noplaylist": True,
    "nocheckcertificate": True,
    "ignoreerrors": False,
    "logtostderr": False,
    "quiet": True,
    "no_warnings": True,
    "default_search": "auto",
    "source_address": "0.0.0.0",
    "socket_timeout": 30,
    "retries": 3,
    "fragment_retries": 3,
    "extractor_retries": 3
    # "cookiesfrombrowser": ('firefox', None, None, None)
}

ffmpeg_options = {
    'before_options': (
        '-nostdin '
        '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 '
        '-reconnect_on_network_error 1 -reconnect_on_http_error 4xx,5xx '
        '-rw_timeout 15000000 '
        '-probesize 256k -analyzeduration 1M '
        '-thread_queue_size 1024'
    ),

    'options': (
        '-vn -sn -dn '
        '-b:a 96k '
        '-af volume=0.20,aresample=async=1:min_hard_comp=0.100:first_pts=0 '
        '-loglevel warning'
    )
}

ytdl = yt_dlp.YoutubeDL(ytdl_format_options)
# Keep ytdl work off the event loop but limit concurrency to avoid CPU spikes that can cause audio hiccups
executor = ThreadPoolExecutor(max_workers=2)

class YTDLSource(discord.AudioSource):
    def __init__(self, audio_source: discord.AudioSource, *, data):
        # Wrap an Opus AudioSource and expose metadata for embeds
        self._audio = audio_source
        self.data = data
        self.title = data.get("title")
        self.url = data.get("url")
        self.webpage_url = data.get("webpage_url")
        self.thumbnail = data.get("thumbnail")
        duration = data.get("duration_string")
        if not duration and data.get("duration"):
            m, s = divmod(data.get("duration"), 60)
            h, m = divmod(m, 60)
            duration = f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"
        self.duration = duration or "Unknown"

    def read(self):
        return self._audio.read()

    def is_opus(self):
        # We're using FFmpegOpusAudio under the hood
        return True

    def cleanup(self):
        try:
            self._audio.cleanup()
        except Exception:
            pass

    @classmethod
    async def create_source(cls, query, *, loop=None, stream=True, retries=3):
        loop = loop or asyncio.get_running_loop()
        
        for attempt in range(retries):
            try:
                data = await loop.run_in_executor(executor, lambda: ytdl.extract_info(query, download=not stream))
                break
            except Exception as e:
                logger.error(f"Error extracting info (attempt {attempt + 1}/{retries}): {e}")
                if attempt == retries - 1:
                    return None
                await asyncio.sleep(1)

        if data is None:
            logger.error("No data found for the query.")
            return None

        if "entries" in data:
            data = data["entries"][0]

        try:
            if not stream:
                # Local file playback via Opus
                filename = os.path.abspath(ytdl.prepare_filename(data))
                if not os.path.exists(filename):
                    logger.error(f"Downloaded file not found: {filename}")
                    return None
                audio = discord.FFmpegOpusAudio(filename, **ffmpeg_options)
            else:
                # Streamed playback via Opus
                filename = data["url"]
                audio = discord.FFmpegOpusAudio(filename, **ffmpeg_options)
        except Exception as e:
            logger.error(f"Error creating FFmpeg Opus source: {e}")
            return None

        return cls(audio, data=data)


class Player:
    def __init__(self, cog, guild, loop):
        self.cog = cog
        self.guild = guild
        self.loop = loop
        self.voice_client = None
        self.queue = asyncio.Queue()
        self.queue_list = deque()
        self.current: YTDLSource | None = None
        self.next_event = asyncio.Event()
        self.idle_counter = 0
        self.text_channel: discord.abc.Messageable | None = None
        # Start timers and player task
        self.dc_timer.start()
        self.player_task = loop.create_task(self.player_loop())

    @tasks.loop(seconds=10)
    async def dc_timer(self):
        # Disconnect if idle (not playing, no current track, and queue empty) for 10 minutes
        if self.voice_client and not self.voice_client.is_playing() and self.current is None and self.queue.empty():
            self.idle_counter += 10
            if self.idle_counter >= 600:
                logger.info(f"Disconnecting from {self.guild.name} due to inactivity.")
                await self.disconnect()
        else:
            self.idle_counter = 0

    async def player_loop(self):
        while True:
            try:
                # Wait for the next track
                self.current = await self.queue.get()
                # Keep display queue in sync
                try:
                    if self.queue_list and self.queue_list[0] is self.current:
                        self.queue_list.popleft()
                    else:
                        # Fallback: remove matching object if order desynced
                        self.queue_list.remove(self.current)
                except ValueError:
                    pass
                # Clear event for new track
                self.next_event.clear()

                def _after_play(error):
                    if error:
                        logger.error(f"Player error: {error}")
                    self.loop.call_soon_threadsafe(self.next_event.set)

                # Start playback
                self.voice_client.play(self.current, after=_after_play)

                # Announce now playing in the bound channel, or a sensible fallback
                try:
                    embed = discord.Embed(
                        title="Now Playing",
                        color=discord.Color.green(),
                        description=f"[{self.current.title}]({self.current.webpage_url})\n\nDuration: {self.current.duration}"
                    )
                    if self.current.thumbnail:
                        embed.set_thumbnail(url=self.current.thumbnail)

                    target_channel = self.text_channel
                    if target_channel is None:
                        # Fallback: system channel or first channel with send permission
                        target_channel = self.guild.system_channel
                        if target_channel is None:
                            me = self.guild.me
                            for ch in self.guild.text_channels:
                                perms = ch.permissions_for(me)
                                if perms.send_messages:
                                    target_channel = ch
                                    break
                    if target_channel is not None:
                        await target_channel.send(embed=embed)
                except Exception as e:
                    logger.debug(f"Failed to send now playing embed: {e}")

                # Wait until track finishes
                await self.next_event.wait()
                self.current = None
                # Small delay to allow FFmpeg to cleanup socket
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in player loop: {e}")
                await asyncio.sleep(0.5)

    async def disconnect(self):
        if self.voice_client:
            await self.voice_client.disconnect()
            self.voice_client = None

        # Drain queue and cleanup current
        while not self.queue.empty():
            try:
                src = self.queue.get_nowait()
                if hasattr(src, 'cleanup'):
                    src.cleanup()
            except Exception:
                pass
        self.queue_list.clear()

        if self.current and hasattr(self.current, 'cleanup'):
            try:
                self.current.cleanup()
            except Exception:
                pass

        self.idle_counter = 0
        self.dc_timer.cancel()
        self.player_task.cancel()

        if self.guild.id in self.cog.players:
            del self.cog.players[self.guild.id]

        logger.info(f"Disconnected from {self.guild.name} and cleaned up player.")

    async def play_song(self, interaction, query):
        # Defer the response since song fetching might take time
        await interaction.response.defer()
        
        # Bind player announcements to the invoking channel
        self.text_channel = interaction.channel
        
        if not await self.ensure_voice(interaction):
            return

        # Fetch song metadata in the background
        source = await YTDLSource.create_source(query, loop=self.loop, stream=True)
        if not source:
            await interaction.edit_original_response(content="Could not find the requested song.")
            return

        await self.queue.put(source)
        self.queue_list.append(source)

        if self.voice_client.is_playing() or self.current is not None:
            embed = discord.Embed(
                title="Queued",
                color=discord.Color.blue(),
                description=f"[{source.title}]({source.webpage_url})\n\nDuration: {source.duration}"
            )
            if source.thumbnail:
                embed.set_thumbnail(url=source.thumbnail)
            await interaction.edit_original_response(embed=embed)
        else:
            await interaction.edit_original_response(content="Starting playback...")

    async def ensure_voice(self, interaction):
        if interaction.user.voice:
            if not self.voice_client:
                try:
                    self.voice_client = await interaction.user.voice.channel.connect()
                except asyncio.TimeoutError:
                    await interaction.edit_original_response(content="Failed to connect to the voice channel.")
                    return False
                except Exception as e:
                    logger.error(f"Error connecting to voice: {e}")
                    await interaction.edit_original_response(content="An error occurred while connecting to the voice channel.")
                    return False
            elif self.voice_client.channel != interaction.user.voice.channel:
                await self.voice_client.move_to(interaction.user.voice.channel)
        else:
            await interaction.edit_original_response(content="You must be in a voice channel to use this command.")
            return False
        return True

    async def show_queue(self, interaction):
        if not self.queue_list:
            await interaction.response.send_message("The queue is currently empty.")
            return

        description = ""
        for idx, song in enumerate(self.queue_list):
            line = f"{idx + 1}. [{song.title}]({song.webpage_url})\n"
            if len(description) + len(line) > 4000:
                description += f"... and {len(self.queue_list) - idx} more."
                break
            description += line

        embed = discord.Embed(
            title="Queue",
            color=discord.Color.purple(),
            description=description
        )
        await interaction.response.send_message(embed=embed)


class MusicCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.players = {}

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info("Bot is ready.")
        try:
            synced = await self.bot.tree.sync()
            logger.info(f"Synced {len(synced)} command(s)")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")

    @discord.app_commands.command(name="play", description="Play a song from YouTube")
    async def play(self, interaction: discord.Interaction, song: str):
        """Play a song from YouTube."""
        logger.info(f"Received play command for song: {song} in guild {interaction.guild.name} by user {interaction.user.name}")
        player = self.players.get(interaction.guild.id)
        if not player:
            player = Player(self, interaction.guild, self.bot.loop)
            self.players[interaction.guild.id] = player
        await player.play_song(interaction, song)

    @discord.app_commands.command(name="skip", description="Skip the currently playing song")
    async def skip(self, interaction: discord.Interaction):
        """Skip the currently playing song."""
        logger.info(f"Received skip command in guild {interaction.guild.name} by user {interaction.user.name}.")
        player = self.players.get(interaction.guild.id)
        if player and player.voice_client and player.voice_client.is_playing():
            player.voice_client.stop()
            await interaction.response.send_message("Song skipped.")
        else:
            await interaction.response.send_message("Nothing is playing to skip.")

    @discord.app_commands.command(name="leave", description="Disconnect the bot from the voice channel")
    async def leave(self, interaction: discord.Interaction):
        """Disconnect the bot from the voice channel."""
        player = self.players.get(interaction.guild.id)
        if player:
            await player.disconnect()
            await interaction.response.send_message("Disconnected from the voice channel.")
        else:
            await interaction.response.send_message("The bot is not connected to any voice channel.")

    @discord.app_commands.command(name="queue", description="Show the current queue")
    async def queue(self, interaction: discord.Interaction):
        """Show the current queue."""
        player = self.players.get(interaction.guild.id)
        if player:
            await player.show_queue(interaction)
        else:
            await interaction.response.send_message("The queue is currently empty.")


bot = commands.Bot(command_prefix='!', intents=intents)

@bot.event
async def setup_hook():
    await bot.add_cog(MusicCog(bot))
    logger.info("Music cog loaded.")

@bot.event
async def on_ready():
    logger.info(f"Bot is ready! Logged in as {bot.user}")

token = os.getenv("DISCORD_BOT_TOKEN")
if not token:
    raise ValueError("Bot token not found. Set DISCORD_BOT_TOKEN as an environment variable.")

logger.info("Starting bot...")
bot.run(token)