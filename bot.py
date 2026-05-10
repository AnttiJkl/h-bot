import asyncio
import logging
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import discord
import yt_dlp
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Discord intents
intents = discord.Intents.default()

# YTDL options
ytdl_format_options = {
    "format": "bestaudio/best",
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
    "extractor_retries": 3,
}

ffmpeg_options = {
    "before_options": (
        "-nostdin "
        "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 "
        "-reconnect_on_network_error 1 -reconnect_on_http_error 4xx,5xx "
        "-rw_timeout 15000000 "
        "-probesize 256k -analyzeduration 1M "
        "-thread_queue_size 1024"
    ),
    "options": (
        "-vn -sn -dn "
        "-b:a 96k "
        "-af volume=0.20,aresample=async=1:min_hard_comp=0.100:first_pts=0 "
        "-loglevel warning"
    ),
}

ytdl = yt_dlp.YoutubeDL(ytdl_format_options)
executor = ThreadPoolExecutor(max_workers=2)


class Song:
    def __init__(self, data):
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
        self.created_at = time.time()

    @classmethod
    async def create(cls, query, loop=None, retries=3):
        loop = loop or asyncio.get_running_loop()

        for attempt in range(retries):
            try:
                # We just extract metadata here, we'll get the real stream URL later
                data = await loop.run_in_executor(
                    executor, lambda: ytdl.extract_info(query, download=False)
                )
                break
            except Exception as e:
                logger.error(
                    f"Error extracting info (attempt {attempt + 1}/{retries}): {e}"
                )
                if attempt == retries - 1:
                    return None
                await asyncio.sleep(1)

        if data is None:
            logger.error("No data found for the query.")
            return None

        if "entries" in data:  # Prevent KeyError
            if not data["entries"]:  # Prevent IndexError
                logger.error("No entries found for the query.")
                return None
            data = data["entries"][0]

        return cls(data)

    async def get_audio_source(self):
        loop = asyncio.get_running_loop()
        try:
            # If the extraction was less than 45 minutes ago, the URL is likely still valid
            if time.time() - self.created_at < 45 * 60:
                filename = self.url
            else:
                # Re-extract just before playing to avoid URL expiration
                # If webpage_url is missing, fallback to the original URL
                target_url = self.webpage_url or self.url
                fresh_data = await loop.run_in_executor(
                    executor, lambda: ytdl.extract_info(target_url, download=False)
                )
                filename = fresh_data["url"]

            return discord.FFmpegOpusAudio(filename, **ffmpeg_options)
        except Exception as e:
            logger.error(f"Error getting fresh audio source: {e}")
            return None


class Player:
    def __init__(self, cog, guild, loop):
        self.cog = cog
        self.guild = guild
        self.loop = loop
        self.voice_client = None
        self.voice_lock = asyncio.Lock()
        self.queue = asyncio.Queue()
        self.queue_list = deque()
        self.current_song: Song | None = None
        self.current_audio: discord.AudioSource | None = None
        self.next_event = asyncio.Event()
        self.idle_counter = 0
        self.text_channel: discord.abc.Messageable | None = None
        # Start timers and player task
        self.dc_timer.start()
        self.player_task = loop.create_task(self.player_loop())

    @tasks.loop(seconds=10)
    async def dc_timer(self):
        if (
            self.voice_client
            and not self.voice_client.is_playing()
            and self.current_song is None
            and self.queue.empty()
        ):
            self.idle_counter += 10
            if self.idle_counter >= 600:
                logger.info(f"Disconnecting from {self.guild.name} due to inactivity.")
                await self.disconnect()
        else:
            self.idle_counter = 0

    async def player_loop(self):
        while True:
            try:
                self.current_song = await self.queue.get()

                try:
                    if self.queue_list and self.queue_list[0] is self.current_song:
                        self.queue_list.popleft()
                    else:
                        self.queue_list.remove(self.current_song)
                except ValueError:
                    pass

                self.next_event.clear()

                # Get fresh stream URL right before playing to prevent expiration
                self.current_audio = await self.current_song.get_audio_source()
                if not self.current_audio:
                    logger.error(
                        f"Failed to get audio source for {self.current_song.title}, skipping."
                    )
                    self.current_song = None
                    continue

                def _after_play(error):
                    if error:
                        logger.error(f"Player error: {error}")
                    self.loop.call_soon_threadsafe(self.next_event.set)

                self.voice_client.play(self.current_audio, after=_after_play)

                try:
                    if not getattr(self.current_song, "announced", False):
                        embed = discord.Embed(
                            title="Now Playing",
                            color=discord.Color.green(),
                            description=f"[{self.current_song.title}]({self.current_song.webpage_url})\n\nDuration: {self.current_song.duration}",
                        )
                        if self.current_song.thumbnail:
                            embed.set_thumbnail(url=self.current_song.thumbnail)

                        if getattr(self.current_song, "requester", None):
                            embed.set_footer(
                                text=f"Requested by {self.current_song.requester.display_name}",
                                icon_url=self.current_song.requester.display_avatar.url,
                            )

                        target_channel = self.text_channel or self.guild.system_channel
                        if target_channel is None:
                            me = self.guild.me
                            for ch in self.guild.text_channels:
                                if ch.permissions_for(me).send_messages:
                                    target_channel = ch
                                    break
                        if target_channel is not None:
                            await target_channel.send(embed=embed)
                except Exception as e:
                    logger.debug(f"Failed to send now playing embed: {e}")

                await self.next_event.wait()

                if self.current_audio:
                    try:
                        self.current_audio.cleanup()
                    except Exception:
                        pass

                self.current_song = None
                self.current_audio = None
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in player loop: {e}")
                if self.current_audio:
                    try:
                        self.current_audio.cleanup()
                    except Exception:
                        pass
                self.current_song = None
                self.current_audio = None
                await asyncio.sleep(0.5)

    async def disconnect(self):
        # We don't need voice_lock here because this is tear-down, but it's safe.
        if self.voice_client:
            await self.voice_client.disconnect()
            self.voice_client = None

        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except Exception:
                pass
        self.queue_list.clear()

        if self.current_audio:
            try:
                self.current_audio.cleanup()
            except Exception:
                pass

        self.idle_counter = 0
        self.dc_timer.cancel()
        self.player_task.cancel()

        if self.guild.id in self.cog.players:
            del self.cog.players[self.guild.id]

        logger.info(f"Disconnected from {self.guild.name} and cleaned up player.")

    async def play_song(self, interaction, query):
        await interaction.response.defer()
        self.text_channel = interaction.channel

        if not await self.ensure_voice(interaction):
            return

        # Fetch song metadata in the background
        song = await Song.create(query, loop=self.loop)
        if not song:
            await interaction.edit_original_response(
                content="Could not find the requested song."
            )
            return

        # Check if the player was disconnected while we were fetching metadata
        if not self.voice_client:
            return

        is_playing = self.voice_client.is_playing() or self.current_song is not None
        if not is_playing:
            song.announced = True

        song.requester = interaction.user

        self.queue_list.append(song)
        self.queue.put_nowait(song)

        if is_playing:
            embed = discord.Embed(
                title="Queued",
                color=discord.Color.blue(),
                description=f"[{song.title}]({song.webpage_url})\n\nDuration: {song.duration}",
            )
            if song.thumbnail:
                embed.set_thumbnail(url=song.thumbnail)
            embed.set_footer(
                text=f"Requested by {song.requester.display_name}",
                icon_url=song.requester.display_avatar.url,
            )
            await interaction.edit_original_response(embed=embed)
        else:
            embed = discord.Embed(
                title="Now Playing",
                color=discord.Color.green(),
                description=f"[{song.title}]({song.webpage_url})\n\nDuration: {song.duration}",
            )
            if song.thumbnail:
                embed.set_thumbnail(url=song.thumbnail)
            embed.set_footer(
                text=f"Requested by {song.requester.display_name}",
                icon_url=song.requester.display_avatar.url,
            )
            await interaction.edit_original_response(embed=embed)

    async def ensure_voice(self, interaction):
        async with self.voice_lock:
            if interaction.user.voice:
                if not self.voice_client or not self.voice_client.is_connected():
                    try:
                        self.voice_client = (
                            await interaction.user.voice.channel.connect()
                        )
                    except asyncio.TimeoutError:
                        await interaction.edit_original_response(
                            content="Failed to connect to the voice channel."
                        )
                        return False
                    except Exception as e:
                        logger.error(f"Error connecting to voice: {e}")
                        await interaction.edit_original_response(
                            content="An error occurred while connecting to the voice channel."
                        )
                        return False
                elif self.voice_client.channel != interaction.user.voice.channel:
                    await self.voice_client.move_to(interaction.user.voice.channel)
            else:
                await interaction.edit_original_response(
                    content="You must be in a voice channel to use this command."
                )
                return False
            return True

    async def show_queue(self, interaction):
        if not self.queue_list and not self.current_song:
            await interaction.response.send_message("The queue is currently empty.")
            return

        embed = discord.Embed(title="Queue", color=discord.Color.purple())
        description = ""

        if self.current_song:
            req = (
                f" • Requested by {self.current_song.requester.display_name}"
                if getattr(self.current_song, "requester", None)
                else ""
            )
            description += f"**__Now Playing__**\n[{self.current_song.title}]({self.current_song.webpage_url}) | `{self.current_song.duration}`{req}\n\n"

        if self.queue_list:
            description += "**__Up Next__**\n"
            for idx, song in enumerate(self.queue_list):
                req = (
                    f" • {song.requester.display_name}"
                    if getattr(song, "requester", None)
                    else ""
                )
                line = f"`{idx + 1}.` [{song.title}]({song.webpage_url}) | `{song.duration}`{req}\n"
                if len(description) + len(line) > 4000:
                    description += (
                        f"\n*... and {len(self.queue_list) - idx} more songs.*"
                    )
                    break
                description += line

        embed.description = description

        total_songs = len(self.queue_list)
        if total_songs > 0:
            embed.set_footer(
                text=f"{total_songs} song{'s' if total_songs > 1 else ''} in queue"
            )

        await interaction.response.send_message(embed=embed)


class MusicCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.players = {}

    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        # Ignore if the state update is not for our bot
        if member.id != self.bot.user.id:
            return

        # If the bot was disconnected from a channel
        if before.channel is not None and after.channel is None:
            player = self.players.get(member.guild.id)
            if player:
                logger.info(
                    f"Bot was externally disconnected from {member.guild.name}."
                )
                # Disconnect to cleanup queue, tasks, and FFmpeg processes
                await player.disconnect()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info("MusicCog is ready.")
        try:
            synced = await self.bot.tree.sync()
            logger.info(f"Synced {len(synced)} command(s)")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")

    @discord.app_commands.command(name="play", description="Play a song from YouTube")
    async def play(self, interaction: discord.Interaction, song: str):
        logger.info(
            f"Received play command for song: {song} in guild {interaction.guild.name} by user {interaction.user.name}"
        )
        player = self.players.get(interaction.guild.id)
        if not player:
            player = Player(self, interaction.guild, self.bot.loop)
            self.players[interaction.guild.id] = player
        await player.play_song(interaction, song)

    @discord.app_commands.command(
        name="skip", description="Skip the currently playing song"
    )
    async def skip(self, interaction: discord.Interaction):
        logger.info(
            f"Received skip command in guild {interaction.guild.name} by user {interaction.user.name}."
        )
        player = self.players.get(interaction.guild.id)
        if player and player.voice_client and player.voice_client.is_playing():
            player.voice_client.stop()
            await interaction.response.send_message("Song skipped.")
        else:
            await interaction.response.send_message("Nothing is playing to skip.")

    @discord.app_commands.command(
        name="leave", description="Disconnect the bot from the voice channel"
    )
    async def leave(self, interaction: discord.Interaction):
        player = self.players.get(interaction.guild.id)
        if player:
            await player.disconnect()
            await interaction.response.send_message(
                "Disconnected from the voice channel."
            )
        else:
            await interaction.response.send_message(
                "The bot is not connected to any voice channel."
            )

    @discord.app_commands.command(name="queue", description="Show the current queue")
    async def queue(self, interaction: discord.Interaction):
        player = self.players.get(interaction.guild.id)
        if player:
            await player.show_queue(interaction)
        else:
            await interaction.response.send_message("The queue is currently empty.")


bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def setup_hook():
    await bot.add_cog(MusicCog(bot))
    logger.info("Music cog loaded.")


@bot.event
async def on_ready():
    logger.info(f"Bot is ready! Logged in as {bot.user}")


token = os.getenv("DISCORD_BOT_TOKEN")
if not token:
    raise ValueError(
        "Bot token not found. Set DISCORD_BOT_TOKEN as an environment variable."
    )

if __name__ == "__main__":
    logger.info("Starting bot...")
    bot.run(token)
