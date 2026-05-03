import yt_dlp

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
    "socket_timeout": 30
    # "cookiesfrombrowser": ('firefox', None, None, None)
}

print(f"yt-dlp version: {yt_dlp.version.__version__}")
ytdl = yt_dlp.YoutubeDL(ytdl_format_options)

try:
    info = ytdl.extract_info("https://www.youtube.com/watch?v=fazMSCZg-mw", download=False)
    print("Success! Title:", info.get('title'))
except Exception as e:
    print("Error:", e)
