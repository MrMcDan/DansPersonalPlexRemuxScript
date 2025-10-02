# README.md - The Video Processing Monstrosity

*Using AI to build an absurdly comprehensive remux script that solves every problem I've ever encountered.*

## ⚠️ Warning: You Have Found The Nuclear Option

Congratulations! You've stumbled upon what happens when someone asks "what if we solved EVERY video processing problem at once?" and then actually follows through. This isn't just a script—it's a 6,000+ line monument to the question "but what if THIS edge case happens?"

## What Fresh Hell Is This?

This repository contains PowerShell scripts that have collectively seen things no script should see:

### Core Processing Scripts

1. **`convertHdr10+ToAV1.ps1`** (6,247 lines of "surely we don't need MORE error handling")
   - The main video processing workhorse
   - Handles encoding, HDR10+ processing, audio/subtitle manipulation
   - Direct hardware acceleration via Intel QuickSync

2. **`parent_process.ps1`** (The sane wrapper that knows when to give up)
   - Intelligent retry logic with quality adjustment
   - Validates encoding results
   - Gives up after 3 failed attempts (rare, but graceful)

### Diagnostic & Utility Scripts

3. **`Get-PlexVideoInfo.ps1`** (The Video Detective)
   - Comprehensive video file analyzer for diagnosing Plex playback issues
   - Checks codecs, HDR/Dolby Vision, container integrity, GOP structure
   - Identifies problematic audio/subtitle formats
   - Provides specific recommendations for fixing detected issues
   - **Usage:** `.\Get-PlexVideoInfo.ps1 -InputFile "movie.mkv" -OutputFile "report.txt"`
   - Perfect for troubleshooting "why won't this play?" scenarios

4. **`Get-PlexServerStatus.ps1`** (The Server Watcher)
   - Monitors Plex server status and activity
   - Checks if MCEBuddy or other processes are running
   - Useful for coordinating multiple automation tasks
   - **Usage:** `.\Get-PlexServerStatus.ps1`

### Features (Yes, ALL of Them)

**The Kitchen Sink Approach:**
- Intel QuickSync hardware acceleration (because speed matters)
- Automatic software fallback (because hardware fails)
- HDR10+ detection across THREE different methods (because one is never enough)
- HDR10+ metadata cleaning with Python/Pandas (because corrupted metadata is apparently a thing)
- Dolby Vision removal that checks file sizes (learned that lesson the hard way)
- Plex-optimized everything (keyframes, audio tracks, subtitle dispositions)
- Quality validation with adaptive thresholds (trust, but verify)
- Asynchronous backup (paranoia is a feature)
- MCEBuddy integration that literally pauses FFmpeg (yes, really)
- Space monitoring (because running out mid-encode is traumatic)
- Multi-instance detection via temporary marker files (because I ran two at once... once)

**Audio Track Gymnastics:**
- TrueHD gets split into AAC primary + TrueHD secondary (Plex clients are picky)
- DTS converts to EAC3 (compatibility over purity)
- FLAC/PCM converts to AAC (file sizes, people)
- Automatic compatibility track creation (works on everything™)
- Duration validation (learned about async issues the hard way)

**Subtitle Madness:**
- Forced subtitle detection across 4 different methods (because manufacturers can't agree)
- External .srt file scanning (they're hiding in plain sight)
- PGS extraction to .sup (image subtitles are pain)
- Proper disposition flags (Plex WILL show wrong subtitles otherwise)
- Title preservation with quality indicators (SDH/CC/Forced markers)

**Error Handling That Won't Quit:**
- EBML corruption detection (Matroska containers are fragile)
- NAL unit validation (H.265 encoding errors are subtle)
- Container-level corruption checks (exit code 0 means nothing)
- Timeout handling (FFmpeg can hang forever)
- File handle cleanup (Windows is clingy)
- Emergency Ctrl+C handlers (because you WILL panic-stop it)

## Prerequisites (Buckle Up)

### Required Software (No, You Can't Skip Any)

**1. PowerShell 6.0+** (PowerShell 7+ recommended for fewer weird bugs)
```powershell
$PSVersionTable.PSVersion  # If this returns 5.1, you're in for a bad time
```

**2. Python 3.8+** (Yes, a PowerShell script calls Python. It's faster. Don't judge.)
```bash
pip install pandas numpy
```
*Why? Because processing 10,000 HDR10+ scene metadata entries in pure PowerShell takes approximately forever.*

**3. FFmpeg with QSV support** (Not just any FFmpeg, THE RIGHT ONE)
- Download from: https://www.gyan.dev/ffmpeg/builds/
- Must include `hevc_qsv` encoder
- Test it: `ffmpeg -encoders | findstr hevc_qsv`
- If empty output: start over, wrong build

**4. MKVToolNix** (Because FFmpeg's MKV handling is... quirky)
- Download: https://mkvtoolnix.download/
- Install to default location or face configuration hell
- We use mkvmerge for everything FFmpeg screws up

### Optional Tools (For the Truly Ambitious)

- **HDR10+ Tool** - If you care about that sweet, sweet dynamic metadata
- **dovi_tool** - When Dolby Vision absolutely must die
- **MCEBuddy CLI** - For the "I schedule everything" crowd
  - **Note**: Full MCEBuddy integration config files are included (see MCEBuddy Integration section)

### Hardware Requirements (Don't Cheap Out Here)

- **CPU**: Intel 6th gen+ with working iGPU (QSV needs this)
- **RAM**: 8GB minimum, 16GB if you value your sanity (4K eats RAM)
- **Storage**: 4x input file size free (yes, really, learned this lesson)
- **GPU**: Intel graphics drivers (update them, seriously, QSV breaks subtly)

## Installation (A Journey)

### Step 1: Accept Your Fate

Download both scripts. Place them somewhere you'll remember. `E:\Plex\` is suggested because by the time you need scripts like these, you have a dedicated media drive.

### Step 2: The Configuration Ritual

Edit `convertHdr10+ToAV1.ps1` and locate the `$Config` hashtable around line 150. This is where dreams go to be path-validated:

```powershell
$Config = @{
    # Optional but recommended (HDR10+ is worth it)
    HDR10PlusToolExe      = "E:\plex\hdr10plus_tool.exe"
    
    # REQUIRED - The script will have an existential crisis without these
    FFmpegExe             = "E:\plex\ffmpeg\bin\ffmpeg.exe"
    FFProbeExe            = "E:\plex\ffmpeg\bin\ffprobe.exe"
    MKVMergeExe           = "C:\Program Files\MKVToolNix\mkvmerge.exe"
    pythonExe             = "python.exe"  # Must be in PATH or provide full path
    
    # Optional chaos enablers
    DolbyVisionRemoverCmd = "E:\Plex\DDVT_REMOVER.cmd"
    mcebuddyCLI           = "C:\Program Files\MCEBuddy2x\MCEBuddy.UserCLI.exe"
}
```

**Pro tip**: Test each path. Copy-paste into PowerShell. Use `Test-Path "your\path\here"`. Save yourself the 2 hours of "why isn't it working?"

### Step 3: Validate Your Python Setup

```powershell
python -c "import pandas, numpy; print('Dependencies OK')"
```

If this errors, go install the packages. If it says "python not found," add Python to PATH. If you don't know how to add things to PATH... maybe start with simpler scripts.

### Step 4: The QSV Verification Dance

```powershell
& "E:\plex\ffmpeg\bin\ffmpeg.exe" -encoders | Select-String "hevc_qsv"
```

Expected output: Something with "hevc_qsv" in it.

If you see nothing: You downloaded the wrong FFmpeg build. Go back. Get the full build with --enable-libvpl or equivalent. QSV is the whole point of this script's speed.

## Diagnostic Tools Setup (Optional but Recommended)

The diagnostic scripts (`Get-PlexVideoInfo.ps1` and `Get-PlexServerStatus.ps1`) only require FFmpeg - no additional setup needed beyond Step 2 above.

**Quick Start for Diagnostics:**
```powershell
# Check if a file has Plex compatibility issues
.\Get-PlexVideoInfo.ps1 -InputFile "C:\Movies\Problem_Movie.mkv"

# Save detailed report to file
.\Get-PlexVideoInfo.ps1 -InputFile "C:\Movies\Problem_Movie.mkv" -OutputFile "C:\report.txt"

# Check Plex server and processing status
.\Get-PlexServerStatus.ps1
```

The diagnostic tools will tell you exactly what's wrong with a file and recommend either:
- Simple fixes (remux with FFmpeg)
- Running through the full conversion script for complex issues (Dolby Vision, HDR10+, problematic audio/subtitles)

## MCEBuddy Integration (The "Set It and Forget It" Approach)

If you want fully automated video processing triggered by file monitoring, MCEBuddy can execute these scripts automatically. Two configuration files are included for complete integration:

### Configuration Files

**1. `mcebuddy.conf` - Monitor Task Configuration**

This defines what MCEBuddy monitors and how it processes files. Key settings:

```ini
[Process Movies]
Profile=Move Movie Only
DestinationPath=z:\media\Movies
MonitorTaskNames=Movies
SkipRemux=True
CustomRenameBySeries=%showname% %premiereyear% {imdb-%imdbmovieid%}
ForceShowType=Movie
StrictProcessing=True
```

**What this does:**
- Monitors your source folder (configured in MCEBuddy's monitor settings)
- Uses the "Move Movie Only" profile (see below)
- Skips MCEBuddy's built-in remux (our script handles this better)
- Renames files with metadata: "Movie Name (Year) {imdb-tt1234567}"
- Strict processing prevents partial failures from being moved to destination

**2. `profile.conf` - Custom Profile Definition**

This is where the magic happens. The profile tells MCEBuddy to call our scripts:

```ini
[Move Movie Only]
Description=Use this profile to copy original tracks and trigger the PowerShell conversion scripts
order=ffmpeg,copy
copy-remuxto=.mkv
ffmpeg-video=-ss 0 -vcodec copy -map 0:v -sn
ffmpeg-audio=-acodec copy -map 0:a
PreConversionCommercialRemover=true
CustomCommandPath="C:\Program Files\PowerShell\7\pwsh.exe"
CustomCommandParameters= -File "E:\plex\parent_process.ps1" -InputFile "%convertedfile%" -OrigFile "%sourcefile%" -EnableQualityValidation "1"
CustomCommandHangPeriod=0
CustomCommandCritical=true
CustomCommandExitCodeCheck=true
```

**Critical settings explained:**
- **`CustomCommandPath`**: Points to PowerShell 7 (not Windows PowerShell 5.1!)
- **`CustomCommandParameters`**: Calls parent_process.ps1 with proper parameters
  - `%convertedfile%` = MCEBuddy's processed file (after commercial removal if applicable)
  - `%sourcefile%` = Original source file (for Dolby Vision removal)
- **`CustomCommandCritical=true`**: If script fails, MCEBuddy marks the job as failed
- **`CustomCommandExitCodeCheck=true`**: Respects script exit codes (enables retry logic)
- **`CustomCommandHangPeriod=0`**: No timeout (our script has its own timeout handling)

### Installation Steps

**Step 1: Locate MCEBuddy Config Files**

MCEBuddy stores configs in: `C:\ProgramData\MCEBuddy2x\`

You'll find:
- `mcebuddy.conf` - Main configuration
- `profiles\` folder - Contains all profile definitions

**Step 2: Backup Existing Configs**

```powershell
Copy-Item "C:\ProgramData\MCEBuddy2x\mcebuddy.conf" "C:\ProgramData\MCEBuddy2x\mcebuddy.conf.backup"
Copy-Item "C:\ProgramData\MCEBuddy2x\profiles\*" "C:\ProgramData\MCEBuddy2x\profiles\backup\" -Recurse
```

**Step 3: Add/Merge Configuration**

**Option A - Fresh Installation:**
1. Copy included `mcebuddy.conf` to `C:\ProgramData\MCEBuddy2x\`
2. Copy included `profile.conf` to `C:\ProgramData\MCEBuddy2x\profiles\`
3. Edit paths in both files to match your setup

**Option B - Existing MCEBuddy Setup:**
1. Open `C:\ProgramData\MCEBuddy2x\mcebuddy.conf`
2. Add the `[Process Movies]` section from the included file
3. Adjust paths and monitor task names to match your setup
4. Open/create `C:\ProgramData\MCEBuddy2x\profiles\custom.conf`
5. Add the `[Move Movie Only]` profile from included file
6. Update the `CustomCommandParameters` path to your scripts

**Step 4: Update Paths**

Edit both files and update these paths for your system:

In `mcebuddy.conf`:
```ini
DestinationPath=z:\media\Movies  # Your final movie destination
WorkingPath=                      # Leave empty to use MCEBuddy's working folder
```

In `profile.conf`:
```ini
CustomCommandPath="C:\Program Files\PowerShell\7\pwsh.exe"  # Your PowerShell 7 path
CustomCommandParameters= -File "E:\plex\parent_process.ps1" -InputFile "%convertedfile%" -OrigFile "%sourcefile%" -EnableQualityValidation "1"
# Update E:\plex\parent_process.ps1 to your script location
```

**Step 5: Restart MCEBuddy Service**

```powershell
Restart-Service MCEBuddy2x
```

Or use MCEBuddy GUI: Settings → Stop Engine → Start Engine

### How It Works (The Full Pipeline)

1. **File Detection**: MCEBuddy monitors source folder, detects new video file
2. **Commercial Removal** (optional): If enabled, removes commercials first
3. **Basic Remux**: MCEBuddy copies streams to MKV (fast, no encoding)
4. **Script Trigger**: MCEBuddy calls `parent_process.ps1` with the remuxed file
5. **Our Magic**: Scripts perform full HDR10+ conversion, audio/subtitle processing
6. **Quality Validation**: Parent script validates output, retries if needed
7. **File Replacement**: Script replaces MCEBuddy's remuxed file with final encode
8. **Final Move**: MCEBuddy moves the processed file to destination folder
9. **Cleanup**: Both MCEBuddy and our scripts clean up temp files

### Coordination Features

The scripts detect when MCEBuddy is running and coordinate automatically:

- **Encoding Pause**: When MCEBuddy starts another job, our script pauses FFmpeg
- **Resume Detection**: Script automatically resumes when MCEBuddy is idle
- **Resource Sharing**: Prevents two heavy encodes from running simultaneously
- **Status Messages**: Script shows "PAUSED - MCEBuddy active" in yellow during pauses

**Check status in logs:**
```
[12:34:56] Encoding progress: 45.2% complete (125.3 fps)
[12:35:12] ⚠ PAUSED - MCEBuddy active, waiting to resume...
[12:36:45] Resuming encoding...
```

### Monitoring and Logs

**MCEBuddy Logs**: `C:\ProgramData\MCEBuddy2x\log\`
- Shows file detection, processing stages, script execution
- Look for "CustomCommand" entries to see script invocations

**Script Logs**: Written to console (captured by MCEBuddy)
- Detailed encoding progress and validation results
- Check MCEBuddy's log files to see script output

### Common Integration Issues

**"CustomCommand failed with exit code 1"**

**Solution**: 
1. Check MCEBuddy log for actual error
2. Verify PowerShell 7 path is correct
3. Run script manually to test: 
   ```powershell
   & "E:\plex\parent_process.ps1" -InputFile "test.mkv" -OrigFile "test.mkv"
   ```

**"Script appears to hang indefinitely"**

**Problem**: `CustomCommandHangPeriod` is set too low

**Solution**: Set to 0 in profile.conf (disables timeout, script has its own)

**MCEBuddy moves file before script finishes**

**Problem**: `CustomCommandCritical=false` or script is backgrounding

**Solution**: Ensure `CustomCommandCritical=true` in profile.conf

**Multiple encoding instances conflict**

**Problem**: MCEBuddy started another job while script is running

**Solution**: This is normal! Script automatically pauses and resumes. Check for yellow "PAUSED" messages.

### Performance Notes

With MCEBuddy integration:
- Commercial removal adds 5-15 minutes (if enabled)
- File moves are asynchronous (non-blocking)
- Total pipeline for 1080p HDR movie: 30-60 minutes
- Total pipeline for 4K HDR10+ movie: 1-3 hours

**Pro tip**: Set MCEBuddy's working folder to an SSD for faster commercial detection. The final encode will use the script's temp folder (configured separately).

## Usage (Simple on the Surface)

### Diagnostic Mode (Start Here If You Have Problems)

Before processing files, diagnose what's wrong:

```powershell
# Full diagnostic report with recommendations
.\Get-PlexVideoInfo.ps1 -InputFile "E:\Movies\Problem_Movie.mkv"

# Save report to file for reference
.\Get-PlexVideoInfo.ps1 -InputFile "E:\Movies\Problem_Movie.mkv" -OutputFile "E:\report.txt"
```

**What it checks:**
- Container format issues (AVI, WMV, corruption)
- Video codec compatibility (Dolby Vision, VC-1, interlacing)
- HDR/HDR10+ detection and validation
- Audio codec issues (TrueHD, DTS, PCM)
- Subtitle format problems (PGS, forced flags)
- GOP structure and frame analysis
- Container integrity errors

**Output includes:** Specific recommendations for each issue found, including whether to use the full conversion script or just a simple remux.

### Basic Usage (The Wrapper Does Everything)

```powershell
.\parent_process.ps1 -InputFile "E:\Movies\BigMovie.mkv" -OrigFile "E:\Movies\BigMovie.mkv"
```

The parent script will:
1. Run the main script
2. Check quality validation results
3. Retry up to 3 times with adjusted settings if needed
4. Give up gracefully if nothing works (rare, but possible)

### Advanced Usage (When You Need Control)

```powershell
.\convertHdr10+ToAV1.ps1 `
    -InputFile "E:\Movies\BigMovie.mkv" `
    -OrigFile "E:\Movies\BigMovie.mkv" `
    -OutputFolder "E:\Processed" `
    -TempFolder "F:\Temp" `
    -BackupFolder "D:\Originals" `
    -EnableQualityValidation 1 `
    -QualityThreshold 35.0 `
    -EnableSoftwareFallback
```

### Key Parameters Explained

- **`-InputFile`** / **`-OrigFile`**: Yes, you need both. Long story involving Dolby Vision removal.
- **`-BackupFolder`**: Where originals go. Script checks if backup already exists (learned from accidents).
- **`-EnableQualityValidation`**: 
  - `0` = YOLO mode (no validation)
  - `1` = Strict validation (may retry with lower quality)
  - `-1` = Validation metrics only (always accepts)
- **`-QualityThreshold`**: PSNR threshold in dB (35 = decent, 40 = paranoid)
- **`-EnableSoftwareFallback`**: Allows libx265 if QSV fails (slower but works)

## What Actually Happens (The Journey)

When you run this behemoth, buckle up for:

1. **Temp Folder Validation** (Checks 4x file size free space because we learned)
2. **Video Analysis** (Comprehensive HDR10+ detection across 3 methods)
3. **Dolby Vision Removal** (If present, with file size validation)
4. **Stream Analysis** (Audio/subtitle selection with deep validation)
5. **Subtitle Extraction** (Including external .srt scanning)
6. **Video Encoding** (QSV with fallback, MCEBuddy pause/resume)
7. **HDR10+ Injection** (If applicable, with metadata cleaning)
8. **MKV Containerization** (mkvmerge does this better than FFmpeg)
9. **Final Remux** (Combines everything with proper timing)
10. **Quality Validation** (PSNR/SSIM with source-aware thresholds)
11. **File Replacement** (Only if validation passes)
12. **Backup Verification** (Async backup gets final check)
13. **Cleanup** (Aggressive temp file removal with handle release)

### Progress Monitoring

The script outputs detailed progress including:
- Current encoding speed (with MCEBuddy pause detection)
- Frame counts and bitrates
- HDR10+ scene processing status
- Audio stream configuration details
- Subtitle disposition mapping
- Disk space warnings

## Common Issues (And Why They Happen)

### "How do I know if I need to process this file?"

**Solution**: Run the diagnostic first!
```powershell
.\Get-PlexVideoInfo.ps1 -InputFile "your_movie.mkv"
```

The diagnostic will tell you:
- If the file has Dolby Vision (major issue)
- If audio codecs are incompatible (TrueHD, DTS)
- If the container is corrupted
- If subtitles have wrong flags
- Specific recommendations for each issue

### "QSV initialization failed"

**Problem**: Intel drivers are outdated or iGPU is disabled in BIOS

**Solution**: 
1. Update Intel graphics drivers
2. Check BIOS - enable integrated graphics
3. Run as fallback: add `-EnableSoftwareFallback` parameter

### "HDR10+ metadata too damaged"

**Problem**: Source file has corrupted dynamic metadata (yes, this happens)

**Solution**: Script detects this and skips HDR10+ processing automatically. You get standard HDR10.

### "Duration mismatch detected"

**Problem**: Container metadata lies about file length (looking at you, badly mastered discs)

**Solution**: Script auto-corrects this. You'll see warnings but it handles it.

### "Quality validation failed"

**Problem**: Encoding quality below threshold (usually means source is already low quality)

**Solution**: Parent script auto-retries with lower quality setting. After 3 attempts, forces completion.

### Script takes FOREVER

**Possible causes**:
1. **MCEBuddy is running**: Script pauses encoding automatically (check yellow "PAUSED" messages)
2. **Software fallback activated**: libx265 is slow (40-60 fps vs 300+ fps for QSV)
3. **4K HDR10+ content**: Just... grab coffee. Lots of coffee.
4. **Quality validation enabled**: PSNR/SSIM calculation is CPU-intensive

### "No audio stream selected"

**Problem**: All audio streams failed validation (corruption, wrong language, duration mismatch)

**Solution**: 
1. Run diagnostic: `.\Get-PlexVideoInfo.ps1 -InputFile "file.mkv"`
2. Check audio stream details in the report
3. Verify with MediaInfo for additional details
4. Script is picky for good reasons - corrupted audio will cause playback issues

### "File won't play in Plex but passes all tests"

**Solution**: 
1. Run full diagnostic to identify subtle issues
2. Check Plex server logs for specific errors
3. Test playback on different clients (issue may be client-specific)
4. Consider running through conversion script even if diagnostic shows minor issues

## Workflow Recommendations

### New to Video Processing?
1. **Start with diagnostics** - Run `Get-PlexVideoInfo.ps1` on problematic files
2. **Understand the issues** - Read the report's recommendations carefully
3. **Simple fixes first** - If report suggests remux, try that before full conversion
4. **Full conversion** - Use main scripts for complex issues (Dolby Vision, HDR10+, audio problems)

### Batch Processing Setup?
1. **Test one file first** - Ensure configuration is correct
2. **Check diagnostic on sample** - Understand what issues exist in your library
3. **Set up MCEBuddy** - For automated processing of new content
4. **Monitor initial runs** - Watch logs for any unexpected issues

### Troubleshooting Workflow?
1. **Run diagnostic** - Get comprehensive report: `.\Get-PlexVideoInfo.ps1 -InputFile "file.mkv" -OutputFile "report.txt"`
2. **Review recommendations** - Report tells you exactly what's wrong
3. **Apply fixes** - Follow script recommendations (remux vs full conversion)
4. **Re-test** - Run diagnostic again on output file to confirm fixes
5. **Test in Plex** - Verify playback on your specific client setup

## Script Selection Guide

**Use `Get-PlexVideoInfo.ps1` when:**
- File won't play in Plex (diagnostic mode)
- Want to check file before processing
- Need detailed technical report
- Troubleshooting specific playback issues
- Checking batch of files for problems

**Use `Get-PlexServerStatus.ps1` when:**
- Need to check if Plex server is active
- Checking if MCEBuddy is currently processing
- Coordinating multiple automation tasks
- Monitoring server activity before starting encodes

**Use `parent_process.ps1` / `convertHdr10+ToAV1.ps1` when:**
- Diagnostic identified issues needing conversion
- Dolby Vision needs removal
- HDR10+ processing required
- Audio/subtitle format conversion needed
- Want Plex-optimized output with quality validation
- Automated processing (via MCEBuddy)

**Use simple FFmpeg remux when:**
- Diagnostic shows only minor container issues
- No codec/audio/subtitle problems
- Just need quick container fix
- Command: `ffmpeg -i input.mkv -c copy -map 0 output.mkv`

### "No audio stream selected"

**Problem**: All audio streams failed validation (corruption, wrong language, duration mismatch)

**Solution**: Check source file with MediaInfo. Script is picky for good reasons.

## Performance Expectations

| Resolution | QSV Encoding | Software Fallback | Quality Validation |
|-----------|-------------|-------------------|-------------------|
| 1080p SDR | ~300-400 fps | ~40-60 fps | +5-10 min |
| 1080p HDR | ~250-350 fps | ~35-50 fps | +5-10 min |
| 4K SDR | ~100-150 fps | ~15-25 fps | +15-20 min |
| 4K HDR10+ | ~80-120 fps | ~12-20 fps | +20-30 min |

*These are rough estimates. Your mileage will vary based on CPU, content complexity, and how many Chrome tabs you have open.*

## Exit Codes (For Automation)

- **0**: Success (file processed and validated)
- **1**: General failure (check logs)
- **0xF0F0 + quality**: Quality validation failed, retry suggested
  - Example: 61700 (0xF0F0 + 20) = "retry with quality 20"
- **130**: Ctrl+C pressed (emergency cleanup performed)

The parent script interprets these automatically.

## Why Does This Exist?

Because video processing is a rabbit hole of edge cases:

- Dolby Vision removal corrupts files (now validated)
- HDR10+ metadata can be garbage (now cleaned)
- TrueHD audio breaks Plex (now dual-tracked)
- Forced subtitles get ignored (now prioritized)
- FFmpeg hangs forever (now has timeouts)
- MCEBuddy conflicts (now coordinated)
- Container metadata lies (now corrected)
- Quality validation is unreliable (now adaptive)
- Temp folders fill up (now monitored)
- Multiple instances conflict (now detected)

And after hitting every single one of these issues, this script was born.

## Contributing

Found a new edge case? Congratulations, you've discovered something even this script doesn't handle. Open an issue with:
1. Full error message
2. MediaInfo output of source file
3. Script log output
4. Your sanity level (1-10)

## License

MIT License - Because this level of overengineering should be shared with the world.

## Final Thoughts

This script is the culmination of every "well, that shouldn't have happened" moment in video processing. It's excessive. It's overkill. It probably checks things that will never break.

But when you have a 80GB 4K HDR10+ Dolby Vision file that absolutely must be processed correctly on the first try... you'll understand.

**Godspeed, and may your encodes be swift and your metadata incorrupt.**

---

*P.S. - If you actually read this entire README, you're exactly the kind of person who needs this script.*
