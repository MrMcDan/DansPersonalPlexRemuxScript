<#
.SYNOPSIS
    Comprehensive video file analysis tool for diagnosing Plex playback issues.

.DESCRIPTION
    Uses FFmpeg and FFprobe to extract detailed information about video files
    that commonly cause Plex playback problems. Outputs human-readable report
    highlighting potential compatibility issues.

.PARAMETER InputFile
    Path to the video file to analyze

.PARAMETER OutputFile
    Optional path to save the report. If not specified, outputs to console only.

.EXAMPLE
    .\Get-PlexVideoInfo.ps1 -InputFile "C:\Movies\Problem_Movie.mkv"
    
.EXAMPLE
    .\Get-PlexVideoInfo.ps1 -InputFile "C:\Movies\Problem_Movie.mkv" -OutputFile "C:\report.txt"

.NOTES
    SETUP INSTRUCTIONS:
    
    1. Download FFmpeg:
       - Visit: https://www.gyan.dev/ffmpeg/builds/
       - Download: "ffmpeg-release-full.7z" (or the latest full build)
       - Extract to a folder like: C:\ffmpeg or E:\tools\ffmpeg
    
    2. Add FFmpeg to PATH (OPTION A - Recommended):
       - Open System Properties > Environment Variables
       - Under "System variables", find "Path" and click Edit
       - Click "New" and add the path to FFmpeg's bin folder
         Example: C:\ffmpeg\bin
       - Click OK on all dialogs
       - Restart PowerShell/Terminal
       - Test by running: ffmpeg -version
    
    3. OR Configure Script Paths (OPTION B):
       - Edit this script (lines 35-36)
       - Set full paths to ffmpeg.exe and ffprobe.exe
       - Example:
         $ffprobePath = "C:\ffmpeg\bin\ffprobe.exe"
         $ffmpegPath = "C:\ffmpeg\bin\ffmpeg.exe"
    
    4. Run the script:
       - Open PowerShell 7 (recommended) or Windows PowerShell
       - Navigate to script location: cd C:\Scripts
       - Run: .\Get-PlexVideoInfo.ps1 -InputFile "C:\path\to\video.mkv"
    
    If you get "cannot be loaded because running scripts is disabled":
       - Run PowerShell as Administrator
       - Execute: Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
       - Answer Y (Yes) to the prompt
       - Close and reopen PowerShell

    Author: Created for Plex troubleshooting
    Requires: FFmpeg (ffmpeg.exe and ffprobe.exe)
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$InputFile,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputFile
)

# Configuration - Update these paths if needed
$ffprobePath = "E:\plex\ffmpeg-2025-06-02-git-688f3944ce-full_build\bin\ffprobe.exe"  # Assumes ffprobe is in PATH, otherwise provide full path
$ffmpegPath = "E:\plex\ffmpeg-2025-06-02-git-688f3944ce-full_build\bin\ffmpeg.exe"    # Assumes ffmpeg is in PATH, otherwise provide full path

# Color coding for console output
function Write-ColorOutput {
    param(
        [string]$Text,
        [string]$Color = "White"
    )
    Write-Host $Text -ForegroundColor $Color
}

function Write-Section {
    param([string]$Title)
    Write-Host ""
    Write-ColorOutput "===================================================================" "Cyan"
    Write-ColorOutput "  $Title" "Cyan"
    Write-ColorOutput "===================================================================" "Cyan"
}

function Write-Issue {
    param([string]$Text)
    Write-ColorOutput "  ! POTENTIAL ISSUE: $Text" "Yellow"
}

function Write-Problem {
    param([string]$Text)
    Write-ColorOutput "  X PROBLEM: $Text" "Red"
}

function Write-Good {
    param([string]$Text)
    Write-ColorOutput "  + $Text" "Green"
}

# Validate file exists
if (-not (Test-Path $InputFile)) {
    Write-Problem "File not found: $InputFile"
    exit 1
}

# Validate ffprobe is available
try {
    $null = & $ffprobePath -version 2>&1
} catch {
    Write-Problem "FFprobe not found. Please install FFmpeg or update the path in the script."
    exit 1
}

# Start output capture if file specified
if ($OutputFile) {
    Start-Transcript -Path $OutputFile -Force | Out-Null
}

Write-Section "PLEX VIDEO DIAGNOSTIC REPORT"
Write-Host "File: $InputFile"
Write-Host "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host ""

# Get file size
$fileSize = (Get-Item $InputFile).Length
$fileSizeGB = [math]::Round($fileSize / 1GB, 2)
Write-Host "File Size: $fileSizeGB GB ($fileSize bytes)"

Write-Section "CONTAINER FORMAT ANALYSIS"

# Get container format
$formatJson = & $ffprobePath -v quiet -print_format json -show_format "$InputFile" 2>&1
$format = $formatJson | ConvertFrom-Json

Write-Host "  Container: $($format.format.format_name)"
Write-Host "  Duration: $([TimeSpan]::FromSeconds($format.format.duration).ToString('hh\:mm\:ss\.fff'))"
Write-Host "  Bitrate: $([math]::Round($format.format.bit_rate / 1000000, 2)) Mbps"

# Check for problematic containers
if ($format.format.format_name -match "avi") {
    Write-Issue "AVI container can cause issues. Consider remuxing to MKV."
}
if ($format.format.format_name -match "wmv|asf") {
    Write-Problem "WMV/ASF containers often cause playback issues in Plex."
}

Write-Section "VIDEO STREAM ANALYSIS"

# Get all streams
$streamsJson = & $ffprobePath -v quiet -print_format json -show_streams "$InputFile" 2>&1
$streams = ($streamsJson | ConvertFrom-Json).streams

# Analyze video streams
$videoStreams = $streams | Where-Object { $_.codec_type -eq "video" }

if ($videoStreams.Count -eq 0) {
    Write-Problem "No video streams found!"
} else {
    foreach ($video in $videoStreams) {
        # Skip Motion JPEG streams (usually thumbnails/cover art)
        if ($video.codec_name -eq "mjpeg") {
            Write-Host ""
            Write-Host "  Skipping Video Stream #$($video.index) (Motion JPEG - likely thumbnail/cover art)"
            continue
        }
        
        $streamIndex = $video.index
        Write-Host ""
        Write-ColorOutput "  Video Stream #$streamIndex" "White"
        Write-Host "  -----------------------------------------------------------------"
        
        # Basic codec info
        Write-Host "  Codec: $($video.codec_name) ($($video.codec_long_name))"
        Write-Host "  Profile: $($video.profile)"
        Write-Host "  Resolution: $($video.width)x$($video.height)"
        Write-Host "  Pixel Format: $($video.pix_fmt)"
        
        # Frame rate
        $fps = if ($video.r_frame_rate -match "(\d+)/(\d+)") {
            [math]::Round([int]$matches[1] / [int]$matches[2], 3)
        } else { "Unknown" }
        Write-Host "  Frame Rate: $fps fps"
        
        # Bitrate
        if ($video.bit_rate) {
            Write-Host "  Bitrate: $([math]::Round($video.bit_rate / 1000000, 2)) Mbps"
        }
        
        # Check for problematic codecs
        if ($video.codec_name -eq "mpeg2video") {
            Write-Issue "MPEG2 video may require transcoding on some clients."
        }
        if ($video.codec_name -eq "vc1") {
            Write-Problem "VC-1 codec often causes issues. Recommend re-encoding to H.264/HEVC."
        }
        if ($video.codec_name -match "vp8|vp9" -and $format.format.format_name -notmatch "webm") {
            Write-Issue "VP8/VP9 in non-WebM container may have compatibility issues."
        }
        
        # HDR Analysis
        Write-Host ""
        Write-ColorOutput "  HDR/Color Information:" "White"
        Write-Host "  Color Space: $($video.color_space)"
        Write-Host "  Color Transfer: $($video.color_transfer)"
        Write-Host "  Color Primaries: $($video.color_primaries)"
        Write-Host "  Color Range: $($video.color_range)"
        
        # Check for HDR
        $isHDR = $false
        if ($video.color_transfer -match "smpte2084|arib-std-b67") {
            $isHDR = $true
            Write-Good "HDR detected (Transfer: $($video.color_transfer))"
        }
        
        # Check for Dolby Vision
        $hasDolbyVision = $false
        if ($video.side_data_list) {
            foreach ($sideData in $video.side_data_list) {
                if ($sideData.side_data_type -match "DOVI") {
                    $hasDolbyVision = $true
                    Write-Problem "DOLBY VISION detected - Known to cause playback issues on many devices!"
                    Write-Host "    Profile: $($sideData.dv_profile)"
                }
            }
        }
        
        # Check pixel format for HDR compatibility
        if ($isHDR -and $video.pix_fmt -notmatch "yuv420p10le|yuv422p10le|yuv444p10le") {
            Write-Issue "HDR video with unusual pixel format: $($video.pix_fmt)"
        }
        
        # B-frames and reference frames (requires frame analysis)
        Write-Host ""
        Write-ColorOutput "  Frame Analysis:" "White"
        Write-Host "  Checking GOP structure (this may take a moment)..."
        
        # Get detailed frame info for first 1000 frames
        $frameInfoJson = & $ffprobePath -v quiet -print_format json -show_frames -read_intervals "%+#1000" -select_streams v:$streamIndex "$InputFile" 2>&1
        $frameInfo = ($frameInfoJson | ConvertFrom-Json).frames
        
        if ($frameInfo) {
            $iFrames = @($frameInfo | Where-Object { $_.pict_type -eq "I" })
            $pFrames = @($frameInfo | Where-Object { $_.pict_type -eq "P" })
            $bFrames = @($frameInfo | Where-Object { $_.pict_type -eq "B" })
            
            Write-Host "  I-Frames: $($iFrames.Count)"
            Write-Host "  P-Frames: $($pFrames.Count)"
            Write-Host "  B-Frames: $($bFrames.Count)"
            
            if ($iFrames.Count -gt 0) {
                $gopSize = [math]::Round(1000 / $iFrames.Count, 0)
                Write-Host "  Estimated GOP Size: ~$gopSize frames"
                
                if ($gopSize -gt 300) {
                    Write-Issue "Very large GOP size ($gopSize) may cause seeking issues in Plex."
                }
            }
            
            # Check for B-frame pyramid issues
            if ($bFrames.Count -gt 0 -and $video.codec_name -eq "h264") {
                Write-Host "  B-Frame Pyramid: Checking..."
                # B-pyramid can cause issues with some hardware decoders
                if ($video.profile -match "High") {
                    Write-Issue "H.264 High Profile with B-frames may not decode on older devices."
                }
            }
        } else {
            Write-Host "  Unable to analyze frame structure"
        }
        
        # Check for interlacing
        if ($video.field_order -and $video.field_order -ne "progressive") {
            Write-Problem "INTERLACED video detected (Field Order: $($video.field_order))"
            Write-Host "    Interlaced content should be deinterlaced for Plex."
        } else {
            Write-Good "Progressive scan (not interlaced)"
        }
        
        # Level and compatibility
        if ($video.level) {
            Write-Host ""
            Write-Host "  Level: $($video.level)"
            
            if ($video.codec_name -eq "h264" -and $video.level -gt 51) {
                Write-Issue "H.264 Level > 5.1 may not be supported by some devices."
            }
            if ($video.codec_name -eq "hevc" -and $video.level -gt 153) {
                Write-Issue "HEVC Level > 5.1 may require very powerful devices."
            }
        }
    }
}

Write-Section "AUDIO STREAM ANALYSIS"

$audioStreams = $streams | Where-Object { $_.codec_type -eq "audio" }

if ($audioStreams.Count -eq 0) {
    Write-Problem "No audio streams found!"
} else {
    foreach ($audio in $audioStreams) {
        $streamIndex = $audio.index
        Write-Host ""
        Write-ColorOutput "  Audio Stream #$streamIndex" "White"
        Write-Host "  -----------------------------------------------------------------"
        
        $language = if ($audio.tags.language) { $audio.tags.language } else { "Unknown" }
        $title = if ($audio.tags.title) { $audio.tags.title } else { "None" }
        
        Write-Host "  Codec: $($audio.codec_name) ($($audio.codec_long_name))"
        Write-Host "  Language: $language"
        Write-Host "  Title: $title"
        Write-Host "  Channels: $($audio.channels)"
        Write-Host "  Channel Layout: $($audio.channel_layout)"
        Write-Host "  Sample Rate: $($audio.sample_rate) Hz"
        
        if ($audio.bit_rate) {
            Write-Host "  Bitrate: $([math]::Round($audio.bit_rate / 1000, 0)) kbps"
        }
        
        # Check for problematic audio codecs
        if ($audio.codec_name -eq "truehd") {
            Write-Issue "TrueHD audio may not play on all Plex clients. Consider dual-track with AAC."
        }
        if ($audio.codec_name -match "dts") {
            Write-Issue "DTS audio may require transcoding on some clients."
        }
        if ($audio.codec_name -match "pcm_") {
            Write-Issue "PCM audio is large and may cause bandwidth issues. Consider AAC/AC3."
        }
        if ($audio.codec_name -eq "flac") {
            Write-Issue "FLAC audio may not be supported by all clients."
        }
        if ($audio.codec_name -match "vorbis|opus" -and $format.format.format_name -notmatch "webm|ogg") {
            Write-Issue "Vorbis/Opus in non-standard container may have compatibility issues."
        }
        
        # Good codecs
        if ($audio.codec_name -match "aac|ac3|eac3|mp3") {
            Write-Good "Widely compatible audio codec"
        }
    }
}

Write-Section "SUBTITLE STREAM ANALYSIS"

$subtitleStreams = $streams | Where-Object { $_.codec_type -eq "subtitle" }

if ($subtitleStreams.Count -eq 0) {
    Write-Host "  No embedded subtitle streams found."
} else {
    foreach ($subtitle in $subtitleStreams) {
        $streamIndex = $subtitle.index
        Write-Host ""
        Write-ColorOutput "  Subtitle Stream #$streamIndex" "White"
        Write-Host "  -----------------------------------------------------------------"
        
        $language = if ($subtitle.tags.language) { $subtitle.tags.language } else { "Unknown" }
        $title = if ($subtitle.tags.title) { $subtitle.tags.title } else { "None" }
        $forced = if ($subtitle.disposition.forced -eq 1) { "Yes" } else { "No" }
        $default = if ($subtitle.disposition.default -eq 1) { "Yes" } else { "No" }
        
        Write-Host "  Codec: $($subtitle.codec_name)"
        Write-Host "  Language: $language"
        Write-Host "  Title: $title"
        Write-Host "  Forced: $forced"
        Write-Host "  Default: $default"
        
        # Check for problematic subtitle formats
        if ($subtitle.codec_name -eq "hdmv_pgs_subtitle") {
            Write-Issue "PGS/Blu-ray subtitles are image-based and can cause performance issues."
        }
        if ($subtitle.codec_name -eq "dvd_subtitle") {
            Write-Issue "DVD subtitles (VobSub) are image-based and may not scale well."
        }
        if ($subtitle.codec_name -match "ssa|ass") {
            Write-Good "Text-based subtitles (good compatibility)"
        }
        if ($subtitle.codec_name -eq "subrip") {
            Write-Good "SRT subtitles (excellent compatibility)"
        }
        
        # Check disposition issues
        if ($forced -eq "Yes" -and $default -eq "No") {
            Write-Issue "Forced subtitle not set as default. May not display automatically."
        }
    }
}

# Check for external subtitle files
Write-Host ""
Write-ColorOutput "  External Subtitle Files:" "White"
$fileDir = Split-Path -Parent $InputFile
$fileBase = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
$externalSubs = Get-ChildItem -Path $fileDir -Filter "$fileBase*.srt" -ErrorAction SilentlyContinue

if ($externalSubs) {
    foreach ($sub in $externalSubs) {
        Write-Host "  Found: $($sub.Name)"
        Write-Good "External SRT subtitle detected"
    }
} else {
    Write-Host "  No external .srt files found"
}

Write-Section "CONTAINER INTEGRITY CHECK"

Write-Host "  Running FFmpeg validation..."
Write-Host "  Note: For large files, this may take 1-2 minutes. Checking first 60 seconds only."

# Fast validation - only check first 60 seconds and use hardware decoding if available
# This catches most corruption issues without processing the entire file
$validationOutput = & $ffmpegPath -v error -hwaccel auto -t 60 -i "$InputFile" -f null - 2>&1

if ($LASTEXITCODE -eq 0 -and -not $validationOutput) {
    Write-Good "No container errors detected (first 60 seconds checked)"
} else {
    Write-Problem "Container errors detected:"
    $validationOutput | ForEach-Object {
        Write-Host "    $_" -ForegroundColor Red
    }
}

Write-Section "PLEX COMPATIBILITY SUMMARY"

Write-Host ""
$issueCount = 0

# Compile issues
Write-ColorOutput "  Critical Issues:" "Red"
Write-Host ""

if ($videoStreams.Count -eq 0) {
    Write-Host "    * No video streams found"
    $issueCount++
}
if ($hasDolbyVision) {
    Write-Host "    * Dolby Vision present (major playback issues on most devices)"
    $issueCount++
}
if ($videoStreams | Where-Object { $_.codec_name -eq "vc1" }) {
    Write-Host "    * VC-1 codec (poor compatibility)"
    $issueCount++
}
if ($videoStreams | Where-Object { $_.field_order -ne "progressive" -and $_.field_order }) {
    Write-Host "    * Interlaced video (needs deinterlacing)"
    $issueCount++
}
if ($validationOutput) {
    Write-Host "    * Container integrity errors detected"
    $issueCount++
}

if ($issueCount -eq 0) {
    Write-Good "No critical issues found"
}

Write-Host ""
Write-ColorOutput "  Recommendations:" "Yellow"
Write-Host ""

if ($hasDolbyVision) {
    Write-Host "    * Remove Dolby Vision and keep HDR10/HDR10+"
}
if ($audioStreams | Where-Object { $_.codec_name -eq "truehd" }) {
    Write-Host "    * Add AAC compatibility audio track alongside TrueHD"
}
if ($audioStreams | Where-Object { $_.codec_name -match "dts" }) {
    Write-Host "    * Consider converting DTS to EAC3 or AAC"
}
if ($videoStreams | Where-Object { $_.field_order -ne "progressive" -and $_.field_order }) {
    Write-Host "    * Deinterlace video before adding to Plex"
}
if ($subtitleStreams | Where-Object { $_.codec_name -eq "hdmv_pgs_subtitle" }) {
    Write-Host "    * Consider extracting PGS subtitles or converting to SRT"
}
if ($format.format.format_name -match "avi|wmv|asf") {
    Write-Host "    * Remux to MKV or MP4 container"
}
if ($validationOutput) {
    Write-Host "    * Container has errors - Remux/re-encode recommended"
    Write-Host "      Option 1: Use Dan's Plex Remux Script (handles all issues automatically)"
    Write-Host "                https://github.com/mridahodan/DansPersonalPlexRemuxScript"
    Write-Host "      Option 2: Manual remux with: ffmpeg -i input.mkv -c copy -map 0 output.mkv"
}

Write-Host ""
Write-ColorOutput "===================================================================" "Cyan"
Write-Host ""

# Stop transcript if output file specified
if ($OutputFile) {
    Stop-Transcript | Out-Null
    Write-Host "Report saved to: $OutputFile" -ForegroundColor Green
}

Write-Host "Analysis complete!" -ForegroundColor Green
Write-Host ""