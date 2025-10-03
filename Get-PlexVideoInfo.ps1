<#
.SYNOPSIS
    Comprehensive Plex video file analyzer with HDR metadata validation

.DESCRIPTION
    Analyzes video files for Plex compatibility issues including:
    - Container format problems
    - Video codec compatibility
    - Audio codec and channel layout issues
    - Subtitle format problems
    - HDR metadata validation (mastering display and content light level)
    - Container integrity validation

.PARAMETER InputFile
    Path to the video file to analyze

.PARAMETER OutputFile
    Optional path to save the analysis report as a text file

.EXAMPLE
    .\Get-PlexVideoInfo.ps1 -InputFile "C:\Videos\movie.mkv"
    
.EXAMPLE
    .\Get-PlexVideoInfo.ps1 -InputFile "C:\Videos\movie.mkv" -OutputFile "C:\Reports\analysis.txt"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$InputFile,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputFile
)

# Path to ffprobe and ffmpeg - update these to match your FFmpeg installation
$ffprobePath = "E:\plex\ffmpeg-2025-06-02-git-688f3944ce-full_build\bin\ffprobe.exe"
$ffmpegPath = "E:\plex\ffmpeg-2025-06-02-git-688f3944ce-full_build\bin\ffmpeg.exe"

# Color output functions
function Write-ColorOutput {
    param([string]$Text, [string]$Color)
    Write-Host $Text -ForegroundColor $Color
}

function Write-Section {
    param([string]$Title)
    Write-Host ""
    Write-ColorOutput "===================================================================" "Cyan"
    Write-ColorOutput $Title "Cyan"
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

function Test-HDRMetadata {
    param([object]$VideoStream)
    
    Write-Section "HDR METADATA VALIDATION"
    
    # Check if this is an HDR stream
    $isHDR = $false
    if ($VideoStream.color_transfer -match "smpte2084|arib-std-b67") {
        $isHDR = $true
        Write-Host "  HDR detected: $($VideoStream.color_transfer)" -ForegroundColor Green
    } else {
        Write-Host "  This is not an HDR video (SDR content)" -ForegroundColor Gray
        return
    }
    
    # Get side_data for HDR metadata
    $sideDataJson = & $ffprobePath -v quiet -select_streams v:0 -print_format json -show_frames -read_intervals "%+#1" -show_entries "frame=side_data_list" "$InputFile" 2>&1
    
    if (-not $sideDataJson) {
        Write-Issue "Could not extract HDR side data for validation"
        return
    }
    
    try {
        $frameData = $sideDataJson | ConvertFrom-Json
        $sideDataList = $frameData.frames[0].side_data_list
        
        if (-not $sideDataList) {
            Write-Issue "No HDR side data found - file may have incomplete HDR metadata"
            return
        }
        
        # Find mastering display metadata
        $masteringDisplay = $sideDataList | Where-Object { $_.side_data_type -eq "Mastering display metadata" }
        
        # Find content light level metadata
        $contentLight = $sideDataList | Where-Object { $_.side_data_type -eq "Content light level metadata" }
        
        # Validate Mastering Display Metadata
        if ($masteringDisplay) {
            Write-Host ""
            Write-ColorOutput "  Mastering Display Metadata:" "White"
            Write-Host "  -----------------------------------------------------------------"
            
            # Parse and validate luminance values
            if ($masteringDisplay.max_luminance) {
                $maxLumRaw = [int]($masteringDisplay.max_luminance -split '/')[0]
                $maxLumNits = [math]::Round($maxLumRaw / 10000, 1)
                Write-Host "  Max Luminance: $maxLumNits cd/m² ($maxLumRaw units)"
                
                # Validate max luminance (should be 1000-10000 cd/m²)
                if ($maxLumRaw -lt 5000000 -or $maxLumRaw -gt 100000000) {
                    Write-Problem "Invalid max luminance value! Should be 5,000,000-100,000,000 units (500-10,000 cd/m²)"
                    Write-Host "    This may cause tone mapping issues on some displays" -ForegroundColor Yellow
                } else {
                    Write-Good "Max luminance is within valid range"
                }
            } else {
                Write-Issue "Max luminance not found in metadata"
            }
            
            if ($masteringDisplay.min_luminance) {
                $minLumRaw = [int]($masteringDisplay.min_luminance -split '/')[0]
                $minLumNits = [math]::Round($minLumRaw / 10000, 4)
                Write-Host "  Min Luminance: $minLumNits cd/m² ($minLumRaw units)"
                
                # Validate min luminance (should be > 0)
                if ($minLumRaw -le 0) {
                    Write-Problem "Invalid min luminance value! Must be greater than 0"
                    Write-Host "    This will cause black crush issues" -ForegroundColor Yellow
                } else {
                    Write-Good "Min luminance is valid"
                }
            } else {
                Write-Issue "Min luminance not found in metadata"
            }
            
            # Display color primaries info
            if ($masteringDisplay.red_x) {
                Write-Host ""
                Write-Host "  Color Primaries (in 0.00002 units):" -ForegroundColor Gray
                Write-Host "    Red:   X=$($masteringDisplay.red_x), Y=$($masteringDisplay.red_y)"
                Write-Host "    Green: X=$($masteringDisplay.green_x), Y=$($masteringDisplay.green_y)"
                Write-Host "    Blue:  X=$($masteringDisplay.blue_x), Y=$($masteringDisplay.blue_y)"
                Write-Host "    White: X=$($masteringDisplay.white_point_x), Y=$($masteringDisplay.white_point_y)"
            }
            
        } else {
            Write-Problem "No mastering display metadata found!"
            Write-Host "    HDR playback may not work correctly without this metadata" -ForegroundColor Yellow
            Write-Host "    Recommendation: Re-encode with proper HDR metadata injection" -ForegroundColor Yellow
        }
        
        # Validate Content Light Level Metadata
        if ($contentLight) {
            Write-Host ""
            Write-ColorOutput "  Content Light Level Metadata:" "White"
            Write-Host "  -----------------------------------------------------------------"
            
            $maxCLL = $null
            $maxFALL = $null
            
            if ($contentLight.max_content) {
                $maxCLL = [int]$contentLight.max_content
                Write-Host "  MaxCLL (Max Content Light Level): $maxCLL nits"
                
                # Validate MaxCLL (should be 1-10000 nits, typically 1000-4000)
                if ($maxCLL -le 1 -or $maxCLL -gt 10000) {
                    Write-Problem "Invalid MaxCLL value! Should be 1-10,000 nits"
                    Write-Host "    Typical values are 1000-4000 nits for most content" -ForegroundColor Yellow
                } else {
                    Write-Good "MaxCLL is within valid range"
                    
                    if ($maxCLL -lt 1000) {
                        Write-Host "    Note: MaxCLL is quite low, may indicate animation or dim content" -ForegroundColor Gray
                    } elseif ($maxCLL -gt 4000) {
                        Write-Host "    Note: MaxCLL is high, indicates very bright highlights" -ForegroundColor Gray
                    }
                }
            } else {
                Write-Issue "MaxCLL not found in metadata"
            }
            
            if ($contentLight.max_average) {
                $maxFALL = [int]$contentLight.max_average
                Write-Host "  MaxFALL (Max Frame Average Light Level): $maxFALL nits"
                
                # Validate MaxFALL (should be 1-4000 nits, typically 100-800)
                if ($maxFALL -le 1 -or $maxFALL -gt 4000) {
                    Write-Problem "Invalid MaxFALL value! Should be 1-4,000 nits"
                    Write-Host "    Typical values are 100-800 nits for most content" -ForegroundColor Yellow
                } else {
                    Write-Good "MaxFALL is within valid range"
                }
                
                # Cross-validate MaxCLL and MaxFALL
                if ($maxCLL -and $maxFALL) {
                    if ($maxFALL -gt $maxCLL) {
                        Write-Problem "MaxFALL ($maxFALL) exceeds MaxCLL ($maxCLL)!"
                        Write-Host "    This is physically impossible and indicates corrupted metadata" -ForegroundColor Yellow
                    } else {
                        Write-Good "MaxCLL and MaxFALL relationship is valid"
                    }
                }
            } else {
                Write-Issue "MaxFALL not found in metadata"
            }
            
        } else {
            Write-Problem "No content light level metadata found!"
            Write-Host "    Some displays may not tone map correctly without this metadata" -ForegroundColor Yellow
            Write-Host "    Recommendation: Re-encode with proper HDR metadata injection" -ForegroundColor Yellow
        }
        
        # Check for HDR10+ dynamic metadata
        $hdr10Plus = $sideDataList | Where-Object { 
            $_.side_data_type -match "HDR dynamic metadata SMPTE2094-40" 
        }
        
        if ($hdr10Plus) {
            Write-Host ""
            Write-ColorOutput "  HDR10+ Dynamic Metadata: DETECTED" "Green"
            Write-Host "    This file contains scene-by-scene HDR metadata"
            Write-Host "    Note: HDR10+ may not be preserved during Plex transcoding" -ForegroundColor Yellow
        }
        
        # Overall assessment
        Write-Host ""
        Write-ColorOutput "  Overall HDR Metadata Assessment:" "White"
        Write-Host "  -----------------------------------------------------------------"
        
        $validationIssues = @()
        
        # Collect all validation issues
        if (-not $masteringDisplay) {
            $validationIssues += "Missing mastering display metadata"
        } else {
            if ($masteringDisplay.max_luminance) {
                $maxLumRaw = [int]($masteringDisplay.max_luminance -split '/')[0]
                if ($maxLumRaw -lt 5000000) {
                    $validationIssues += "Max luminance too low: $maxLumRaw (should be >= 5,000,000)"
                }
                if ($maxLumRaw -gt 100000000) {
                    $validationIssues += "Max luminance too high: $maxLumRaw (should be <= 100,000,000)"
                }
            } else {
                $validationIssues += "Max luminance not found"
            }
            
            if ($masteringDisplay.min_luminance) {
                $minLumRaw = [int]($masteringDisplay.min_luminance -split '/')[0]
                if ($minLumRaw -le 0) {
                    $validationIssues += "Min luminance invalid: $minLumRaw (must be > 0)"
                }
            } else {
                $validationIssues += "Min luminance not found"
            }
        }
        
        if (-not $contentLight) {
            $validationIssues += "Missing content light level metadata"
        } else {
            if ($contentLight.max_content) {
                $maxCLL = [int]$contentLight.max_content
                if ($maxCLL -le 1) {
                    $validationIssues += "MaxCLL too low: $maxCLL (should be > 1)"
                }
                if ($maxCLL -gt 10000) {
                    $validationIssues += "MaxCLL too high: $maxCLL (should be <= 10,000)"
                }
            } else {
                $validationIssues += "MaxCLL not found"
            }
            
            if ($contentLight.max_average) {
                $maxFALL = [int]$contentLight.max_average
                if ($maxFALL -le 1) {
                    $validationIssues += "MaxFALL too low: $maxFALL (should be > 1)"
                }
                if ($maxFALL -gt 4000) {
                    $validationIssues += "MaxFALL too high: $maxFALL (should be <= 4,000)"
                }
                
                # Cross-validate MaxCLL and MaxFALL
                if ($contentLight.max_content) {
                    $maxCLL = [int]$contentLight.max_content
                    if ($maxFALL -gt $maxCLL) {
                        $validationIssues += "MaxFALL ($maxFALL) exceeds MaxCLL ($maxCLL) - physically impossible"
                    }
                }
            } else {
                $validationIssues += "MaxFALL not found"
            }
        }
        
        # Display results
        if ($validationIssues.Count -gt 0) {
            Write-Problem "HDR metadata has $($validationIssues.Count) validation issue(s):"
            foreach ($issue in $validationIssues) {
                Write-Host "    - $issue" -ForegroundColor Red
            }
            Write-Host ""
            Write-Host "  Recommendation: Re-encode with the Plex conversion script" -ForegroundColor Yellow
            Write-Host "  The script will inject proper HDR metadata with validated values" -ForegroundColor Yellow
        } else {
            Write-Good "All HDR metadata validation checks passed"
            Write-Host "    Mastering display and content light level metadata are valid" -ForegroundColor Green
        }
        
    } catch {
        Write-Problem "Error parsing HDR metadata: $_"
    }
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
        $streamIndex = $video.index
        Write-Host ""
        Write-ColorOutput "  Video Stream #$streamIndex" "White"
        Write-Host "  -----------------------------------------------------------------"
        
        Write-Host "  Codec: $($video.codec_name) ($($video.codec_long_name))"
        Write-Host "  Resolution: $($video.width)x$($video.height)"
        Write-Host "  Pixel Format: $($video.pix_fmt)"
        Write-Host "  Frame Rate: $($video.r_frame_rate) fps"
        Write-Host "  Aspect Ratio: $($video.display_aspect_ratio)"
        
        if ($video.bit_rate) {
            Write-Host "  Bitrate: $([math]::Round($video.bit_rate / 1000000, 2)) Mbps"
        }
        
        # Color space information
        Write-Host "  Color Space: $($video.color_space)"
        Write-Host "  Color Transfer: $($video.color_transfer)"
        Write-Host "  Color Primaries: $($video.color_primaries)"
        
        # Check for HDR
        if ($video.color_transfer -match "smpte2084|arib-std-b67") {
            Write-Good "HDR video detected"
            if ($video.color_transfer -eq "smpte2084") {
                Write-Host "    HDR10/HDR10+ (PQ transfer function)"
            } elseif ($video.color_transfer -eq "arib-std-b67") {
                Write-Host "    HLG (Hybrid Log-Gamma)"
            }
        }
        
        # Check for problematic codecs
        if ($video.codec_name -match "mpeg2video") {
            Write-Issue "MPEG-2 video is outdated and inefficient. Consider re-encoding."
        }
        if ($video.codec_name -eq "vc1") {
            Write-Issue "VC-1 codec may require transcoding on some clients."
        }
        
        # Check for interlacing
        if ($video.field_order -and $video.field_order -ne "progressive") {
            Write-Issue "Video is interlaced ($($video.field_order)). May cause playback issues."
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
    
    # Perform HDR metadata validation on the first video stream
    Test-HDRMetadata -VideoStream $videoStreams[0]

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

# Quick validation check - just check first 60 seconds
$validationOutput = & $ffmpegPath -hwaccel auto -v error -t 60 -i "$InputFile" -f null - 2>&1

if ($validationOutput) {
    $errorLines = $validationOutput -split "`n" | Where-Object { $_.Trim() -ne "" }
    $errorCount = $errorLines.Count
    Write-Problem "Container validation found $errorCount error(s):"
    $errorLines | Select-Object -First 10 | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
    if ($errorCount -gt 10) {
        Write-Host "    ... and $($errorCount - 10) more error(s)" -ForegroundColor Yellow
    }
} else {
    Write-Good "No container errors detected (first 60 seconds checked)"
}

Write-Section "PLEX COMPATIBILITY SUMMARY"

Write-Host "  Recommendations for optimal Plex playback:"
Write-Host ""

# Container recommendations
if ($format.format.format_name -match "matroska") {
    Write-Good "MKV container is excellent for Plex"
}
if ($format.format.format_name -match "mov,mp4") {
    Write-Good "MP4 container is excellent for Plex"
}
if ($format.format.format_name -match "avi|wmv|asf") {
    Write-Host "    * Remux to MKV or MP4 container"
}

# Video codec recommendations
if ($videoStreams.Count -gt 0) {
    $videoCodec = $videoStreams[0].codec_name
    if ($videoCodec -match "h264|hevc|av1") {
        Write-Good "Video codec is modern and efficient"
    } else {
        Write-Host "    * Consider re-encoding to H.264 or HEVC"
    }
}

# Audio recommendations
$hasCompatibleAudio = $false
foreach ($audio in $audioStreams) {
    if ($audio.codec_name -match "aac|ac3|eac3") {
        $hasCompatibleAudio = $true
        break
    }
}

if (-not $hasCompatibleAudio) {
    Write-Host "    * Add AAC or AC3 audio track for better compatibility"
}

# Subtitle recommendations
if ($subtitleStreams | Where-Object { $_.codec_name -eq "hdmv_pgs_subtitle" }) {
    Write-Host "    * Consider extracting PGS subtitles or converting to SRT"
}

# HDR metadata recommendations
if ($videoStreams.Count -gt 0 -and $videoStreams[0].color_transfer -match "smpte2084|arib-std-b67") {
    Write-Host ""
    Write-ColorOutput "  HDR Content Recommendations:" "Yellow"
    Write-Host "    * Ensure your Plex server supports HDR tone mapping"
    Write-Host "    * Verify clients support HDR playback"
    Write-Host "    * If HDR metadata validation failed, use the conversion script to fix"
    Write-Host "    * HDR10+ may not survive Plex transcoding (converted to HDR10)"
}

# Container validation recommendations
if ($validationOutput) {
    Write-Host ""
    Write-Host "    * Container has errors - Remux/re-encode recommended"
    Write-Host "      Option 1: Use Dan's Plex Remux Script (handles all issues automatically)"
    Write-Host "                Including HDR metadata validation and injection"
    Write-Host "                https://github.com/mridahodan/DansPersonalPlexRemuxScript"
    Write-Host "      Option 2: Manual remux with: ffmpeg -i input.mkv -c copy -map 0 output.mkv"
    Write-Host "                Note: Manual remux will not fix HDR metadata issues"
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