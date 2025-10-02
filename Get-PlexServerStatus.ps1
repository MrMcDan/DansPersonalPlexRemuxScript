# Plex Remote Access Diagnostic Tool
# =========================================================

# CONFIGURATION - Edit these values
$plexToken = "YOUR_PLEX_TOKEN_HERE"
$plexPublicIP = "YOUR_PUBLIC_IP_OR_DOMAIN"
$plexLocalIP = "192.168.1.XXX"  # Your Plex server's local IP
$localPlexPort = 32400
$externalPlexPort = 32400

# =========================================================
# HOW TO GET YOUR PLEX TOKEN:
# Official Plex Support Article:
# https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/
#
# Quick Method:
# 1. Sign in to Plex Web App (app.plex.tv)
# 2. Browse to a library item
# 3. Click the "..." menu and select "Get Info" 
# 4. Click "View XML" at the bottom
# 5. Look in the browser URL bar for "X-Plex-Token=XXXXX"
# 6. Copy everything after the = sign
# =========================================================

# Disable SSL certificate validation
add-type @"
    using System.Net;
    using System.Security.Cryptography.X509Certificates;
    public class TrustAllCertsPolicy : ICertificatePolicy {
        public bool CheckValidationResult(
            ServicePoint srvPoint, X509Certificate certificate,
            WebRequest request, int certificateProblem) {
            return true;
        }
    }
"@
[System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Plex Remote Access Diagnostic Tool" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Test 1: Local Network Access
Write-Host "[TEST 1] Checking LOCAL network access..." -ForegroundColor Yellow
$localUrl = "https://${plexLocalIP}:${localPlexPort}/identity?X-Plex-Token=${plexToken}"
try {
    $localResponse = Invoke-WebRequest -Uri $localUrl -TimeoutSec 10 -UseBasicParsing -ErrorAction Stop
    Write-Host "LOCAL ACCESS: Working" -ForegroundColor Green
    Write-Host "  This means Plex server is running and accessible on your network" -ForegroundColor Gray
    $localWorks = $true
} catch {
    Write-Host "LOCAL ACCESS: Failed" -ForegroundColor Red
    Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "  → Problem: Plex server may not be running or local IP is wrong" -ForegroundColor Yellow
    $localWorks = $false
}
Write-Host ""

# Test 2: Port Connectivity (without SSL)
Write-Host "[TEST 2] Checking if port $externalPlexPort is reachable..." -ForegroundColor Yellow
try {
    $tcpClient = New-Object System.Net.Sockets.TcpClient
    $connection = $tcpClient.BeginConnect($plexPublicIP, $externalPlexPort, $null, $null)
    $wait = $connection.AsyncWaitHandle.WaitOne(5000, $false)
    
    if ($wait -and $tcpClient.Connected) {
        Write-Host "PORT TEST: Port $externalPlexPort is open and reachable" -ForegroundColor Green
        Write-Host "  This means port forwarding appears to be working" -ForegroundColor Gray
        $tcpClient.Close()
        $portOpen = $true
    } else {
        Write-Host "PORT TEST: Cannot reach port $externalPlexPort" -ForegroundColor Red
        Write-Host "  Problem: Port forwarding may not be configured correctly" -ForegroundColor Yellow
        Write-Host "  Or: ISP may be blocking the port" -ForegroundColor Yellow
        $tcpClient.Close()
        $portOpen = $false
    }
} catch {
    Write-Host "PORT TEST: Failed to test port" -ForegroundColor Red
    Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Red
    $portOpen = $false
}
Write-Host ""

# Test 3: External HTTPS Access
Write-Host "[TEST 3] Checking EXTERNAL HTTPS access..." -ForegroundColor Yellow
$externalUrl = "https://${plexPublicIP}:${externalPlexPort}/identity?X-Plex-Token=${plexToken}"
try {
    $externalResponse = Invoke-WebRequest -Uri $externalUrl -TimeoutSec 10 -UseBasicParsing -ErrorAction Stop
    Write-Host "EXTERNAL ACCESS: Working!" -ForegroundColor Green
    Write-Host "  Remote access is fully functional" -ForegroundColor Gray
    $externalWorks = $true
} catch {
    Write-Host "EXTERNAL ACCESS: Failed" -ForegroundColor Red
    Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Red
    $externalWorks = $false
}
Write-Host ""

# Test 4: Windows Firewall Check
Write-Host "[TEST 4] Checking Windows Firewall rules..." -ForegroundColor Yellow
try {
    $firewallRules = Get-NetFirewallRule -DisplayName "*Plex*" -ErrorAction SilentlyContinue
    if ($firewallRules) {
        Write-Host "FIREWALL: Found Plex firewall rules" -ForegroundColor Green
        $firewallRules | ForEach-Object {
            $enabled = if ($_.Enabled) { "Enabled" } else { "Disabled" }
            Write-Host "  - $($_.DisplayName): $enabled" -ForegroundColor Gray
        }
    } else {
        Write-Host "FIREWALL: No Plex-specific rules found" -ForegroundColor Yellow
        Write-Host "  This may be OK if Windows Firewall is disabled or using default rules" -ForegroundColor Gray
    }
} catch {
    Write-Host "FIREWALL: Unable to check (may need admin rights)" -ForegroundColor Yellow
}
Write-Host ""

# Test 5: Check Plex.tv Remote Access Status
Write-Host "[TEST 5] Checking Plex.tv reported status..." -ForegroundColor Yellow
try {
    $plexTvUrl = "https://plex.tv/api/resources?X-Plex-Token=${plexToken}"
    $resources = Invoke-RestMethod -Uri $plexTvUrl -ErrorAction Stop
    $server = $resources.MediaContainer.Device | Where-Object { $_.provides -match "server" } | Select-Object -First 1
    
    if ($server) {
        Write-Host "PLEX.TV: Found server registration" -ForegroundColor Green
        Write-Host "  Server Name: $($server.name)" -ForegroundColor Gray
        Write-Host "  Public Address: $($server.publicAddress)" -ForegroundColor Gray
        Write-Host "  Remote Access: $($server.publicAddressMatches)" -ForegroundColor Gray
        
        if ($server.publicAddressMatches -eq "1") {
            Write-Host "  Plex.tv confirms remote access is enabled" -ForegroundColor Green
        } else {
            Write-Host "  Plex.tv reports remote access may not be working" -ForegroundColor Yellow
        }
    }
} catch {
    Write-Host "PLEX.TV: Unable to check status" -ForegroundColor Yellow
    Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Red
}
Write-Host ""

# Diagnosis Summary
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "DIAGNOSTIC SUMMARY" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if ($externalWorks) {
    Write-Host "RESULT: Remote access is working perfectly!" -ForegroundColor Green
} else {
    Write-Host "RESULT: Remote access is NOT working" -ForegroundColor Red
    Write-Host ""
    Write-Host "LIKELY CAUSE:" -ForegroundColor Yellow
    
    if (-not $localWorks) {
        Write-Host "→ PLEX SERVER ISSUE" -ForegroundColor Red
        Write-Host "  - Verify Plex Media Server is running" -ForegroundColor White
        Write-Host "  - Check local IP address is correct: $plexLocalIP" -ForegroundColor White
        Write-Host "  - Ensure Plex is listening on port $localPlexPort" -ForegroundColor White
    } elseif ($localWorks -and -not $portOpen) {
        Write-Host "→ PORT FORWARDING or ISP BLOCKING" -ForegroundColor Red
        Write-Host "  - Configure port forwarding on your router:" -ForegroundColor White
        Write-Host "    External Port: $externalPlexPort → Internal IP: $plexLocalIP : $plexPort" -ForegroundColor White
        Write-Host "  - Check if your ISP blocks port $externalPlexPort" -ForegroundColor White
        Write-Host "  - Try a different port (like 32401) in Plex settings" -ForegroundColor White
        Write-Host "  - Some ISPs block common ports - contact them to verify" -ForegroundColor White
    } elseif ($localWorks -and $portOpen -and -not $externalWorks) {
        Write-Host "→ FIREWALL or SSL/CERTIFICATE ISSUE" -ForegroundColor Red
        Write-Host "  - Check Windows Firewall allows Plex" -ForegroundColor White
        Write-Host "  - Verify any antivirus isn't blocking connections" -ForegroundColor White
        Write-Host "  - Try disabling 'Require secure connections' in Plex Settings" -ForegroundColor White
        Write-Host "  - Check router firewall settings" -ForegroundColor White
    }
}

Write-Host ""
Write-Host "For more help, visit:" -ForegroundColor Cyan
Write-Host "https://support.plex.tv/articles/200289506-remote-access/" -ForegroundColor Cyan
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Press any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")