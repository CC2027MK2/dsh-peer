#!/usr/bin/env pwsh
# Publish dsh-peer for the first time.
#
# npm cannot publish a brand-new package through Trusted Publishing (a trusted
# publisher can only be configured on an existing package page), so the very
# first release needs one interactive credential:
#
#   .\publish-first.ps1 -Token npm_xxx        # granular token with "Bypass 2FA"
#   .\publish-first.ps1 -Otp 123456           # or a 6-digit OTP (30 s lifetime)
#
# The token / OTP is passed as an argument, never written to disk: it lives in
# this process only. After this succeeds, configure Trusted Publishing on
# https://www.npmjs.com/package/dsh-peer/access and later releases go through
# .github/workflows/publish.yml with no credentials at all.
[CmdletBinding(DefaultParameterSetName = 'Token')]
param(
  [Parameter(ParameterSetName = 'Token')][string]$Token,
  [Parameter(ParameterSetName = 'Otp')][string]$Otp,
  [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

if ($PSCmdlet.ParameterSetName -eq 'Otp' -and $Otp -notmatch '^\d{6}$') {
  throw 'OTP must be six digits.'
}

# Refuse to publish something that does not even parse.
node --check index.js
if ($LASTEXITCODE -ne 0) { throw 'index.js does not parse.' }

# The smoke test spins up a server on 38499 and must pass before we ship.
node smoke.mjs
if ($LASTEXITCODE -ne 0) { throw 'smoke.mjs failed.' }

$pkg = Get-Content package.json -Raw | ConvertFrom-Json
Write-Host "publishing $($pkg.name)@$($pkg.version)"

$npmArgs = @('publish', '--access', 'public')
if ($PSCmdlet.ParameterSetName -eq 'Otp') {
  $npmArgs += "--otp=$Otp"
}
if ($DryRun) { $npmArgs += '--dry-run' }

if ($PSCmdlet.ParameterSetName -eq 'Token') {
  # A throwaway npmrc keeps the credential out of the process list (a CLI
  # argument would be visible to every local process) and out of ~/.npmrc. It is
  # deleted in the finally block even when npm fails.
  $credFile = Join-Path $PSScriptRoot '.npmrc.publish'
  Set-Content -LiteralPath $credFile -Value "//registry.npmjs.org/:_authToken=$Token" -Encoding ASCII -NoNewline
  try {
    & npm @npmArgs --userconfig $credFile
  } finally {
    Remove-Item -LiteralPath $credFile -Force -ErrorAction SilentlyContinue
  }
} else {
  & npm @npmArgs
}

if ($LASTEXITCODE -ne 0) { throw "npm publish exited $LASTEXITCODE" }

if (-not $DryRun) {
  # The registry processes new versions asynchronously; a 404 for the first
  # minute after a successful publish is normal.
  $deadline = (Get-Date).AddMinutes(5)
  do {
    Start-Sleep -Seconds 10
    $code = & curl.exe -s -o NUL -w '%{http_code}' "https://registry.npmjs.org/$($pkg.name)/$($pkg.version)"
    Write-Host "registry probe: HTTP $code"
  } while ($code -ne '200' -and (Get-Date) -lt $deadline)

  if ($code -eq '200') {
    Write-Host "PUBLISHED $($pkg.name)@$($pkg.version)"
    & curl.exe -s "https://registry.npmjs.org/-/npm/v1/attestations/$($pkg.name)@$($pkg.version)" |
      ConvertFrom-Json | Select-Object -ExpandProperty attestations |
      ForEach-Object { Write-Host "attestation: $($_.predicateType)" }
  } else {
    Write-Host "publish returned success but the registry still answers $code after 5 minutes."
  }
}
