# Manual smoke test for the RESP server.
#   1. cargo run            (listens on 127.0.0.1:6379)
#   2. ./scripts/resp.ps1   (SET, GET, EXISTS, DEL, then a miss over RESP2)
param(
    [string]$Server = '127.0.0.1',
    [int]$Port = 6379,
    [string]$Key = 'smoke',
    [string]$Value = 'hello from powershell'
)

function Send-Command([System.IO.Stream]$Stream, [string[]]$Command) {
    $builder = [System.Text.StringBuilder]::new()
    [void]$builder.Append("*$($Command.Count)`r`n")
    foreach ($part in $Command) {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($part)
        [void]$builder.Append("`$$($bytes.Length)`r`n")
        [void]$builder.Append($part)
        [void]$builder.Append("`r`n")
    }
    $payload = [System.Text.Encoding]::UTF8.GetBytes($builder.ToString())
    $Stream.Write($payload, 0, $payload.Length)
    $Stream.Flush()
}

function Read-Line([System.IO.Stream]$Stream) {
    $builder = [System.Text.StringBuilder]::new()
    while ($true) {
        $byte = $Stream.ReadByte()
        if ($byte -lt 0) { throw 'server closed the connection' }
        if ($byte -eq 10) { break }
        if ($byte -ne 13) { [void]$builder.Append([char]$byte) }
    }
    $builder.ToString()
}

# Replies come back with their RESP marker: '+OK', ':1', or a decoded bulk string.
function Read-Reply([System.IO.Stream]$Stream) {
    $line = Read-Line $Stream
    if (-not $line.StartsWith('$')) { return $line }

    $length = [int]$line.Substring(1)
    if ($length -lt 0) { return $null }

    $buffer = [byte[]]::new($length + 2)
    $read = 0
    while ($read -lt $buffer.Length) {
        $chunk = $Stream.Read($buffer, $read, $buffer.Length - $read)
        if ($chunk -eq 0) { throw 'server closed before the bulk string completed' }
        $read += $chunk
    }
    return [System.Text.Encoding]::UTF8.GetString($buffer, 0, $length)
}

function Expect([object]$Actual, [object]$Expected, [string]$Step) {
    if ("$Actual" -ne "$Expected") {
        throw "$Step : expected '$Expected', got '$Actual'"
    }
    Write-Host "OK: $Step -> '$Actual'"
}

$client = [System.Net.Sockets.TcpClient]::new()
try {
    $client.Connect($Server, $Port)
    $stream = $client.GetStream()
    $stream.ReadTimeout = 5000

    Send-Command $stream @('SET', $Key, $Value)
    Expect (Read-Reply $stream) '+OK' 'SET'

    Send-Command $stream @('GET', $Key)
    Expect (Read-Reply $stream) $Value 'GET'

    Send-Command $stream @('EXISTS', $Key)
    Expect (Read-Reply $stream) ':1' 'EXISTS'

    Send-Command $stream @('DEL', $Key)
    Expect (Read-Reply $stream) ':1' 'DEL'

    Send-Command $stream @('GET', $Key)
    Expect (Read-Reply $stream) $null 'GET after DEL'
}
finally {
    $client.Dispose()
}
