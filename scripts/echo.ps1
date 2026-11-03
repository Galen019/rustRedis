# Manual smoke test for the echo server.
#   1. cargo run            (listens on 127.0.0.1:6379)
#   2. ./scripts/echo.ps1   (sends bytes, expects the same bytes back)
param(
    [string]$Server = '127.0.0.1',
    [int]$Port = 6379,
    [string]$Message = 'hello from powershell'
)

$payload = [System.Text.Encoding]::UTF8.GetBytes($Message)
$client = [System.Net.Sockets.TcpClient]::new()
try {
    $client.Connect($Server, $Port)
    $stream = $client.GetStream()
    $stream.ReadTimeout = 5000

    $stream.Write($payload, 0, $payload.Length)
    $stream.Flush()

    $reply = [byte[]]::new($payload.Length)
    $read = 0
    while ($read -lt $reply.Length) {
        $n = $stream.Read($reply, $read, $reply.Length - $read)
        if ($n -eq 0) { throw 'server closed before echoing everything back' }
        $read += $n
    }

    $replyText = [System.Text.Encoding]::UTF8.GetString($reply)
    Write-Host "sent:     $Message"
    Write-Host "received: $replyText"
    if ($replyText -ne $Message) { throw 'echo mismatch' }
    Write-Host 'OK: echoed verbatim'
}
finally {
    $client.Dispose()
}
