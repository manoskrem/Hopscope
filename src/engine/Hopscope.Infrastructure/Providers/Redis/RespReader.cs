using System.Text;

namespace Hopscope.Infrastructure.Providers.Redis;

/// <summary>
/// AOT-safe, allocation-minimal RESP (REdis Serialization Protocol) frame reader.
/// Reads one RESP value at a time from a <see cref="Stream"/>.
///
/// Supports all RESP2 types:
///   +  simple string
///   -  error
///   :  integer
///   $  bulk string (incl. $-1 null bulk)
///   *  array (incl. *-1 null array, nested)
///
/// Bulk strings are read by their declared byte length then the trailing CRLF is
/// consumed — the payload is NEVER line-split, so binary payloads are safe.
///
/// Unit-testable: feed a <see cref="MemoryStream"/> with crafted frames.
/// </summary>
internal static class RespReader
{
    /// <summary>
    /// Hard cap on a server-declared bulk-string / line length before we allocate. Keyevent
    /// payloads are Redis key names (small); a multi-MB declared length is a buggy or hostile
    /// server and must not be allowed to drive an unbounded managed allocation on the hot read
    /// loop (RAM-amplification guard against the &lt;35 MB budget). Oversize → InvalidDataException,
    /// which the ingestor's bounded reconnect loop recovers from.
    /// </summary>
    private const int MaxBulkLength = 64 * 1024;

    // ── Public surface ──────────────────────────────────────────────────────

    /// <summary>
    /// Reads one RESP value from <paramref name="stream"/>.
    /// Returns <see langword="null"/> for RESP null bulk ($-1) and null array (*-1).
    /// Throws <see cref="InvalidDataException"/> on a malformed frame.
    /// </summary>
    internal static async ValueTask<string?> ReadValueAsync(Stream stream, CancellationToken ct)
    {
        var prefix = await ReadByteAsync(stream, ct).ConfigureAwait(false);
        return (char)prefix switch
        {
            '+' => await ReadLineAsync(stream, ct).ConfigureAwait(false),
            '-' => await ReadLineAsync(stream, ct).ConfigureAwait(false),
            ':' => await ReadLineAsync(stream, ct).ConfigureAwait(false),
            '$' => await ReadBulkStringAsync(stream, ct).ConfigureAwait(false),
            '*' => await ReadArrayFirstStringAsync(stream, ct).ConfigureAwait(false),
            _   => throw new InvalidDataException($"Unknown RESP prefix byte: 0x{prefix:X2}"),
        };
    }

    /// <summary>
    /// Reads a RESP *4 pmessage frame and returns (pattern, channel, payload).
    /// Returns <see langword="null"/> if the frame is not a pmessage.
    ///
    /// A Redis pmessage push frame has the shape:
    ///   *4\r\n  $9\r\npmessage\r\n  $&lt;n&gt;\r\n&lt;pattern&gt;\r\n  $&lt;n&gt;\r\n&lt;channel&gt;\r\n  $&lt;n&gt;\r\n&lt;payload&gt;\r\n
    /// </summary>
    internal static async ValueTask<(string Pattern, string Channel, string Payload)?> ReadPmessageAsync(
        Stream stream, CancellationToken ct)
    {
        // Read the leading prefix byte.
        var prefix = await ReadByteAsync(stream, ct).ConfigureAwait(false);
        if ((char)prefix != '*')
            throw new InvalidDataException($"Expected RESP array prefix '*', got 0x{prefix:X2}");

        var countLine = await ReadLineAsync(stream, ct).ConfigureAwait(false);
        if (!int.TryParse(countLine, out var count) || count < 0)
            throw new InvalidDataException($"Invalid array count: {countLine}");

        // Not a pmessage (too few elements) — e.g. the PSUBSCRIBE confirmation
        // (*3: [psubscribe, pattern, count]) or a non-pattern 'message' push. We MUST
        // still consume its elements, or the leftover bytes desync the stream and the
        // next read sees the wrong prefix byte (which would bounce the connection in a
        // reconnect loop and deliver zero envelopes). Drain, then return null.
        if (count < 4)
        {
            for (var i = 0; i < count; i++)
                await DrainOneValueAsync(stream, ct).ConfigureAwait(false);
            return null;
        }

        // Element 0: message type (e.g. "pmessage").
        var msgType = await ReadBulkStringValueAsync(stream, ct).ConfigureAwait(false);
        if (!string.Equals(msgType, "pmessage", StringComparison.Ordinal))
        {
            // Drain the remaining elements so the stream stays in sync.
            for (var i = 1; i < count; i++)
                await DrainOneValueAsync(stream, ct).ConfigureAwait(false);
            return null;
        }

        // Element 1: pattern.
        var pattern = await ReadBulkStringValueAsync(stream, ct).ConfigureAwait(false) ?? string.Empty;

        // Element 2: channel.
        var channel = await ReadBulkStringValueAsync(stream, ct).ConfigureAwait(false) ?? string.Empty;

        // Element 3: payload (the key name for keyevent notifications).
        var payload = await ReadBulkStringValueAsync(stream, ct).ConfigureAwait(false) ?? string.Empty;

        // Drain any extra elements (shouldn't happen, but be defensive).
        for (var i = 4; i < count; i++)
            await DrainOneValueAsync(stream, ct).ConfigureAwait(false);

        return (pattern, channel, payload);
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// <summary>
    /// Reads a bulk string ($) starting AFTER the prefix byte has been consumed.
    /// Handles $-1 null bulk. Reads by declared byte length, then consumes CRLF.
    /// </summary>
    internal static async ValueTask<string?> ReadBulkStringValueAsync(Stream stream, CancellationToken ct)
    {
        var prefix = await ReadByteAsync(stream, ct).ConfigureAwait(false);
        if ((char)prefix != '$')
            throw new InvalidDataException($"Expected bulk-string prefix '$', got 0x{prefix:X2}");
        return await ReadBulkStringAsync(stream, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a bulk string body after the '$' prefix has been consumed.
    /// </summary>
    private static async ValueTask<string?> ReadBulkStringAsync(Stream stream, CancellationToken ct)
    {
        var lenLine = await ReadLineAsync(stream, ct).ConfigureAwait(false);
        if (!int.TryParse(lenLine, out var len))
            throw new InvalidDataException($"Invalid bulk-string length: {lenLine}");
        if (len < 0) return null;   // $-1 null bulk
        if (len > MaxBulkLength)
            throw new InvalidDataException($"Bulk-string length {len} exceeds cap {MaxBulkLength}.");

        // Read exactly <len> bytes (LENGTH-based, not line-split — binary-safe).
        var buf = new byte[len];
        var read = 0;
        while (read < len)
        {
            var n = await stream.ReadAsync(buf.AsMemory(read, len - read), ct).ConfigureAwait(false);
            if (n == 0) throw new InvalidDataException("Unexpected end of stream inside bulk string.");
            read += n;
        }

        // Consume the trailing CRLF.
        await ConsumeCrLfAsync(stream, ct).ConfigureAwait(false);

        return Encoding.UTF8.GetString(buf);
    }

    /// <summary>
    /// Reads a RESP array and returns the first string element (for simple inline use).
    /// Used by <see cref="ReadValueAsync"/> when it encounters the '*' prefix at the
    /// top level — only the first element is returned (caller uses ReadPmessageAsync
    /// for full frame parsing).
    /// </summary>
    private static async ValueTask<string?> ReadArrayFirstStringAsync(Stream stream, CancellationToken ct)
    {
        var countLine = await ReadLineAsync(stream, ct).ConfigureAwait(false);
        if (!int.TryParse(countLine, out var count) || count < 0) return null;

        string? first = null;
        for (var i = 0; i < count; i++)
        {
            var val = await ReadValueAsync(stream, ct).ConfigureAwait(false);
            if (i == 0) first = val;
        }
        return first;
    }

    /// <summary>
    /// Drains one complete RESP value without returning it (keeps stream in sync
    /// when we encounter an array element we don't care about).
    /// </summary>
    private static async ValueTask DrainOneValueAsync(Stream stream, CancellationToken ct)
    {
        var prefix = await ReadByteAsync(stream, ct).ConfigureAwait(false);
        switch ((char)prefix)
        {
            case '+':
            case '-':
            case ':':
                await ReadLineAsync(stream, ct).ConfigureAwait(false);
                break;
            case '$':
                await ReadBulkStringAsync(stream, ct).ConfigureAwait(false);
                break;
            case '*':
            {
                var countLine = await ReadLineAsync(stream, ct).ConfigureAwait(false);
                if (int.TryParse(countLine, out var n) && n > 0)
                    for (var i = 0; i < n; i++)
                        await DrainOneValueAsync(stream, ct).ConfigureAwait(false);
                break;
            }
        }
    }

    // ── Primitive stream reads ───────────────────────────────────────────────

    /// <summary>
    /// Reads a CRLF-terminated line, returning the content WITHOUT the trailing CRLF.
    /// </summary>
    internal static async ValueTask<string> ReadLineAsync(Stream stream, CancellationToken ct)
    {
        var sb = new List<byte>(64);
        while (true)
        {
            var b = await ReadByteAsync(stream, ct).ConfigureAwait(false);
            if (b == '\r')
            {
                var lf = await ReadByteAsync(stream, ct).ConfigureAwait(false);
                if (lf != '\n')
                    throw new InvalidDataException($"Expected LF after CR, got 0x{lf:X2}");
                break;
            }
            sb.Add(b);
            if (sb.Count > MaxBulkLength)
                throw new InvalidDataException($"RESP line exceeds cap {MaxBulkLength}.");
        }
        return Encoding.UTF8.GetString(sb.ToArray());
    }

    private static async ValueTask ConsumeCrLfAsync(Stream stream, CancellationToken ct)
    {
        var cr = await ReadByteAsync(stream, ct).ConfigureAwait(false);
        var lf = await ReadByteAsync(stream, ct).ConfigureAwait(false);
        if (cr != '\r' || lf != '\n')
            throw new InvalidDataException($"Expected CRLF after bulk string, got 0x{cr:X2} 0x{lf:X2}");
    }

    private static async ValueTask<byte> ReadByteAsync(Stream stream, CancellationToken ct)
    {
        var buf = new byte[1];
        var n   = await stream.ReadAsync(buf.AsMemory(0, 1), ct).ConfigureAwait(false);
        if (n == 0) throw new InvalidDataException("Unexpected end of RESP stream.");
        return buf[0];
    }
}
