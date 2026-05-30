using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Topology;
using Hopscope.Infrastructure.Serialization;

namespace Hopscope.Push;

/// <summary>
/// Raw <see cref="System.Net.WebSockets"/> push hub.
///
/// Design invariants (AOT-safe):
///   - Each connected client is held in a <see cref="ConcurrentDictionary{K,V}"/>
///     keyed by connectionId (a caller-supplied string, typically a new Guid).
///   - Sends are serialized per-client via a <see cref="SemaphoreSlim"/>: the
///     WebSocket protocol forbids concurrent SendAsync calls on the same socket.
///   - Serialization uses <c>AppJsonSerializerContext.Default.PushFrame</c> — the
///     source-generated overload, no reflection.
///   - <see cref="BroadcastAsync"/> drops and removes any client whose send throws
///     (closed/faulted socket). It does NOT re-snapshot them; that is a documented
///     TODO for a future gap-recovery pass (Phase 2+).
/// </summary>
public sealed class WebSocketPushChannel : IPushChannel
{
    // -----------------------------------------------------------------------
    // Connected-client state
    // -----------------------------------------------------------------------
    private sealed class ClientConn(WebSocket socket)
    {
        public WebSocket Socket { get; } = socket;
        // SemaphoreSlim(1,1) ensures only one SendAsync runs at a time per socket.
        public SemaphoreSlim SendLock { get; } = new(1, 1);
    }

    private readonly ConcurrentDictionary<string, ClientConn> _clients =
        new(StringComparer.Ordinal);

    // -----------------------------------------------------------------------
    // IPushChannel
    // -----------------------------------------------------------------------

    /// <inheritdoc />
    public async ValueTask SendSnapshotAsync(
        string connectionId,
        GraphSnapshot snapshot,
        CancellationToken ct)
    {
        if (!_clients.TryGetValue(connectionId, out var conn))
            return;

        var frame = new PushFrame("snapshot", snapshot, null);
        await SendFrameAsync(conn, frame, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask BroadcastAsync(GraphDelta delta, CancellationToken ct)
    {
        if (_clients.IsEmpty)
            return;

        var frame = new PushFrame("delta", null, delta);
        // Serialize once; reuse the UTF-8 bytes for every client.
        var json = JsonSerializer.Serialize(frame, AppJsonSerializerContext.Default.PushFrame);
        var bytes = Encoding.UTF8.GetBytes(json);

        List<string>? toRemove = null;

        foreach (var (id, conn) in _clients)
        {
            try
            {
                await conn.SendLock.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    await conn.Socket.SendAsync(
                        bytes,
                        WebSocketMessageType.Text,
                        endOfMessage: true,
                        ct).ConfigureAwait(false);
                }
                finally
                {
                    conn.SendLock.Release();
                }
            }
            catch (Exception ex) when (
                ex is WebSocketException
                    or OperationCanceledException
                    or ObjectDisposedException)
            {
                toRemove ??= new List<string>();
                toRemove.Add(id);
            }
        }

        if (toRemove is not null)
        {
            foreach (var id in toRemove)
                _clients.TryRemove(id, out _);
        }
    }

    // -----------------------------------------------------------------------
    // WebSocket lifecycle — called by the /ws endpoint
    // -----------------------------------------------------------------------

    /// <summary>
    /// Registers the socket, sends the current snapshot, then holds the connection
    /// open by running a receive loop (inbound messages are ignored — this is a
    /// server-push channel). Removes the client on close, error, or cancellation.
    ///
    /// Connect-race safety: the client is added to <see cref="_clients"/> BEFORE the
    /// snapshot is captured, and the snapshot is captured + sent while holding the
    /// client's send lock. This closes the window where a delta broadcast between
    /// "capture snapshot" and "register client" would reach no one: any delta emitted
    /// after registration is either already reflected in the snapshot captured here,
    /// or is queued behind the send lock and delivered after the snapshot. The client
    /// drops any delta whose sequence is &lt;= the snapshot's sequence (idempotent
    /// upserts make that safe) and reconnects for a fresh snapshot on a forward gap.
    ///
    /// <paramref name="captureSnapshot"/> is invoked lazily here (not pre-captured by
    /// the caller) precisely so the capture happens after registration.
    /// </summary>
    public async Task HandleClientAsync(
        string connectionId,
        WebSocket socket,
        Func<GraphSnapshot> captureSnapshot,
        CancellationToken ct)
    {
        var conn = new ClientConn(socket);
        // Register FIRST — see the connect-race note above.
        _clients[connectionId] = conn;

        try
        {
            // Capture + send the snapshot while holding the per-client send lock, so no
            // concurrent BroadcastAsync can interleave a delta ahead of the snapshot.
            await conn.SendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var json = JsonSerializer.Serialize(
                    new PushFrame("snapshot", captureSnapshot(), null),
                    AppJsonSerializerContext.Default.PushFrame);
                var bytes = Encoding.UTF8.GetBytes(json);
                await conn.Socket.SendAsync(
                    bytes, WebSocketMessageType.Text, endOfMessage: true, ct).ConfigureAwait(false);
            }
            finally
            {
                conn.SendLock.Release();
            }

            // Receive loop: drain inbound frames so the WebSocket stays healthy;
            // a Close frame from the client exits the loop cleanly.
            var buf = new byte[256];
            while (!ct.IsCancellationRequested
                   && socket.State == WebSocketState.Open)
            {
                var result = await socket.ReceiveAsync(buf, ct).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    // Acknowledge the close handshake.
                    if (socket.State == WebSocketState.CloseReceived)
                    {
                        await socket.CloseOutputAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Goodbye",
                            ct).ConfigureAwait(false);
                    }
                    break;
                }
                // All other inbound messages are intentionally discarded.
            }
        }
        catch (Exception ex) when (
            ex is WebSocketException
                or OperationCanceledException
                or ObjectDisposedException)
        {
            // Socket closed externally or app shutting down — clean exit.
        }
        finally
        {
            _clients.TryRemove(connectionId, out _);
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private static async Task SendFrameAsync(
        ClientConn conn,
        PushFrame frame,
        CancellationToken ct)
    {
        var json  = JsonSerializer.Serialize(frame, AppJsonSerializerContext.Default.PushFrame);
        var bytes = Encoding.UTF8.GetBytes(json);

        await conn.SendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await conn.Socket.SendAsync(
                bytes,
                WebSocketMessageType.Text,
                endOfMessage: true,
                ct).ConfigureAwait(false);
        }
        finally
        {
            conn.SendLock.Release();
        }
    }
}
