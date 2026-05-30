using System.Diagnostics;
using System.Net.WebSockets;
using System.Text;
using Hopscope.Domain.Topology;
using Hopscope.Push;

namespace Hopscope.Engine.Tests;

/// <summary>
/// Connect-race tests for <see cref="WebSocketPushChannel"/> (architecture review
/// finding #1). The guarantee under test: the client is registered for broadcasts
/// BEFORE the snapshot is captured, and the snapshot is sent under the per-client
/// send lock — so a delta emitted during/after registration is never lost and is
/// always ordered after the snapshot.
/// </summary>
public sealed class WebSocketPushChannelTests
{
    // ------------------------------------------------------------------
    // A minimal in-memory WebSocket: records every frame sent (in order),
    // and blocks ReceiveAsync on a gate until the test signals a client close.
    // ------------------------------------------------------------------
    private sealed class TestWebSocket : WebSocket
    {
        private readonly TaskCompletionSource _closeGate =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private WebSocketState _state = WebSocketState.Open;

        /// The "kind" of each frame sent, in order ("snapshot" | "delta" | "other").
        public List<string> Sent { get; } = new();
        /// The raw JSON of each frame sent, in order.
        public List<string> SentRaw { get; } = new();

        /// Signal that the client has sent a Close frame, releasing the receive loop.
        public void SignalClose() => _closeGate.TrySetResult();

        public override Task SendAsync(
            ArraySegment<byte> buffer, WebSocketMessageType messageType,
            bool endOfMessage, CancellationToken cancellationToken)
        {
            var text = Encoding.UTF8.GetString(buffer.Array!, buffer.Offset, buffer.Count);
            lock (Sent)
            {
                SentRaw.Add(text);
                Sent.Add(
                    text.StartsWith("{\"kind\":\"snapshot\"", StringComparison.Ordinal) ? "snapshot" :
                    text.StartsWith("{\"kind\":\"delta\"",    StringComparison.Ordinal) ? "delta"    :
                    "other");
            }
            return Task.CompletedTask;
        }

        public override async Task<WebSocketReceiveResult> ReceiveAsync(
            ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => _closeGate.TrySetCanceled(cancellationToken)))
                await _closeGate.Task.ConfigureAwait(false);

            _state = WebSocketState.CloseReceived;
            return new WebSocketReceiveResult(
                0, WebSocketMessageType.Close, true,
                WebSocketCloseStatus.NormalClosure, "bye");
        }

        public override WebSocketState State => _state;
        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override string? SubProtocol => null;
        public override void Abort() => _state = WebSocketState.Aborted;
        public override Task CloseAsync(WebSocketCloseStatus s, string? d, CancellationToken c)
        { _state = WebSocketState.Closed; return Task.CompletedTask; }
        public override Task CloseOutputAsync(WebSocketCloseStatus s, string? d, CancellationToken c)
        { _state = WebSocketState.Closed; return Task.CompletedTask; }
        public override void Dispose() { }
    }

    private static GraphSnapshot Snapshot(long seq) =>
        new(Array.Empty<GraphNode>(), Array.Empty<GraphEdge>(), seq);

    private static GraphDelta Delta(long seq) =>
        new(Array.Empty<GraphNode>(), Array.Empty<GraphEdge>(), seq);

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 2_000)
    {
        var sw = Stopwatch.StartNew();
        while (!condition())
        {
            if (sw.ElapsedMilliseconds > timeoutMs)
                throw new TimeoutException("Condition was not met within the timeout.");
            await Task.Delay(10).ConfigureAwait(false);
        }
    }

    // ------------------------------------------------------------------
    // 1. The snapshot is the first frame, and it is captured LAZILY from the
    //    factory (regression guard: we must NOT go back to pre-capturing it).
    // ------------------------------------------------------------------
    [Fact]
    public async Task SnapshotIsFirstFrame_AndCapturedLazilyFromFactory()
    {
        var channel = new WebSocketPushChannel();
        var socket  = new TestWebSocket();

        var factoryCalls = 0;
        var handle = channel.HandleClientAsync(
            "c1", socket,
            captureSnapshot: () => { factoryCalls++; return Snapshot(7); },
            CancellationToken.None);

        await WaitUntilAsync(() => socket.Sent.Count >= 1);
        socket.SignalClose();
        await handle;

        Assert.Equal(1, factoryCalls);                       // captured once, lazily
        Assert.Equal("snapshot", socket.Sent[0]);            // snapshot is the first frame
        Assert.Contains("\"sequence\":7", socket.SentRaw[0]); // and it is the factory's value
    }

    // ------------------------------------------------------------------
    // 2. The connect-race fix: a delta broadcast DURING snapshot capture (i.e.
    //    after the client is registered) is delivered AFTER the snapshot and is
    //    never lost. Because HandleClientAsync holds the client's send lock while
    //    the factory runs, the concurrent BroadcastAsync must queue behind it.
    // ------------------------------------------------------------------
    [Fact]
    public async Task DeltaBroadcastDuringSnapshotCapture_DeliveredAfterSnapshot_NotLost()
    {
        var channel = new WebSocketPushChannel();
        var socket  = new TestWebSocket();

        Task? broadcast = null;
        var handle = channel.HandleClientAsync(
            "c1", socket,
            captureSnapshot: () =>
            {
                // Simulate a delta arriving exactly during snapshot capture. The client
                // is already registered, so this targets c1 — but c1's send lock is held
                // by HandleClientAsync, so the send queues until the snapshot is sent.
                broadcast = channel.BroadcastAsync(Delta(6), CancellationToken.None).AsTask();
                return Snapshot(5);
            },
            CancellationToken.None);

        await WaitUntilAsync(() => socket.Sent.Count >= 2);   // snapshot followed by delta
        socket.SignalClose();
        await handle;
        await broadcast!;

        Assert.Equal(new[] { "snapshot", "delta" }, socket.Sent);  // ordered, not lost
    }
}
