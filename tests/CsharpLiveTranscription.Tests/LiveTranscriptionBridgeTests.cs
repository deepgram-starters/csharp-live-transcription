using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using CsharpLiveTranscription;
using Deepgram.Models.Listen.v2.WebSocket;
using Xunit;

namespace CsharpLiveTranscription.Tests;

public class LiveTranscriptionBridgeTests
{
    [Fact]
    public async Task HandleAsync_WhenConnectReturnsFalse_SendsContractErrorBefore1011Close()
    {
        var socket = new RecordingWebSocket();
        var client = new FailingLiveClient(() => Task.FromResult(false));

        await LiveTranscriptionBridge.HandleAsync(socket, null, client, "test", CancellationToken.None);

        AssertConnectionFailureLifecycle(socket, client);
    }

    [Fact]
    public async Task HandleAsync_WhenConnectThrows_SendsContractErrorBefore1011Close()
    {
        var socket = new RecordingWebSocket();
        var client = new FailingLiveClient(() => Task.FromException<bool>(new InvalidOperationException("unavailable")));

        await LiveTranscriptionBridge.HandleAsync(socket, null, client, "test", CancellationToken.None);

        AssertConnectionFailureLifecycle(socket, client);
    }

    private static void AssertConnectionFailureLifecycle(RecordingWebSocket socket, FailingLiveClient client)
    {
        Assert.Equal(new[] { "send", "close" }, socket.Events);
        Assert.Equal(WebSocketCloseStatus.InternalServerError, socket.CloseStatus);
        Assert.Equal(1011, (int)socket.CloseStatus!);
        Assert.Equal("Deepgram connection error", socket.CloseStatusDescription);
        Assert.Equal(1, client.StopCalls);

        using var error = JsonDocument.Parse(Assert.Single(socket.SentFrames));
        Assert.Equal("Error", error.RootElement.GetProperty("type").GetString());
        Assert.Equal(2, error.RootElement.EnumerateObject().Count());
        var detail = error.RootElement.GetProperty("error");
        Assert.Equal(3, detail.EnumerateObject().Count());
        Assert.Equal("connection", detail.GetProperty("type").GetString());
        Assert.Equal("CONNECTION_FAILED", detail.GetProperty("code").GetString());
        Assert.Equal("Deepgram connection error", detail.GetProperty("message").GetString());
    }

    private sealed class FailingLiveClient(Func<Task<bool>> connect) : ILiveTranscriptionClient
    {
        public int StopCalls { get; private set; }

        public Task Subscribe(EventHandler<ResultResponse> handler) => Task.CompletedTask;
        public Task Subscribe(EventHandler<MetadataResponse> handler) => Task.CompletedTask;
        public Task Subscribe(EventHandler<SpeechStartedResponse> handler) => Task.CompletedTask;
        public Task Subscribe(EventHandler<UtteranceEndResponse> handler) => Task.CompletedTask;
        public Task Subscribe(EventHandler<ErrorResponse> handler) => Task.CompletedTask;
        public Task<bool> Connect(LiveSchema schema) => connect();
        public void Send(byte[] chunk) => throw new InvalidOperationException("Audio must not be forwarded after a failed connection.");
        public void SendMessage(byte[] message) => throw new InvalidOperationException("Controls must not be forwarded after a failed connection.");

        public Task Stop()
        {
            StopCalls++;
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingWebSocket : WebSocket
    {
        private WebSocketState state = WebSocketState.Open;
        private WebSocketCloseStatus? closeStatus;
        private string? closeStatusDescription;

        public List<string> Events { get; } = [];
        public List<string> SentFrames { get; } = [];
        public override WebSocketCloseStatus? CloseStatus => closeStatus;
        public override string? CloseStatusDescription => closeStatusDescription;
        public override WebSocketState State => state;
        public override string? SubProtocol => null;

        public override void Abort() => state = WebSocketState.Aborted;

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        {
            Events.Add("close");
            this.closeStatus = closeStatus;
            closeStatusDescription = statusDescription;
            state = WebSocketState.Closed;
            return Task.CompletedTask;
        }

        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) =>
            CloseAsync(closeStatus, statusDescription, cancellationToken);

        public override void Dispose() => state = WebSocketState.Closed;

        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("The bridge must not receive after a failed connection.");

        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            Events.Add("send");
            SentFrames.Add(Encoding.UTF8.GetString(buffer));
            return Task.CompletedTask;
        }

        public override ValueTask SendAsync(ReadOnlyMemory<byte> buffer, WebSocketMessageType messageType, WebSocketMessageFlags messageFlags, CancellationToken cancellationToken = default)
        {
            Events.Add("send");
            SentFrames.Add(Encoding.UTF8.GetString(buffer.Span));
            return ValueTask.CompletedTask;
        }
    }
}
