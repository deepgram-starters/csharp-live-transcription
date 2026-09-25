using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Deepgram.Models.Listen.v2.WebSocket;

namespace CsharpLiveTranscription;

public interface ILiveTranscriptionClient
{
    Task Subscribe(EventHandler<ResultResponse> handler);
    Task Subscribe(EventHandler<MetadataResponse> handler);
    Task Subscribe(EventHandler<SpeechStartedResponse> handler);
    Task Subscribe(EventHandler<UtteranceEndResponse> handler);
    Task Subscribe(EventHandler<ErrorResponse> handler);
    Task<bool> Connect(LiveSchema schema);
    void Send(byte[] chunk);
    void SendMessage(byte[] message);
    Task Stop();
}

public sealed class DelegatingLiveTranscriptionClient : ILiveTranscriptionClient
{
    private readonly Func<EventHandler<ResultResponse>, Task> subscribeResult;
    private readonly Func<EventHandler<MetadataResponse>, Task> subscribeMetadata;
    private readonly Func<EventHandler<SpeechStartedResponse>, Task> subscribeSpeechStarted;
    private readonly Func<EventHandler<UtteranceEndResponse>, Task> subscribeUtteranceEnd;
    private readonly Func<EventHandler<ErrorResponse>, Task> subscribeError;
    private readonly Func<LiveSchema, Task<bool>> connect;
    private readonly Action<byte[]> send;
    private readonly Action<byte[]> sendMessage;
    private readonly Func<Task> stop;

    public DelegatingLiveTranscriptionClient(
        Func<EventHandler<ResultResponse>, Task> subscribeResult,
        Func<EventHandler<MetadataResponse>, Task> subscribeMetadata,
        Func<EventHandler<SpeechStartedResponse>, Task> subscribeSpeechStarted,
        Func<EventHandler<UtteranceEndResponse>, Task> subscribeUtteranceEnd,
        Func<EventHandler<ErrorResponse>, Task> subscribeError,
        Func<LiveSchema, Task<bool>> connect,
        Action<byte[]> send,
        Action<byte[]> sendMessage,
        Func<Task> stop)
    {
        this.subscribeResult = subscribeResult;
        this.subscribeMetadata = subscribeMetadata;
        this.subscribeSpeechStarted = subscribeSpeechStarted;
        this.subscribeUtteranceEnd = subscribeUtteranceEnd;
        this.subscribeError = subscribeError;
        this.connect = connect;
        this.send = send;
        this.sendMessage = sendMessage;
        this.stop = stop;
    }

    public Task Subscribe(EventHandler<ResultResponse> handler) => subscribeResult(handler);
    public Task Subscribe(EventHandler<MetadataResponse> handler) => subscribeMetadata(handler);
    public Task Subscribe(EventHandler<SpeechStartedResponse> handler) => subscribeSpeechStarted(handler);
    public Task Subscribe(EventHandler<UtteranceEndResponse> handler) => subscribeUtteranceEnd(handler);
    public Task Subscribe(EventHandler<ErrorResponse> handler) => subscribeError(handler);
    public Task<bool> Connect(LiveSchema schema) => connect(schema);
    public void Send(byte[] chunk) => send(chunk);
    public void SendMessage(byte[] message) => sendMessage(message);
    public Task Stop() => stop();
}

public static class LiveTranscriptionBridge
{
    private static readonly JsonSerializerOptions BrowserJsonOptions = new(JsonSerializerDefaults.Web);

    public static async Task HandleAsync(
        WebSocket clientWs,
        string? queryString,
        ILiveTranscriptionClient liveClient,
        string connectionId,
        CancellationToken appCt)
    {
        var outbound = System.Threading.Channels.Channel.CreateUnbounded<string>();

        await liveClient.Subscribe(new EventHandler<ResultResponse>((_, e) => outbound.Writer.TryWrite(JsonSerializer.Serialize(e, BrowserJsonOptions))));
        await liveClient.Subscribe(new EventHandler<MetadataResponse>((_, e) => outbound.Writer.TryWrite(JsonSerializer.Serialize(e, BrowserJsonOptions))));
        await liveClient.Subscribe(new EventHandler<SpeechStartedResponse>((_, e) => outbound.Writer.TryWrite(JsonSerializer.Serialize(e, BrowserJsonOptions))));
        await liveClient.Subscribe(new EventHandler<UtteranceEndResponse>((_, e) => outbound.Writer.TryWrite(JsonSerializer.Serialize(e, BrowserJsonOptions))));
        await liveClient.Subscribe(new EventHandler<ErrorResponse>((_, e) => outbound.Writer.TryWrite(JsonSerializer.Serialize(e, BrowserJsonOptions))));

        var pump = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in outbound.Reader.ReadAllAsync(appCt))
                {
                    if (clientWs.State != WebSocketState.Open) break;
                    await clientWs.SendAsync(Encoding.UTF8.GetBytes(msg), WebSocketMessageType.Text, true, appCt);
                }
            }
            catch (OperationCanceledException) { }
            catch (WebSocketException) { }
        });

        var closeStatus = WebSocketCloseStatus.NormalClosure;
        var closeDescription = "Connection ended";
        var connected = false;

        async Task SendConnectionError()
        {
            if (clientWs.State != WebSocketState.Open) return;

            try
            {
                await clientWs.SendAsync(
                    Encoding.UTF8.GetBytes(DeepgramConnectionFailure.ErrorPayload),
                    WebSocketMessageType.Text,
                    true,
                    CancellationToken.None);
            }
            catch { }
        }

        try
        {
            var schema = AppConfiguration.BuildLiveSchema(queryString);
            Console.WriteLine($"[{connectionId}] Connecting to Deepgram STT API...");

            if (!await liveClient.Connect(schema))
            {
                Console.Error.WriteLine($"[{connectionId}] Failed to connect to Deepgram");
                closeStatus = DeepgramConnectionFailure.CloseStatus;
                closeDescription = DeepgramConnectionFailure.Description;
                await SendConnectionError();
                return;
            }
            connected = true;
            Console.WriteLine($"[{connectionId}] ✓ Connected to Deepgram STT API");

            var buffer = new byte[8192];
            while (clientWs.State == WebSocketState.Open)
            {
                using var message = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await clientWs.ReceiveAsync(new ArraySegment<byte>(buffer), appCt);
                    if (result.MessageType == WebSocketMessageType.Close) break;
                    message.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                if (result.MessageType == WebSocketMessageType.Close) break;
                if (message.Length == 0) continue;

                var chunk = message.ToArray();

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    liveClient.SendMessage(chunk);
                    continue;
                }

                if (result.MessageType != WebSocketMessageType.Binary) continue;
                liveClient.Send(chunk);
            }
        }
        catch (OperationCanceledException)
        {
            // App shutdown or client disconnect.
        }
        catch (WebSocketException ex)
        {
            Console.Error.WriteLine($"[{connectionId}] WebSocket error: {ex.Message}");
            if (DeepgramConnectionFailure.ShouldReportToBrowser(connected))
            {
                closeStatus = DeepgramConnectionFailure.CloseStatus;
                closeDescription = DeepgramConnectionFailure.Description;
                await SendConnectionError();
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{connectionId}] Deepgram connection error: {ex.GetType().Name}");
            closeStatus = DeepgramConnectionFailure.CloseStatus;
            closeDescription = DeepgramConnectionFailure.Description;
            await SendConnectionError();
        }
        finally
        {
            try { await liveClient.Stop(); } catch { }
            outbound.Writer.TryComplete();
            try { await pump; } catch { }

            if (clientWs.State == WebSocketState.Open)
            {
                try
                {
                    await clientWs.CloseAsync(
                        closeStatus,
                        closeDescription,
                        CancellationToken.None);
                }
                catch { }
            }
        }
    }
}
