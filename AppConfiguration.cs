using System.Net.WebSockets;
using System.Text;
using Deepgram.Models.Listen.v2.WebSocket;

namespace CsharpLiveTranscription;

public static class AppConfiguration
{
    public const int MinimumSessionSecretBytes = 32;

    public static void ValidateSessionSecret(string? sessionSecret)
    {
        if (sessionSecret is not null && Encoding.UTF8.GetByteCount(sessionSecret) < MinimumSessionSecretBytes)
        {
            throw new InvalidOperationException(
                $"SESSION_SECRET must contain at least {MinimumSessionSecretBytes} UTF-8 bytes.");
        }
    }

    public static LiveSchema BuildLiveSchema(string? queryString)
    {
        var query = System.Web.HttpUtility.ParseQueryString(queryString ?? "");

        return new LiveSchema
        {
            Model = query["model"] ?? "nova-3",
            Language = query["language"] ?? "en",
            SmartFormat = (query["smart_format"] ?? "true") == "true",
            InterimResults = (query["interim_results"] ?? "false") == "true",
            Encoding = query["encoding"] ?? "linear16",
            SampleRate = int.TryParse(query["sample_rate"], out var sr) ? sr : 16000,
            Channels = int.TryParse(query["channels"], out var ch) ? ch : 1,
        };
    }
}

public static class DeepgramConnectionFailure
{
    public const string Description = "Deepgram connection error";
    public const string ErrorPayload = "{\"type\":\"Error\",\"error\":{\"type\":\"connection\",\"code\":\"CONNECTION_FAILED\",\"message\":\"Deepgram connection error\"}}";
    public const WebSocketCloseStatus CloseStatus = WebSocketCloseStatus.InternalServerError;

    public static bool ShouldReportToBrowser(bool connected) => !connected;
}
