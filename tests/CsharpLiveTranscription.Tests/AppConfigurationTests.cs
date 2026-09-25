using System.Text.Json;
using CsharpLiveTranscription;
using Xunit;

namespace CsharpLiveTranscription.Tests;

public class AppConfigurationTests
{
    [Fact]
    public void BuildLiveSchema_UsesStableDefaults()
    {
        var schema = AppConfiguration.BuildLiveSchema(null);

        Assert.Equal("nova-3", schema.Model);
        Assert.Equal("en", schema.Language);
        Assert.True(schema.SmartFormat);
        Assert.False(schema.InterimResults);
        Assert.Equal("linear16", schema.Encoding);
        Assert.Equal(16000, schema.SampleRate);
        Assert.Equal(1, schema.Channels);
    }

    [Fact]
    public void BuildLiveSchema_UsesSupportedQueryParameters()
    {
        var schema = AppConfiguration.BuildLiveSchema(
            "?model=nova-2&language=es&smart_format=false&interim_results=true&encoding=opus&sample_rate=48000&channels=2");

        Assert.Equal("nova-2", schema.Model);
        Assert.Equal("es", schema.Language);
        Assert.False(schema.SmartFormat);
        Assert.True(schema.InterimResults);
        Assert.Equal("opus", schema.Encoding);
        Assert.Equal(48000, schema.SampleRate);
        Assert.Equal(2, schema.Channels);
    }

    [Fact]
    public void ValidateSessionSecret_RejectsAnExplicitSecretShorterThan32Utf8Bytes()
    {
        var exception = Assert.Throws<InvalidOperationException>(
            () => AppConfiguration.ValidateSessionSecret(new string('a', 31)));

        Assert.Contains("at least 32 UTF-8 bytes", exception.Message);
    }

    [Fact]
    public void ValidateSessionSecret_AllowsTheGeneratedSecretPathAndA32ByteSecret()
    {
        AppConfiguration.ValidateSessionSecret(null);
        AppConfiguration.ValidateSessionSecret(new string('a', 32));
    }

    [Fact]
    public void PreConnectionFailure_UsesTheBrowserErrorContract()
    {
        Assert.True(DeepgramConnectionFailure.ShouldReportToBrowser(connected: false));
        Assert.False(DeepgramConnectionFailure.ShouldReportToBrowser(connected: true));
        Assert.Equal(1011, (int)DeepgramConnectionFailure.CloseStatus);

        using var error = JsonDocument.Parse(DeepgramConnectionFailure.ErrorPayload);
        Assert.Equal("Error", error.RootElement.GetProperty("type").GetString());
        Assert.Equal(2, error.RootElement.EnumerateObject().Count());

        var detail = error.RootElement.GetProperty("error");
        Assert.Equal(3, detail.EnumerateObject().Count());
        Assert.Equal("connection", detail.GetProperty("type").GetString());
        Assert.Equal("CONNECTION_FAILED", detail.GetProperty("code").GetString());
        Assert.Equal("Deepgram connection error", detail.GetProperty("message").GetString());
    }
}
