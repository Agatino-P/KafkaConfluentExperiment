using Confluent.Kafka;
using KafkaConfluentExperiment;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public class KafkaSeeker : IDisposable
{
    private readonly ILogger<KafkaSeeker> _logger;
    private readonly KafkaOptions _options;
    private IConsumer<string, string> _consumer = default!;
    private bool _disposedValue;

    public KafkaSeeker(ILogger<KafkaSeeker> logger, IOptions<KafkaOptions> options)
    {
        _logger = logger;
        _options = options.Value;
        initialize();
    }


    private void initialize()
    {
        string topic = _options.Topics[0];


        ConsumerConfig config = new()
        {
            BootstrapServers = _options.BootstrapServers,
            //SecurityProtocol = SecurityProtocol.SaslPlaintext,
            //SaslMechanism = SaslMechanism.ScramSha256,
            //SaslUsername = "my-consumer",
            //SaslPassword = "pass",
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();

        _consumer.Subscribe(topic);
    }

    public string? Seek(string keyTobeFound)
    {
        _logger.LogInformation("SeekerCalled");
        try
        {
            ConsumeResult<string, string> result = _consumer.Consume(TimeSpan.FromSeconds(1));

            if (result?.Message?.Key != null)
            {

                if (result.Message.Key == keyTobeFound)
                {
                    return result.Message.Value;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message);
        }
        return null;
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                // TODO: dispose managed state (managed objects)
                _consumer?.Close();
            }
            _consumer?.Dispose();
            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
            // TODO: set large fields to null
            _disposedValue = true;
        }
    }

    // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    ~KafkaSeeker()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: false);
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}