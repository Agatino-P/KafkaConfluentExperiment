using Confluent.Kafka;
using KafkaConfluentExperiment;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;



internal class KafkaConsumer : BackgroundService
{
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly KafkaOptions _options;
    private IConsumer<string, string> _consumer = default!;
    private Dictionary<string, string> _consumerSettings = new();

    public KafkaConsumer(ILogger<KafkaConsumer> logger, IOptions<KafkaOptions> options)
    {
        _logger = logger;
        _options = options.Value;




    }
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        string topic = _options.Topics[0];

        var config = new ConsumerConfig
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

        return base.StartAsync(cancellationToken);
    }
    public override Task StopAsync(CancellationToken cancellationToken)
    {
        Dispose();
        return base.StopAsync(cancellationToken);
    }
    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

                try
                {
                    var cts = new CancellationTokenSource();
                    cts.CancelAfter(TimeSpan.FromSeconds(5));
                    ConsumeResult<string, string> result = _consumer.Consume(cts.Token);
                    _logger.LogInformation(createLogMessage("TopicOne", result.Message.Key, result.Message.Value));
                }
                catch (OperationCanceledException)
                {
                    _logger.LogError("Consumer timeout expired");
                }
            }
        }
        catch (OperationCanceledException)
        {

        }
        finally
        {
            _consumer.Close();
        }
        while (!cancellationToken.IsCancellationRequested)
        {
            string message = $"{nameof(KafkaConsumer)} at {DateTime.Now}";
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
        }
    }

    private string createLogMessage(string topic, string resultKkey, string resultValue)
    {
        return $"Consumed event - Topic {topic}: - Key= {resultKkey} - Value= {resultValue} at {DateTime.Now}";
    }

}
