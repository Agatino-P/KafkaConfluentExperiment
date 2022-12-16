using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
//using Serilog;
using static Confluent.Kafka.ConfigPropertyNames;

namespace KafkaConfluentExperiment;

internal class KafkaProducer : BackgroundService
{
    private readonly ILogger<KafkaProducer> _logger;
    private readonly KafkaOptions _options;
    private IProducer<string, string> _producer = default!;
    private Dictionary<string, string> _producerSettings = new();

    public KafkaProducer(ILogger<KafkaProducer> logger, IOptions<KafkaOptions> options)
    {
        this._logger = logger;
        this._options = options.Value;

        _producerSettings["bootstrap.servers"] = _options.BootstrapServers;

        /*
            security.protocol=SASL_SSL
            sasl.mechanisms=PLAIN
            sasl.username=< CLUSTER API KEY >
            sasl.password=< CLUSTER API SECRET >
         */
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _producer = new ProducerBuilder<string, string>(_producerSettings).Build();

        return base.StartAsync(cancellationToken);
    } 

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        Dispose();
        return base.StopAsync(cancellationToken);
    }
    public override void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        base.Dispose();
    }
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);

            string topic = _options.Topics[0];

            Message<string, string> kafkaMessage = new Message<string, string>() { Key = nameof(KafkaProducer), Value = DateTime.Now.ToString() };

            _producer!.Produce(topic, kafkaMessage, (deliveryReport) => _logger.LogInformation(createLogMessage(topic, kafkaMessage, deliveryReport)));
        }
    }

    private static string createLogMessage(string topic, Message<string, string> kafkaMessage, DeliveryReport<string, string> deliveryReport)
    {
        if (deliveryReport.Error.Code != ErrorCode.NoError)
        {
            return $"Failed to deliver message: {deliveryReport.Error.Reason} at {DateTime.Now}";
        }
        else
        {
            return $"Produced event - Topic {topic}: - Key= {kafkaMessage.Key} - Value= {kafkaMessage.Value} at {DateTime.Now}";
        }
    }

}